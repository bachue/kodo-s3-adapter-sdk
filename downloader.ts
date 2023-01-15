import { pipeline, Readable, Transform, TransformCallback, Writable } from 'stream';
import { createWriteStream, promises as fsPromises } from 'fs';
import { ThrottleGroup, ThrottleOptions } from 'stream-throttle';

import { Ref } from './types';
import { Adapter, Domain, ObjectHeader, ProgressCallback, StorageObject } from './adapter';
import { HttpClient } from './http-client';

export class Downloader {
    static readonly userCanceledError = new Error('User Canceled');
    static readonly chunkTimeoutError = new Error('Chunk Timeout');

    static readonly defaultChunkTimeout = 3000; // 3s
    private abortController?: AbortController;
    private chunkTimeoutTimer?: number;

    constructor(private readonly adapter: Adapter) {
    }

    async getObjectToFile(
        region: string,
        object: StorageObject,
        filePath: string,
        domain?: Domain,
        getFileOption?: GetFileOption,
    ): Promise<void> {
        this.abortController = new AbortController();

        if (this.aborted) {
            throw Downloader.userCanceledError;
        }

        const header = await this.adapter.getObjectHeader(region, object, domain);
        getFileOption?.getCallback?.headerCallback?.(header);

        // get recovered position
        const recoveredFrom = await this.getRecoverPosition(filePath, getFileOption?.recoveredFrom);

        const pipeList: (Readable | Writable)[] = [];

        // get write stream
        const fileWriteStream = this.getWriteableStream(
            filePath,
            recoveredFrom,
        );
        pipeList.unshift(fileWriteStream);

        // get progress monitor
        if (getFileOption?.getCallback?.progressCallback) {
            const progressMonitor = this.getProgressMonitor(
                recoveredFrom,
                header.size,
                getFileOption.getCallback.progressCallback,
            );
            pipeList.unshift(progressMonitor);
        }

        // get part get monitor
        if (getFileOption?.getCallback?.partGetCallback &&
            getFileOption.partSize &&
            getFileOption.partSize > 0
        ) {
            const partGetMonitor = this.getPartGetMonitor(
                getFileOption.partSize,
                getFileOption.getCallback.partGetCallback,
            );
            pipeList.unshift(partGetMonitor);
        }

        // get chunk timeout monitor
        const chunkTimeout = getFileOption?.chunkTimeout ?? Downloader.defaultChunkTimeout;
        const readerOnDataError: Ref<Error> = {};
        if (chunkTimeout > 0) {
            const chunkTimeoutMonitor = this.getChunkTimeoutMonitor(
                chunkTimeout,
                readerOnDataError,
            );
            pipeList.unshift(chunkTimeoutMonitor);
        }

        // get throttle stream
        if (getFileOption?.downloadThrottleOption) {
            const throttleGroup = getFileOption?.downloadThrottleGroup ?? new ThrottleGroup(getFileOption.downloadThrottleOption);
            pipeList.unshift(throttleGroup.throttle(getFileOption.downloadThrottleOption));
        }

        // get read stream
        const reader = await this.getObjectReader(
            region,
            object,
            domain,
            {
                rangeStart: recoveredFrom,
            },
        );
        pipeList.unshift(reader);

        // change to stream/promises pipeline
        const result = new Promise<void>((resolve, reject) => {
            pipeline(pipeList, err => {
                if (!err) {
                    resolve();
                    return;
                }
                reject(err);
                return;
            });
        });

        try {
            await result;
        } catch (err) {
            if (
                err === HttpClient.userCanceledError ||
                err.toString().includes('aborted')
            ) {
                if (readerOnDataError.current) {
                    throw (readerOnDataError.current);
                }
                throw Downloader.userCanceledError;
            }
            throw err;
        } finally {
            clearTimeout(this.chunkTimeoutTimer);
            this.chunkTimeoutTimer = undefined;
        }
    }

    private get aborted(): boolean {
        return this.abortController?.signal.aborted ?? false;
    }

    abort(): void {
        this.abortController?.abort();
    }

    private async getRecoverPosition(filePath: string, from = 0): Promise<number> {
        if (!from) {
            return 0;
        }
        const stat = await fsPromises.stat(filePath);
        return Math.min(stat.size, from);
    }

    private getWriteableStream(
        filePath: string,
        start = 0,
    ): Writable {
        return createWriteStream(filePath, {
            flags: 'a',
            encoding: 'binary',
            start: start,
        });
    }

    private async getObjectReader(
        regionId: string,
        object: StorageObject,
        domain: Domain | undefined,
        option?: {
            rangeStart?: number,
        },
    ): Promise<Readable> {
        // default values
        const start = option?.rangeStart ?? 0;

        // get reader stream
        return await this.adapter.getObjectStream(
            regionId,
            object,
            domain,
            {
                rangeStart: start,
                abortSignal: this.abortController?.signal,
            }
        );
    }

    private getChunkTimeoutMonitor(
        chunkTimeout: number,
        errorRef: Ref<Error>,
    ): Transform {
        const checkNeedTimeout = () => {
            if (this.aborted) {
                return;
            }
            // TODO: upgrade @types/node to support `writableNeedDrain` for removing @ts-ignore
            // @ts-ignore
            if (t.writableNeedDrain) {
                // upstream.on('data') -> t.write -> t.transform -> checkNeedTimeout
                // so need chunkTimeout if no more data get in after drain.
                t.once('drain', () => {
                    if (this.chunkTimeoutTimer) {
                        clearTimeout(this.chunkTimeoutTimer);
                    }
                    this.chunkTimeoutTimer = setTimeout(checkNeedTimeout, chunkTimeout) as unknown as number;
                });
                return;
            }
            errorRef.current = Downloader.chunkTimeoutError;
            this.chunkTimeoutTimer = undefined;
            this.abort();
        };

        const transform = (chunk: Buffer, _encoding: BufferEncoding, callback: TransformCallback) => {
            if (this.chunkTimeoutTimer) {
                clearTimeout(this.chunkTimeoutTimer);
            }
            this.chunkTimeoutTimer = setTimeout(checkNeedTimeout, chunkTimeout) as unknown as number;
            callback(null, chunk);
        };

        const t = new Transform({
            transform,
        });
        return t;
    }

    private getProgressMonitor(start: number, totalSize: number, progressCallback: ProgressCallback) {
        let receivedDataBytes = start;
        return new Transform({
            transform: (chunk, _encoding, callback) => {
                receivedDataBytes += chunk.length;
                progressCallback(receivedDataBytes, totalSize);
                callback(null, chunk);
            },
        });
    }

    private getPartGetMonitor(
        partSize: number,
        partGetCallback: (partSize: number) => void,
    ) {
        let receivedPartSize = 0;
        return new Transform({
            transform: (chunk, _encoding, callback) => {
                receivedPartSize += chunk.length;
                if (receivedPartSize > partSize) {
                    partGetCallback(receivedPartSize);
                    receivedPartSize = 0;
                }
                callback(null, chunk);
            }
        });
    }
}

export interface GetCallback {
    progressCallback?: ProgressCallback;
    headerCallback?: (header: ObjectHeader) => void;
    partGetCallback?: (partSize: number) => void;
}

export interface GetFileOption {
    recoveredFrom?: number,
    partSize?: number;
    chunkTimeout?: number;
    downloadThrottleGroup?: ThrottleGroup;
    downloadThrottleOption?: ThrottleOptions;
    getCallback?: GetCallback;
}
