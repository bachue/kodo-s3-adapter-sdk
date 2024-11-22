import { Readable, Transform, TransformCallback, Writable } from 'stream';
import { pipeline } from 'stream/promises';
import { createWriteStream, promises as fsPromises } from 'fs';
import { ThrottleGroup, ThrottleOptions } from 'stream-throttle';

import { Ref } from './types';
import { Progress, ProgressStream, SpeedMonitor } from './progress-stream';
import { Adapter, Domain, ObjectHeader, StorageObject, UrlStyle } from './adapter';
import { HttpClient } from './http-client';

export class Downloader {
    static readonly userCanceledError = new Error('User Canceled');
    static readonly chunkTimeoutError = new Error('Chunk Timeout');

    static readonly defaultChunkTimeout = 3000; // 3s
    static readonly speedInterval = 1000; // 1s
    static readonly speedWindowSize = 3; // window duration is 3 * speedInterval
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
        const readerOnDataError: Ref<Error> = {};

        try {
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
                const progressMonitor = this.getProgressMonitor({
                    start: recoveredFrom,
                    totalSize: header.size,
                    progressCallback: getFileOption.getCallback.progressCallback,
                });
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
                    urlStyle: getFileOption?.urlStyle,
                },
            );
            pipeList.unshift(reader);

            await pipeline(pipeList);
        } catch (err) {
            if (
                err === HttpClient.userCanceledError ||
                err.toString().includes('aborted')
            ) {
                if (readerOnDataError.current) {
                    throw readerOnDataError.current;
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
        if (this.aborted) {
            return;
        }
        this.abortController?.abort();
    }

    private async getRecoverPosition(filePath: string, from = 0): Promise<number> {
        if (!from) {
            return 0;
        }
        try {
            const stat = await fsPromises.stat(filePath);
            return Math.min(stat.size, from);
        } catch {
            return 0;
        }
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
            urlStyle?: UrlStyle,
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
                urlStyle: option?.urlStyle,
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
            //   Tried upgrade to v16, but still no `writableNeedDrain`. Maybe a mistake of @types/node.
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

    private getProgressMonitor({
        totalSize,
        start = 0,
        progressCallback,
    }: {
        totalSize: number,
        start?: number,
        progressCallback: (progress: Progress) => void,
    }) {
        const monitor = new SpeedMonitor({
            total: totalSize,
            transferred: start,
            interval: Downloader.speedInterval,
            windowSize: Downloader.speedWindowSize,
            autoStart: false,
        });
        monitor.on('progress', progressCallback);
        return new ProgressStream({
            monitor,
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
    progressCallback?: (progress: Progress) => void;
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
    urlStyle?: UrlStyle;
}
