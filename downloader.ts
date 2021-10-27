import { Adapter, Domain, ObjectHeader, ProgressCallback, StorageObject } from './adapter';
import { Readable, Writable } from 'stream';
import { constants as fsConstants, createWriteStream, promises as fsPromises, WriteStream } from 'fs';
import { ThrottleGroup, ThrottleOptions } from 'stream-throttle';

const DEFAULT_RETRIES_ON_SAME_OFFSET = 10;

export class Downloader {
    private aborted = false;
    private static readonly userCanceledError = new Error('User Canceled');

    constructor(private readonly adapter: Adapter) {
    }

    async getObjectToFile(
        region: string,
        object: StorageObject,
        filePath: string,
        domain?: Domain,
        getFileOption?: GetFileOption,
    ): Promise<void> {
        this.aborted = false;

        const header = await this.adapter.getObjectHeader(region, object, domain);
        getFileOption?.getCallback?.headerCallback?.(header);

        let recoveredFrom = 0;
        if (getFileOption?.recoveredFrom) {
            const stat = await fsPromises.stat(filePath);
            recoveredFrom = stat.size;
            if (typeof (getFileOption.recoveredFrom) === 'number') {
                recoveredFrom = getFileOption.recoveredFrom > stat.size ? stat.size : getFileOption.recoveredFrom;
            }
        }

        return await this.getObjectToFilePath(
            region,
            object,
            filePath,
            recoveredFrom,
            header.size,
            0,
            domain,
            getFileOption,
        );
    }

    private async getObjectToFilePath(
        region: string,
        object: StorageObject,
        filePath: string,
        offset: number,
        totalObjectSize: number,
        retriedOnThisOffset: number,
        domain?: Domain,
        getFileOption?: GetFileOption,
    ): Promise<void> {
        const fileWriteStream = createWriteStream(filePath, {
            flags: (fsConstants.O_CREAT | fsConstants.O_WRONLY | fsConstants.O_NONBLOCK) as any,
            encoding: 'binary',
            start: offset,
        });
        try {
            const getResult = await this.getObjectToFileWriteStream(
                region,
                object,
                fileWriteStream,
                offset,
                totalObjectSize,
                domain,
                getFileOption,
            );
            // retries(getResult: GetResult)
            const receivedDataBytes: number = getResult.downloaded;
            const err: Error | undefined = getResult.error;

            if (err) {
                throw err;
            }

            if (this.aborted) {
                throw Downloader.userCanceledError;
            }

            if (receivedDataBytes === totalObjectSize) {
                return;
            }

            if (receivedDataBytes > offset) {
                await this.getObjectToFilePath(
                    region,
                    object,
                    filePath,
                    receivedDataBytes,
                    totalObjectSize,
                    0,
                    domain,
                    getFileOption,
                );
                return;
            }

            if (retriedOnThisOffset < (getFileOption?.retriesOnSameOffset ?? DEFAULT_RETRIES_ON_SAME_OFFSET)) {
                await this.getObjectToFilePath(
                    region,
                    object,
                    filePath,
                    receivedDataBytes,
                    totalObjectSize,
                    retriedOnThisOffset + 1,
                    domain,
                    getFileOption,
                );
                return;
            }

            throw new Error(`File content size mismatch, got ${receivedDataBytes}, expected ${totalObjectSize}`);
        } finally {
            if (!fileWriteStream.destroyed) {
                fileWriteStream.destroy();
            }
        }
    }

    private getObjectToFileWriteStream(region: string, object: StorageObject, fileWriteStream: WriteStream,
        offset: number, totalObjectSize: number, domain?: Domain, getFileOption?: GetFileOption): Promise<GetResult> {

        let tid: number | undefined;
        const clearChunkTimeout = () => {
            if (tid) {
                clearTimeout(tid);
                tid = undefined;
            }
        };
        return new Promise((resolve, reject) => {
            this.adapter.getObjectStream(region, object, domain, { rangeStart: offset }).then((reader) => {
                let receivedDataBytes = offset;
                let thisPartSize = 0;
                let chain: Readable | Writable = reader.on('data', (chunk) => {
                    if (this.aborted) {
                        if (!reader.destroyed) {
                            reader.destroy(Downloader.userCanceledError);
                        }
                        reject(Downloader.userCanceledError);
                        this.abort();
                        return;
                    }

                    receivedDataBytes += chunk.length;
                    if (getFileOption?.chunkTimeout) {
                        clearChunkTimeout();
                        tid = (setTimeout(() => {
                            const timeoutErr = new Error('Chunk Timeout');
                            if (!reader.destroyed) {
                                reader.destroy(timeoutErr);
                            }
                        }, getFileOption.chunkTimeout) as any);
                    }
                    if (getFileOption?.getCallback?.progressCallback) {
                        try {
                            getFileOption.getCallback.progressCallback(receivedDataBytes, totalObjectSize);
                        } catch (err) {
                            if (!reader.destroyed) {
                                reader.destroy(err);
                            }
                            if (!this.aborted) {
                                this.abort();
                                reject(err);
                            }
                            return;
                        }
                    }
                    if (getFileOption?.partSize && getFileOption?.getCallback?.partGetCallback) {
                        thisPartSize += chunk.length;
                        if (thisPartSize > getFileOption.partSize) {
                            try {
                                getFileOption.getCallback.partGetCallback(thisPartSize);
                            } catch (err) {
                                if (!reader.destroyed) {
                                    reader.destroy(err);
                                }
                                if (!this.aborted) {
                                    this.abort();
                                    reject(err);
                                }
                                return;
                            }
                            thisPartSize = 0;
                        }
                    }
                }).on('error', (err) => {
                    if (this.aborted) {
                        reject(err);
                        return;
                    }
                    resolve({ downloaded: receivedDataBytes, error: err });
                });
                if (getFileOption?.downloadThrottleOption) {
                    const throttleGroup = getFileOption?.downloadThrottleGroup ?? new ThrottleGroup(getFileOption.downloadThrottleOption);
                    chain = chain.pipe(throttleGroup.throttle(getFileOption.downloadThrottleOption));
                }
                chain.pipe(fileWriteStream).on('error', (err) => {
                    clearChunkTimeout();
                    if (this.aborted) {
                        reject(err);
                        return;
                    }
                    resolve({ downloaded: receivedDataBytes, error: err });
                }).on('finish', () => {
                    clearChunkTimeout();
                    if (this.aborted) {
                        reject(Downloader.userCanceledError);
                        return;
                    }
                    resolve({ downloaded: receivedDataBytes });
                });
            }).catch((err) => {
                resolve({ downloaded: offset, error: err });
            });
        });
    }

    abort(): void {
        this.aborted = true;
    }
}

interface GetResult {
    error?: Error;
    downloaded: number;
}

export interface GetCallback {
    progressCallback?: ProgressCallback;
    headerCallback?: (header: ObjectHeader) => void;
    partGetCallback?: (partSize: number) => void;
}

export interface GetFileOption {
    recoveredFrom?: number | boolean,
    getCallback?: GetCallback;
    partSize?: number;
    chunkTimeout?: number;
    retriesOnSameOffset?: number;
    downloadThreshold?: number;
    downloadThrottleGroup?: ThrottleGroup;
    downloadThrottleOption?: ThrottleOptions;
}
