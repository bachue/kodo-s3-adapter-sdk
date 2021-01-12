import { Adapter, Object, Domain, ProgressCallback } from './adapter';
import { Readable, Writable } from 'stream';
import { createWriteStream, WriteStream, constants as fsConstants } from 'fs';
import { promises as fsPromises } from 'fs';
import { ThrottleGroup, ThrottleOptions } from 'stream-throttle';

const DEFAULT_RETRIES_ON_SAME_OFFSET = 10;

export class Downloader {
    private aborted: boolean = false;
    private static readonly userCanceledError = new Error('User Canceled');

    constructor(private readonly adapter: Adapter) {
    }

    getObjectToFile(region: string, object: Object, filePath: string, domain?: Domain, getFileOption?: GetFileOption): Promise<void> {
        this.aborted = false;

        return new Promise((resolve, reject) => {
            this.adapter.getObjectHeader(region, object, domain).then((header) => {
                if (getFileOption?.recoveredFrom) {
                    fsPromises.stat(filePath).then((stat) => {
                        let recoveredFrom = stat.size;
                        if (typeof(getFileOption.recoveredFrom) === 'number') {
                            recoveredFrom = getFileOption.recoveredFrom > stat.size ? stat.size : getFileOption.recoveredFrom;
                        }
                        this.getObjectToFilePath(region, object, filePath, recoveredFrom, header.size, 0, domain, getFileOption).then(resolve, reject);
                    }, reject);
                } else {
                    this.getObjectToFilePath(region, object, filePath, 0, header.size, 0, domain, getFileOption).then(resolve, reject);
                }
            }, reject);
        });
    }

    private getObjectToFilePath(region: string, object: Object, filePath: string, offset: number, totalObjectSize: number,
                                retriedOnThisOffset: number, domain?: Domain, getFileOption?: GetFileOption): Promise<void> {
        return new Promise((resolve, reject) => {
            const fileWriteStream = createWriteStream(filePath, {
                flags: <any>(fsConstants.O_CREAT | fsConstants.O_WRONLY | fsConstants.O_NONBLOCK),
                encoding: 'binary',
                start: offset,
            });
            const retries = (getResult: GetResult) => {
                const receivedDataBytes: number = getResult.downloaded;
                const err: Error | undefined = getResult.error;
                if (this.aborted) {
                    reject(err);
                } else if (receivedDataBytes === totalObjectSize) {
                    resolve();
                } else if (receivedDataBytes > offset) {
                    this.getObjectToFilePath(region, object, filePath, receivedDataBytes, totalObjectSize,
                                             0, domain, getFileOption).then(resolve, reject);
                } else if (retriedOnThisOffset < (getFileOption?.retriesOnSameOffset ?? DEFAULT_RETRIES_ON_SAME_OFFSET)) {
                    this.getObjectToFilePath(region, object, filePath, receivedDataBytes, totalObjectSize,
                                             retriedOnThisOffset + 1, domain, getFileOption).then(resolve, reject);
                } else if (err) {
                    reject(err);
                } else {
                    reject(new Error(`File content size mismatch, got ${receivedDataBytes}, expected ${totalObjectSize}`));
                }
            };
            const destroyFileWriteStream = () => {
                if (!fileWriteStream.destroyed) {
                    fileWriteStream.destroy();
                }
            };
            this.getObjectToFileWriteStream(region, object, fileWriteStream, offset, totalObjectSize, domain, getFileOption).then((getResult) => {
                destroyFileWriteStream();
                retries(getResult);
            }, (err) => {
                destroyFileWriteStream();
                reject(err);
            });
        });
    }

    private getObjectToFileWriteStream(region: string, object: Object, fileWriteStream: WriteStream,
                                       offset: number, totalObjectSize: number, domain?: Domain, getFileOption?: GetFileOption): Promise<GetResult> {
        return new Promise((resolve, reject) => {
            this.adapter.getObjectStream(region, object, domain, { rangeStart: offset }).then((reader) => {
                let receivedDataBytes = offset;
                let thisPartSize = 0;
                let tid: number | undefined = undefined;
                let chain: Readable | Writable = reader.on('data', (chunk) => {
                    if (this.aborted) {
                        reject(Downloader.userCanceledError);
                        return;
                    }

                    receivedDataBytes += chunk.length;
                    if (getFileOption?.chunkTimeout) {
                        if (tid) {
                            clearTimeout(tid);
                            tid = undefined;
                        }
                        tid = <any>setTimeout(() => {
                            if (!reader.destroyed) {
                                reader.destroy(new Error('Timeout'));
                            }
                        }, getFileOption.chunkTimeout);
                    }
                    if (getFileOption?.getCallback?.progressCallback) {
                        getFileOption.getCallback.progressCallback(receivedDataBytes, totalObjectSize);
                    }
                    if (getFileOption?.partSize && getFileOption?.getCallback?.partGetCallback) {
                        thisPartSize += chunk.length;
                        if (thisPartSize > getFileOption.partSize) {
                            getFileOption.getCallback.partGetCallback(thisPartSize);
                            thisPartSize = 0;
                        }
                    }
                }).on('error', (err) => {
                    if (this.aborted) {
                        return;
                    }
                    resolve({ downloaded: receivedDataBytes, error: err });
                });
                if (getFileOption?.downloadThrottleOption) {
                    const throttleGroup = getFileOption?.downloadThrottleGroup ?? new ThrottleGroup(getFileOption.downloadThrottleOption);
                    chain = chain.pipe(throttleGroup.throttle(getFileOption.downloadThrottleOption));
                }
                chain.pipe(fileWriteStream).on('error', (err) => {
                    if (this.aborted) {
                        return;
                    }
                    resolve({ downloaded: receivedDataBytes, error: err });
                }).on('finish', () => {
                    if (this.aborted) {
                        return;
                    }
                    resolve({ downloaded: receivedDataBytes });
                });
            }, (err) => {
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
