import { createReadStream } from 'fs';
import { Readable, Transform, Writable } from 'stream';
import { pipeline } from 'stream/promises';
import { ThrottleGroup, ThrottleOptions } from 'stream-throttle';
import { Semaphore } from 'semaphore-promise';

import { Ref } from './types';
import { Progress, SpeedMonitor } from './progress-stream';
import { HttpClient } from './http-client';
import {
    Adapter,
    Part,
    SetObjectHeader,
    StorageObject,
} from './adapter';

export class Uploader {
    static readonly userCanceledError = new Error('User Canceled');
    static readonly chunkTimeoutError = new Error('Chunk Timeout');

    static readonly defaultChunkTimeout = 3000; // 3s
    static speedInterval = 1000; // 1s
    static speedWindowSize = 16; // window duration is 16 * speedInterval
    private abortController?: AbortController;
    private speedMonitor?: SpeedMonitor;
    private chunkTimeoutTimer?: number;

    constructor(private readonly adapter: Adapter) {
    }

    async putObjectFromFile(
        region: string,
        object: StorageObject,
        filePath: string,
        fileSize: number,
        originalFileName: string,
        putFileOption?: PutFileOption,
    ): Promise<void> {
        this.abortController = new AbortController();
        const putProgressError: Ref<Error> = {};

        // handle upload
        try {
            // should use form upload
            const partSize = putFileOption?.partSize ?? (1 << 22);
            const partsCount = partsCountOfFile(fileSize, partSize);
            if (putFileOption?.uploadThreshold && fileSize <= putFileOption.uploadThreshold || partsCount <= 1) {
                await this.putObject(
                    region,
                    object,
                    filePath,
                    fileSize,
                    originalFileName,
                    putProgressError,
                    putFileOption,
                );
                return;
            }

            // should use multiple parts upload
            await this.multiplePartsUpload(
                region,
                object,
                filePath,
                fileSize,
                originalFileName,
                putProgressError,
                putFileOption,
            );
        } catch (e) {
            if (e === HttpClient.userCanceledError) {
                if (putProgressError.current) {
                    throw putProgressError.current;
                }
                throw Uploader.userCanceledError;
            } else {
                throw e;
            }
        } finally {
            if (this.chunkTimeoutTimer) {
                clearTimeout(this.chunkTimeoutTimer);
                this.chunkTimeoutTimer = undefined;
            }
            if (this.speedMonitor) {
                this.speedMonitor.destroy();
                this.speedMonitor = undefined;
            }
        }
    }

    private get aborted(): boolean {
        return this.abortController?.signal.aborted ?? false;
    }

    abort(): void {
        this.speedMonitor?.destroy();
        this.abortController?.abort();
    }

    private async putObject(
        region: string,
        object: StorageObject,
        filePath: string,
        fileSize: number,
        originalFileName: string,
        putProgressError: Ref<Error>,
        putFileOption?: PutFileOption,
    ): Promise<void> {
        const data = this.getPutReader({
            filePath,
            putProgressError,
            putFileOption,
        });
        this.speedMonitor = this.getSpeedMonitor({
            totalSize: fileSize,
            start: 0,
            progressCallback: putFileOption?.putCallback?.progressCallback,
        });
        let lastUploaded = 0;
        try {
            // send request
            await this.adapter.putObject(
                region,
                object,
                data,
                originalFileName,
                putFileOption?.header,
                {
                    abortSignal: this.abortController?.signal,
                    fileStreamSetting: {
                        path: filePath,
                        start: 0,
                        end: Infinity,
                    },
                    progressCallback: (uploaded) => {
                        this.speedMonitor?.updateProgress(uploaded - lastUploaded);
                        lastUploaded = uploaded;
                    },
                    beforeRequestCallback: () => {
                        // when calc md5/crc32 do not monitor to avoid chunk timeout too sensitive
                        this.speedMonitor?.start();
                    }
                },
            );
        } finally {
            this.speedMonitor.destroy();
            this.speedMonitor = undefined;
        }
    }

    private async multiplePartsUpload(
        region: string,
        object: StorageObject,
        filePath: string,
        fileSize: number,
        originalFileName: string,
        putProgressError: Ref<Error>,
        putFileOption: PutFileOption = {},
    ) {
        const partSize = putFileOption.partSize ?? (1 << 22);
        const partMaxConcurrency = putFileOption.partMaxConcurrency ?? 1;
        const partsCount = partsCountOfFile(fileSize, partSize);

        // init parts
        const recovered = await this.initParts(region, object, originalFileName, putFileOption);
        putFileOption.putCallback?.partsInitCallback?.({
            uploadId: recovered.uploadId,
            parts: recovered.parts.map(p => ({ ...p })), // deep copy in case of changed outer
        });
        const uploaded = uploadedSizeOfParts(recovered.parts, fileSize, partSize);

        // upload parts
        await this.uploadParts({
            region,
            object,
            filePath,
            fileSize,
            uploaded,
            recovered,
            partsCount,
            partSize,
            partMaxConcurrency,
            putProgressError,
            putFileOption,
        });

        recovered.parts.sort((part1, part2) => part1.partNumber - part2.partNumber);
        await this.adapter.completeMultipartUpload(
            region,
            object,
            recovered.uploadId,
            recovered.parts,
            originalFileName,
            putFileOption?.header,
            this.abortController?.signal,
        );
    }

    private async initParts(
        region: string,
        object: StorageObject,
        originalFileName: string,
        putFileOption?: PutFileOption,
    ): Promise<RecoveredOption> {
        if (this.aborted) {
            throw Uploader.userCanceledError;
        }

        const recovered: RecoveredOption = { uploadId: '', parts: [] };

        if (putFileOption?.recovered?.uploadId && checkParts(putFileOption.recovered.parts)) {
            recovered.uploadId = putFileOption.recovered.uploadId;
            recovered.parts = recovered.parts.concat(putFileOption.recovered.parts);
            return recovered;
        }
        const initPartsOutput = await this.adapter.createMultipartUpload(
            region,
            object,
            originalFileName,
            putFileOption?.header,
            this.abortController?.signal,
        );
        recovered.uploadId = initPartsOutput.uploadId;
        return recovered;
    }

    private async uploadParts({
        region,
        object,
        filePath,
        fileSize,
        uploaded,
        recovered,
        partsCount,
        partSize,
        partMaxConcurrency,
        putProgressError,
        putFileOption,
    }: {
        region: string,
        object: StorageObject,
        filePath: string,
        fileSize: number,
        uploaded: number,
        recovered: RecoveredOption,
        partsCount: number,
        partSize: number,
        partMaxConcurrency: number,
        putProgressError: Ref<Error>,
        putFileOption: PutFileOption,
    }): Promise<void> {
        if (this.aborted) {
            throw Uploader.userCanceledError;
        }

        this.speedMonitor = this.getSpeedMonitor({
            totalSize: fileSize,
            start: uploaded,
            progressCallback: putFileOption.putCallback?.progressCallback,
        });

        try {
            const limiter = new Semaphore(partMaxConcurrency);
            const restPromise: Set<Promise<Part>> = new Set();
            let error: Error | null = null;
            for (let partNum = 1; partNum <= partsCount; partNum += 1) {
                if (findPartsByNumber(recovered.parts, partNum)) {
                    continue;
                }

                const releaseLimiter = await limiter.acquire();
                if (error) {
                    throw error;
                }
                const p = this.uploadPart({
                    region,
                    object,
                    filePath,
                    fileSize,
                    uploadId: recovered.uploadId,
                    partNum,
                    partSize,
                    putProgressError,
                    putFileOption,
                });
                restPromise.add(p);
                p.then(part => {
                    restPromise.delete(p);
                    recovered.parts.push(part);
                })
                    .catch(err => {
                        if (!error) {
                            error = err;
                        }
                    })
                    .finally(releaseLimiter);
            }
            await Promise.all(restPromise);
            if (error) {
                throw error;
            }
        } finally {
            this.speedMonitor.destroy();
            this.speedMonitor = undefined;
        }
    }

    private async uploadPart({
        region,
        object,
        filePath,
        fileSize,
        uploadId,
        partNum,
        partSize,
        putProgressError,
        putFileOption,
    }: {
        region: string,
        object: StorageObject,
        filePath: string,
        fileSize: number,
        uploadId: string,
        partNum: number,
        partSize: number,
        putProgressError: Ref<Error>,
        putFileOption: PutFileOption,
    }): Promise<Part> {
        const start = partSize * (partNum - 1);
        const end = Math.min(
            start + partSize - 1,
            fileSize - 1,
        );
        const fileReader = this.getPutReader({
            filePath,
            start,
            end,
            putProgressError,
            putFileOption,
        });

        let lastUploaded = 0;
        const uploadPartResp = await this.adapter.uploadPart(
            region,
            object,
            uploadId,
            partNum,
            fileReader,
            {
                abortSignal: this.abortController?.signal,
                fileStreamSetting: {
                    path: filePath,
                    start,
                    end,
                },
                progressCallback: (uploaded) => {
                    this.speedMonitor?.updateProgress(uploaded - lastUploaded);
                    lastUploaded = uploaded;
                },
                beforeRequestCallback: () => {
                    // when calc md5/crc32 do not monitor to avoid chunk timeout too sensitive
                    this.speedMonitor?.start();
                }
            },
        );

        const part: Part = { etag: uploadPartResp.etag, partNumber: partNum };
        putFileOption?.putCallback?.partPutCallback?.(part);
        return part;
    }

    private getChunkTimeoutMonitor(
        chunkTimeout: number,
        errorRef: Ref<Error>,
    ): Transform {
        const checkNeedTimeout = () => {
            if (this.aborted) {
                return;
            }
            if (this.speedMonitor && this.speedMonitor.speed * 1000 >= 512) {
                this.chunkTimeoutTimer = setTimeout(checkNeedTimeout, chunkTimeout) as unknown as number;
                return;
            }
            errorRef.current = Uploader.chunkTimeoutError;
            this.chunkTimeoutTimer = undefined;
            this.abort();
        };
        return new Transform({
            transform: (chunk, _encoding, callback) => {
                if (this.chunkTimeoutTimer) {
                    clearTimeout(this.chunkTimeoutTimer);
                }
                this.chunkTimeoutTimer = setTimeout(checkNeedTimeout, chunkTimeout) as unknown as number;
                callback(null, chunk);
            }
        });
    }

    private getSpeedMonitor({
        totalSize,
        start = 0,
        progressCallback,
    }: {
        totalSize: number,
        start?: number,
        progressCallback?: (progress: Progress) => void,
    }): SpeedMonitor {
        const result = new SpeedMonitor({
            total: totalSize,
            transferred: start,
            interval: Uploader.speedInterval,
            windowSize: Uploader.speedWindowSize,
            autoStart: false,
        });
        if (progressCallback) {
            result.on('progress', p => progressCallback(p));
        }
        return result;
    }

    private getPutReader({
        filePath,
        start,
        end,
        putProgressError,
        putFileOption,
    }: {
        filePath: string,
        start?: number,
        end?: number,
        putProgressError: Ref<Error>,
        putFileOption?: PutFileOption,
    }): Readable {
        const pipeList: (Readable | Writable)[] = [];

        // reader
        const fileReader = createReadStream(filePath, {
            start,
            end,
        });
        pipeList.push(fileReader);

        // throttle
        if (putFileOption?.uploadThrottleOption) {
            const throttleGroup = putFileOption?.uploadThrottleGroup ?? new ThrottleGroup(putFileOption.uploadThrottleOption);
            const throttle = throttleGroup.throttle(putFileOption.uploadThrottleOption);
            pipeList.push(throttle);
        }

        // chunk timeout
        pipeList.push(this.getChunkTimeoutMonitor(
            Uploader.defaultChunkTimeout,
            putProgressError,
        ));

        // check last is reader
        const result = pipeList[pipeList.length - 1];
        if (!(result instanceof Readable)) {
            throw new Error('The last element in pipe list must be Readable');
        }

        // pipe
        pipeline(pipeList)
            .catch(err => {
                putProgressError.current = err;
            });
        return result;
    }
}

export interface PutCallback {
    progressCallback?: (progress: Progress) => void;
    partsInitCallback?: (initInfo: RecoveredOption) => void;
    partPutCallback?: (part: Part) => void;
}

export interface PutFileOption {
    header?: SetObjectHeader;
    recovered?: RecoveredOption,
    putCallback?: PutCallback;
    partSize?: number;
    partMaxConcurrency?: number;
    uploadThreshold?: number;
    uploadThrottleGroup?: ThrottleGroup;
    uploadThrottleOption?: ThrottleOptions;
    chunkTimeout?: number;
}

export interface RecoveredOption {
    uploadId: string,
    parts: Part[],
}

function checkParts(parts: Part[]): boolean {
    const partNumbers = new Set<number>();

    for (const part of parts) {
        partNumbers.add(part.partNumber);
    }

    return partNumbers.size === parts.length;
}

function findPartsByNumber(parts: Part[], partNumber: number): Part | undefined {
    return parts.find((part) => part?.partNumber === partNumber);
}

function partsCountOfFile(fileSize: number, partSize: number): number {
    const count = (fileSize + partSize - 1) / partSize;
    return ~~count;
}

function uploadedSizeOfParts(parts: Part[], fileSize: number, partSize: number): number {
    const partsCount = partsCountOfFile(fileSize, partSize);
    let uploaded = 0;
    parts.forEach((part) => {
        uploaded += partSize;
        if (part.partNumber === partsCount) {
            uploaded -= (partSize * partsCount - fileSize);
        }
    });
    return uploaded;
}
