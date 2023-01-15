import { FileHandle } from 'fs/promises';
import { Throttle, ThrottleGroup, ThrottleOptions } from 'stream-throttle';

import { Ref } from './types';
import { HttpClient } from './http-client';
import {
    Adapter,
    Part,
    ProgressCallback,
    SetObjectHeader,
    StorageObject,
} from './adapter';

export class Uploader {
    static readonly userCanceledError = new Error('User Canceled');
    static readonly chunkTimeoutError = new Error('Chunk Timeout');

    static readonly defaultChunkTimeout = 3000; // 3s
    private abortController?: AbortController;
    private chunkTimeoutTimer?: number;

    constructor(private readonly adapter: Adapter) {
    }

    async putObjectFromFile(
        region: string,
        object: StorageObject,
        file: FileHandle,
        fileSize: number,
        originalFileName: string,
        putFileOption?: PutFileOption,
    ): Promise<void> {
        this.abortController = new AbortController();

        if (this.aborted) {
            throw Uploader.userCanceledError;
        }

        // handle chunk timeout by wrap progress callback
        const progressCallbackError: Ref<Error> = {};
        if (
            putFileOption?.putCallback?.progressCallback
        ) {
            putFileOption.putCallback.progressCallback = this.wrapProgressCallback(
                putFileOption.putCallback.progressCallback,
                progressCallbackError,
                {
                    chunkTimeout: putFileOption.chunkTimeout,
                    filePath: putFileOption.filePath,
                },
            );
        }

        // handle upload
        try {
            // should use form upload
            const partSize = putFileOption?.partSize ?? (1 << 22);
            const partsCount = partsCountOfFile(fileSize, partSize);
            if (putFileOption?.uploadThreshold && fileSize <= putFileOption.uploadThreshold || partsCount <= 1) {
                await this.putObject(region, object, file, fileSize, originalFileName, putFileOption);
                return;
            }

            // should use multiple parts upload
            // init parts
            const recovered = await this.initParts(region, object, originalFileName, putFileOption);
            putFileOption?.putCallback?.partsInitCallback?.({
                uploadId: recovered.uploadId,
                parts: recovered.parts.map(p => ({ ...p })), // deep copy in case of changed outer
            });
            const uploaded = uploadedSizeOfParts(recovered.parts, fileSize, partSize);

            // upload parts
            await this.uploadParts(
                region,
                object,
                file,
                fileSize,
                uploaded,
                recovered,
                1,
                partsCount,
                partSize,
                putFileOption || {},
            );

            recovered.parts.sort((part1, part2) => part1.partNumber - part2.partNumber);
            await this.adapter.completeMultipartUpload(
                region,
                object,
                recovered.uploadId,
                recovered.parts,
                originalFileName,
                putFileOption?.header,
                this.abortController.signal,
            );
        } catch (e) {
            if (e === HttpClient.userCanceledError) {
                if (progressCallbackError.current) {
                    throw progressCallbackError.current;
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
        }
    }

    private get aborted(): boolean {
        return this.abortController?.signal.aborted ?? false;
    }

    abort(): void {
        this.abortController?.abort();
    }

    private async putObject(
        region: string,
        object: StorageObject,
        file: FileHandle,
        fileSize: number,
        originalFileName: string,
        putFileOption?: PutFileOption,
    ): Promise<void> {
        const data = Buffer.alloc(fileSize);
        const { bytesRead } = await file.read(data, 0, fileSize, 0);

        if (this.aborted) {
            throw Uploader.userCanceledError;
        }

        let throttle: Throttle | undefined;
        if (putFileOption?.uploadThrottleOption) {
            const throttleGroup = putFileOption?.uploadThrottleGroup ?? new ThrottleGroup(putFileOption.uploadThrottleOption);
            throttle = throttleGroup.throttle(putFileOption.uploadThrottleOption);
        }
        await this.adapter.putObject(
            region,
            object,
            data.subarray(0, bytesRead),
            originalFileName,
            putFileOption?.header,
            {
                progressCallback: (uploaded: number, total: number) => {
                    if (this.aborted) {
                        return;
                    }
                    putFileOption?.putCallback?.progressCallback?.(uploaded, total);
                },
                throttle,
                crc32: putFileOption?.crc32,
                abortSignal: this.abortController?.signal,
                fileStreamSetting: putFileOption?.filePath
                    ? {
                        path: putFileOption.filePath,
                        start: 0,
                        end: Infinity,
                    }
                    : undefined,
            },
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

    private async uploadParts(
        region: string,
        object: StorageObject,
        file: FileHandle,
        fileSize: number,
        uploaded: number,
        recovered: RecoveredOption,
        partNumber: number,
        partsCount: number,
        partSize: number,
        putFileOption: PutFileOption,
    ): Promise<void> {
        if (partNumber > partsCount) {
            return;
        }

        if (this.aborted) {
            throw Uploader.userCanceledError;
        }

        if (findPartsByNumber(recovered.parts, partNumber)) {
            await this.uploadParts(
                region,
                object,
                file,
                fileSize,
                uploaded,
                recovered,
                partNumber + 1,
                partsCount,
                partSize,
                putFileOption,
            );
            return;
        }

        let data: Buffer | undefined = Buffer.alloc(partSize);
        const start = partSize * (partNumber - 1);
        const end = start + partSize;
        const { bytesRead } = await file.read(data, 0, partSize, start);
        if (this.aborted) {
            throw Uploader.userCanceledError;
        }

        const makeThrottle = (): Throttle | undefined => {
            if (!putFileOption.uploadThrottleOption) {
                return;
            }
            if (!putFileOption.uploadThrottleGroup) {
                putFileOption.uploadThrottleGroup = new ThrottleGroup(putFileOption.uploadThrottleOption);
            }
            return putFileOption.uploadThrottleGroup.throttle(putFileOption.uploadThrottleOption);
        };

        let progressCallback: ProgressCallback | undefined;
        if (putFileOption.putCallback?.progressCallback) {
            progressCallback = (partUploaded: number, _partTotal: number) => {
                if (this.aborted) {
                    return;
                }
                putFileOption.putCallback?.progressCallback?.(uploaded + partUploaded, fileSize);
            };
        }
        const output = await this.adapter.uploadPart(
            region,
            object,
            recovered.uploadId,
            partNumber,
            data!.subarray(0, bytesRead),
            {
                progressCallback,
                throttle: makeThrottle(),
                abortSignal: this.abortController?.signal,
                fileStreamSetting: putFileOption.filePath
                    ? {
                        path: putFileOption.filePath,
                        start,
                        end,
                    }
                    : undefined,
            },
        );
        if (this.aborted) {
            throw Uploader.userCanceledError;
        }

        data = undefined;
        const part: Part = { etag: output.etag, partNumber };
        putFileOption?.putCallback?.partPutCallback?.(part);
        recovered.parts.push(part);
        uploaded += bytesRead;
        await this.uploadParts(
            region,
            object,
            file,
            fileSize,
            uploaded,
            recovered,
            partNumber + 1,
            partsCount,
            partSize,
            putFileOption,
        );
    }

    /**
     * warp progress callback for some error handle.
     * such as chunk timeout.
     */
    private wrapProgressCallback(
        callback: ProgressCallback,
        errorRef: Ref<Error>,
        options: {
            chunkTimeout?: number,
            filePath?: string,
        },
    ): ProgressCallback {
        const chunkTimeout = options.chunkTimeout ?? Uploader.defaultChunkTimeout;
        if (!options.filePath) {
            // S3 doesn't support chunk time when http if not provide filePath
            return callback;
        }
        if (chunkTimeout <= 0) {
            return callback;
        }
        return (...args) => {
            if (this.chunkTimeoutTimer) {
                clearTimeout(this.chunkTimeoutTimer);
            }
            this.chunkTimeoutTimer = setTimeout(() => {
                if (this.aborted) {
                    return;
                }
                errorRef.current = Uploader.chunkTimeoutError;
                this.chunkTimeoutTimer = undefined;
                this.abort();
            }, chunkTimeout) as unknown as number;
            callback(...args);
        };
    }
}

export interface PutCallback {
    progressCallback?: ProgressCallback;
    partsInitCallback?: (initInfo: RecoveredOption) => void;
    partPutCallback?: (part: Part) => void;
}

export interface PutFileOption {
    header?: SetObjectHeader;
    recovered?: RecoveredOption,
    putCallback?: PutCallback;
    partSize?: number;
    uploadThreshold?: number;
    uploadThrottleGroup?: ThrottleGroup;
    uploadThrottleOption?: ThrottleOptions;
    crc32?: string;
    chunkTimeout?: number;
    filePath?: string;
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
