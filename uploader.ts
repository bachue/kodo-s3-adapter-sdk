import { Adapter, SetObjectHeader, Part, ProgressCallback, Object } from './adapter';
import { FileHandle } from 'fs/promises';
import { Throttle, ThrottleGroup, ThrottleOptions } from 'stream-throttle';

export class Uploader {
    private aborted = false;
    private static readonly userCanceledError = new Error('User Canceled');

    constructor(private readonly adapter: Adapter) {
    }

    putObjectFromFile(region: string, object: Object, file: FileHandle, fileSize: number, originalFileName: string, putFileOption?: PutFileOption): Promise<void> {
        this.aborted = false;

        return new Promise((resolve, reject) => {
            if (this.aborted) {
                reject(Uploader.userCanceledError);
                return;
            }
            const partSize = putFileOption?.partSize ?? (1 << 22);
            const partsCount = partsCountOfFile(fileSize, partSize);

            if (putFileOption?.uploadThreshold && fileSize <= putFileOption!.uploadThreshold || partsCount <= 1) {
                this.putObject(region, object, file, fileSize, originalFileName, putFileOption).then(resolve).catch(reject);
                return;
            }

            this.initParts(region, object, originalFileName, putFileOption).then((recovered) => {
                if (this.aborted) {
                    reject(Uploader.userCanceledError);
                    return;
                }

                if (putFileOption?.putCallback?.partsInitCallback) {
                    try {
                        putFileOption.putCallback.partsInitCallback(recovered);
                    } catch (err) {
                        reject(err);
                        return;
                    }
                }

                const uploaded = uploadedSizeOfParts(recovered.parts, fileSize, partSize);
                this.uploadParts(region, object, file, fileSize, uploaded, recovered, 1, partsCount, partSize, putFileOption || {}).then(() => {
                    if (this.aborted) {
                        reject(Uploader.userCanceledError);
                        return;
                    }

                    recovered.parts.sort((part1, part2) => part1.partNumber - part2.partNumber);
                    this.adapter.completeMultipartUpload(region, object, recovered.uploadId, recovered.parts, originalFileName, putFileOption?.header)
                                .then(resolve).catch(reject);
                }).catch(reject);
            }).catch(reject);
        });
    }

    abort(): void {
        this.aborted = true;
    }

    private putObject(region: string, object: Object, file: FileHandle, fileSize: number, originalFileName: string, putFileOption?: PutFileOption): Promise<void> {
        return new Promise((resolve, reject) => {
            const data = Buffer.alloc(fileSize);
            file.read(data, 0, fileSize, 0).then(({ bytesRead }) => {
                if (this.aborted) {
                    reject(Uploader.userCanceledError);
                    return;
                }

                let throttle: Throttle | undefined;
                if (putFileOption?.uploadThrottleOption) {
                    const throttleGroup = putFileOption?.uploadThrottleGroup ?? new ThrottleGroup(putFileOption.uploadThrottleOption);
                    throttle = throttleGroup.throttle(putFileOption.uploadThrottleOption);
                }
                this.adapter.putObject(region, object, data.subarray(0, bytesRead), originalFileName,
                                       putFileOption?.header, {
                                           progressCallback: putFileOption?.putCallback?.progressCallback,
                                           throttle,
                                       }).then(resolve).catch(reject);
            }).catch(reject);
        });
    }

    private initParts(region: string, object: Object, originalFileName: string, putFileOption?: PutFileOption): Promise<RecoveredOption> {
        return new Promise((resolve, reject) => {
            const recovered: RecoveredOption = { uploadId: '', parts: [] };

            if (putFileOption?.recovered && checkParts(putFileOption.recovered.parts)) {
                recovered.uploadId = putFileOption.recovered.uploadId;
                recovered.parts = recovered.parts.concat(putFileOption.recovered.parts);
                resolve(recovered);
            } else {
                this.adapter.createMultipartUpload(region, object, originalFileName, putFileOption?.header).then((initPartsOutput) => {
                    recovered.uploadId = initPartsOutput.uploadId;
                    resolve(recovered);
                }).catch(reject);
            }
        });
    }

    private uploadParts(region: string, object: Object, file: FileHandle, fileSize: number, uploaded: number, recovered: RecoveredOption,
                        partNumber: number, partsCount: number, partSize: number, putFileOption: PutFileOption): Promise<void> {
        return new Promise((resolve, reject) => {
            if (partNumber > partsCount) {
                resolve();
                return;
            }

            if (this.aborted) {
                reject(Uploader.userCanceledError);
                return;
            }

            if (findPartsByNumber(recovered.parts, partNumber)) {
                this.uploadParts(region, object, file, fileSize, uploaded, recovered,
                                 partNumber + 1, partsCount, partSize, putFileOption)
                    .then(resolve).catch(reject);
            } else {
                let data: Buffer | undefined = Buffer.alloc(partSize);
                file.read(data, 0, partSize, partSize * (partNumber - 1)).then(({ bytesRead }) => {
                    if (this.aborted) {
                        reject(Uploader.userCanceledError);
                        return;
                    }

                    const makeThrottle = (): Throttle | undefined => {
                        if (putFileOption.uploadThrottleOption) {
                            if (!putFileOption.uploadThrottleGroup) {
                                putFileOption.uploadThrottleGroup = new ThrottleGroup(putFileOption.uploadThrottleOption);
                            }
                            return putFileOption.uploadThrottleGroup.throttle(putFileOption.uploadThrottleOption);
                        }
                        return undefined;
                    };

                    let progressCallback: ProgressCallback | undefined;
                    if (putFileOption.putCallback?.progressCallback) {
                        progressCallback = (partUploaded: number, _partTotal: number) => {
                            putFileOption.putCallback!.progressCallback!(uploaded + partUploaded, fileSize);
                        };
                    }
                    this.adapter.uploadPart(region, object, recovered.uploadId, partNumber,
                                            data!.subarray(0, bytesRead), {
                                                progressCallback,
                                                throttle: makeThrottle(),
                                            }).then((output) => {
                        data = undefined;
                        const part: Part = { etag: output.etag, partNumber };
                        if (putFileOption?.putCallback?.partPutCallback) {
                            try {
                                putFileOption.putCallback.partPutCallback(part);
                            } catch (err) {
                                reject(err);
                                return;
                            }
                        }
                        recovered.parts.push(part);
                        uploaded += bytesRead;
                        this.uploadParts(region, object, file, fileSize, uploaded, recovered,
                                         partNumber + 1, partsCount, partSize, putFileOption)
                            .then(resolve).catch(reject);
                    }).catch(reject);
                }).catch(reject);
            }
        });
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
    return parts.find((part) => part.partNumber === partNumber);
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
