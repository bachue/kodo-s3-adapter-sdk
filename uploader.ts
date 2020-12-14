import { Adapter, SetObjectHeader, Part, ProgressCallback, Object } from './adapter';
import { FileHandle } from 'fs/promises';

export class Uploader {
    constructor(private readonly adapter: Adapter) {
    }

    putObjectFromFile(region: string, object: Object, file: FileHandle, putFileOption?: PutFileOption): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.all([this.initParts(region, object, putFileOption), file.stat()]).then(([recovered, fileStat]) => {
                const fileSize = fileStat.size;
                const partSize = putFileOption?.partSize ?? (1 << 22);
                const partsCount = partsCountOfFile(fileSize, partSize);

                if (partsCount <= 1) {
                    this.putObject(region, object, file, fileSize, putFileOption).then(resolve, reject);
                    return;
                }

                const uploaded = uploadedSizeOfParts(recovered.parts, fileSize, partSize);
                this.uploadParts(region, object, file, fileSize, uploaded, recovered, 1, partsCount, partSize, putFileOption).then(() => {
                    recovered.parts.sort((part1, part2) => part1.partNumber - part2.partNumber);
                    this.adapter.completeMultipartUpload(region, object, recovered.uploadId, recovered.parts, putFileOption?.header)
                                .then(resolve, reject);
                }, reject);
            }, reject);
        });
    }

    private putObject(region: string, object: Object, file: FileHandle, fileSize: number, putFileOption?: PutFileOption): Promise<void> {
        return new Promise((resolve, reject) => {
            const data = Buffer.alloc(fileSize);
            file.read(data, 0, fileSize, 0).then(({ bytesRead }) => {
                this.adapter.putObject(region, object, data.subarray(0, bytesRead),
                                       putFileOption?.header, putFileOption?.putCallback?.progressCallback)
                            .then(resolve, reject);
            }, reject);
        });
    }

    private initParts(region: string, object: Object, putFileOption?: PutFileOption): Promise<RecoveredOption> {
        return new Promise((resolve, reject) => {
            const recovered: RecoveredOption = { uploadId: '', parts: [] };

            if (putFileOption?.recovered && checkParts(putFileOption.recovered.parts)) {
                recovered.uploadId = putFileOption.recovered.uploadId;
                recovered.parts = recovered.parts.concat(putFileOption.recovered.parts);
                resolve(recovered);
            } else {
                this.adapter.createMultipartUpload(region, object, putFileOption?.header).then((initPartsOutput) => {
                    recovered.uploadId = initPartsOutput.uploadId;
                    resolve(recovered);
                }, reject);
            }
        });
    }

    private uploadParts(region: string, object: Object, file: FileHandle, fileSize: number, uploaded: number, recovered: RecoveredOption,
                        partNumber: number, partsCount: number, partSize: number, putFileOption?: PutFileOption): Promise<void> {
        return new Promise((resolve, reject) => {
            if (partNumber > partsCount) {
                resolve();
                return;
            }

            if (findPartsByNumber(recovered.parts, partNumber)) {
                this.uploadParts(region, object, file, fileSize, uploaded, recovered,
                                 partNumber + 1, partsCount, partSize, putFileOption)
                    .then(resolve, reject);
            } else {
                const data = Buffer.alloc(partSize);
                file.read(data, 0, partSize, partSize * (partNumber - 1)).then(({ bytesRead }) => {
                    let progressCallback: ProgressCallback | undefined = undefined;
                    if (putFileOption?.putCallback?.progressCallback) {
                        progressCallback = (partUploaded: number, _partTotal: number) => {
                            putFileOption!.putCallback!.progressCallback!(uploaded + partUploaded, fileSize);
                        };
                    }
                    this.adapter.uploadPart(region, object, recovered.uploadId, partNumber,
                                            data.subarray(0, bytesRead), progressCallback).then((output) => {
                        const part: Part = { etag: output.etag, partNumber: partNumber };
                        if (putFileOption?.putCallback?.partPutCallback) {
                            putFileOption.putCallback.partPutCallback(part);
                        }
                        recovered.parts.push(part);
                        uploaded += bytesRead;
                        this.uploadParts(region, object, file, fileSize, uploaded, recovered,
                                         partNumber + 1, partsCount, partSize, putFileOption)
                            .then(resolve, reject);
                    });
                }, reject);
            }
        });
    }
}

export interface PutCallback {
    progressCallback?: ProgressCallback;
    partPutCallback?: (part: Part) => void;
}

export interface PutFileOption {
    header?: SetObjectHeader;
    recovered?: RecoveredOption,
    putCallback?: PutCallback;
    partSize?: number;
}

export interface RecoveredOption {
    uploadId: string,
    parts: Array<Part>,
}

function checkParts(parts: Array<Part>): boolean {
    const partNumbers = new Set<number>();

    for (const part of parts) {
        partNumbers.add(part.partNumber);
    }

    return partNumbers.size === parts.length;
}

function findPartsByNumber(parts: Array<Part>, partNumber: number): Part | undefined {
    return parts.find((part) => part.partNumber === partNumber);
}

function partsCountOfFile(fileSize: number, partSize: number): number {
    const count = (fileSize + partSize - 1) / partSize;
    return ~~count;
}

function uploadedSizeOfParts(parts: Array<Part>, fileSize: number, partSize: number): number {
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
