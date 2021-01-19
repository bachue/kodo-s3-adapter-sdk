import AsyncLock from 'async-lock';
import AWS from 'aws-sdk';
import os from 'os';
import pkg from './package.json';
import md5 from 'js-md5';
import { URL } from 'url';
import { Readable } from 'stream';
import { Semaphore } from 'semaphore-promise';
import { RegionService } from './region_service';
import { Kodo } from './kodo';
import { ReadableStreamBuffer } from 'stream-buffers';
import { Adapter, AdapterOption, Bucket, Domain, Object, SetObjectHeader, ObjectGetResult, ObjectHeader,
         TransferObject, PartialObjectError, BatchCallback, FrozenInfo, ListedObjects, ListObjectsOption, PutObjectOption,
         InitPartsOutput, UploadPartOutput, StorageClass, Part, GetObjectStreamOption } from './adapter';

export const USER_AGENT: string = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/s3`;

export class S3 implements Adapter {
    private readonly bucketNameToIdCache: { [name: string]: string; } = {};
    private readonly bucketIdToNameCache: { [id: string]: string; } = {};
    private readonly clients: { [key: string]: AWS.S3; } = {};
    private readonly bucketNameToIdCacheLock = new AsyncLock();
    private readonly clientsLock = new AsyncLock();
    private readonly kodo: Kodo;
    private readonly regionService: RegionService;

    constructor(private readonly adapterOption: AdapterOption) {
        this.kodo = new Kodo(adapterOption);
        this.regionService = new RegionService(adapterOption);
    }

    private getClient(s3RegionId?: string): Promise<AWS.S3> {
        return new Promise((resolve, reject) => {
            const cacheKey = s3RegionId ?? '';
            if (this.clients[cacheKey]) {
                resolve(this.clients[cacheKey]);
                return;
            }
            this.clientsLock.acquire(cacheKey, (): Promise<AWS.S3> => {
                return new Promise((resolve, reject) => {
                    let userAgent = USER_AGENT;
                    if (this.adapterOption.appendedUserAgent) {
                        userAgent += `/${this.adapterOption.appendedUserAgent}`;
                    }
                    this.regionService.getS3Endpoint(s3RegionId).then((s3IdEndpoint) => {
                        resolve(new AWS.S3({
                            apiVersion: "2006-03-01",
                            customUserAgent: userAgent,
                            computeChecksums: true,
                            region: s3IdEndpoint.s3Id,
                            endpoint: s3IdEndpoint.s3Endpoint,
                            accessKeyId: this.adapterOption.accessKey,
                            secretAccessKey: this.adapterOption.secretKey,
                            // logger: console, TODO: Use Adapter Option here
                            maxRetries: 10,
                            s3ForcePathStyle: true,
                            signatureVersion: "v4",
                            useDualstack: true,
                            httpOptions: {
                                connectTimeout: 30000,
                                timeout: 300000,
                            }
                        }));
                    }, reject);
                });
            }).then((client: AWS.S3) => {
                this.clients[cacheKey] = client;
                resolve(client);
            }, reject);
        });
    }

    fromKodoBucketNameToS3BucketId(bucketName: string): Promise<string> {
        return new Promise((resolve, reject) => {
            if (this.bucketNameToIdCache[bucketName]) {
                resolve(this.bucketNameToIdCache[bucketName]);
                return;
            }
            this.bucketNameToIdCacheLock.acquire('all', (): Promise<void> => {
                return new Promise((resolve, reject) => {
                    if (this.bucketNameToIdCache[bucketName]) {
                        resolve();
                        return;
                    }
                    this.kodo.listBucketIdNames().then((buckets) => {
                        buckets.forEach((bucket) => {
                            this.bucketNameToIdCache[bucket.name] = bucket.id;
                            this.bucketIdToNameCache[bucket.id] = bucket.name;
                        });
                        resolve();
                    }, reject);
                });
            }).then(() => {
                if (this.bucketNameToIdCache[bucketName]) {
                    resolve(this.bucketNameToIdCache[bucketName]);
                } else {
                    resolve(bucketName);
                }
            }, reject);
        });
    }

    fromS3BucketIdToKodoBucketName(bucketId: string): Promise<string> {
        return new Promise((resolve, reject) => {
            if (this.bucketIdToNameCache[bucketId]) {
                resolve(this.bucketIdToNameCache[bucketId]);
                return;
            }
            this.bucketNameToIdCacheLock.acquire('all', (): Promise<void> => {
                return new Promise((resolve, reject) => {
                    if (this.bucketIdToNameCache[bucketId]) {
                        resolve();
                        return;
                    }
                    this.kodo.listBucketIdNames().then((buckets) => {
                        buckets.forEach((bucket) => {
                            this.bucketNameToIdCache[bucket.name] = bucket.id;
                            this.bucketIdToNameCache[bucket.id] = bucket.name;
                        });
                        resolve();
                    }, reject);
                });
            }).then(() => {
                if (this.bucketIdToNameCache[bucketId]) {
                    resolve(this.bucketIdToNameCache[bucketId]);
                } else {
                    reject(new Error(`Cannot find bucket name of bucket ${bucketId}`));
                }
            }, reject);
        });
    }

    createBucket(s3RegionId: string, bucket: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.getClient(s3RegionId).then((s3) => {
                s3.createBucket({
                    Bucket: bucket,
                    CreateBucketConfiguration: {
                        LocationConstraint: s3RegionId,
                    },
                }, function(err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            }, reject);
        });
    }

    deleteBucket(s3RegionId: string, bucket: string): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(bucket)]).then(([s3, bucketId]) => {
                s3.deleteBucket({ Bucket: bucketId }, function(err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            }, reject);
        });
    }

    getBucketLocation(bucket: string): Promise<string> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(), this.fromKodoBucketNameToS3BucketId(bucket)]).then(([s3, bucketId]) => {
                this._getBucketLocation(s3, bucketId, resolve, reject);
            }, reject);
        });
    }

    private _getBucketLocation(s3: AWS.S3, bucketId: string, resolve: any, reject: any): void {
        s3.getBucketLocation({ Bucket: bucketId }, (err, data) => {
            if (err) {
                reject(err);
            } else {
                const s3RegionId: string = data.LocationConstraint!;
                resolve(s3RegionId);
            }
        });
    }

    listBuckets(): Promise<Array<Bucket>> {
        return new Promise((resolve, reject) => {
            this.getClient().then((s3) => {
                s3.listBuckets((err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        const bucketNamePromises: Array<Promise<string>> = data.Buckets!.map((info: any) => {
                            return this.fromS3BucketIdToKodoBucketName(info.Name);
                        });
                        const bucketLocationPromises: Array<Promise<string | undefined>> = data.Buckets!.map((info: any) => {
                            return new Promise((resolve) => {
                                s3.getBucketLocation({ Bucket: info.Name }, (err, data) => {
                                    if (err) {
                                        resolve(undefined);
                                    } else {
                                        resolve(data.LocationConstraint);
                                    }
                                });
                            });
                        });
                        Promise.all([Promise.all(bucketNamePromises), Promise.all(bucketLocationPromises)])
                            .then(([bucketNames, bucketLocations]) => {
                            const bucketInfos: Array<Bucket> = data.Buckets!.map((info: any, index: number) => {
                                return {
                                    id: info.Name, name: bucketNames[index],
                                    createDate: info.CreationDate,
                                    regionId: bucketLocations[index],
                                };
                            });
                            resolve(bucketInfos);
                        }, reject);
                    }
                });
            }, reject);
        });
    }

    listDomains(_s3RegionId: string, _bucket: string): Promise<Array<Domain>> {
        return Promise.resolve([]);
    }

    isExists(s3RegionId: string, object: Object): Promise<boolean> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                s3.listObjects({ Bucket: bucketId, MaxKeys: 1, Prefix: object.key }, (err, data) => {
                    if (err) {
                        reject(err);
                    } else if (data.Contents && data.Contents.length > 0) {
                        resolve(data.Contents[0].Key === object.key);
                    } else {
                        resolve(false);
                    }
                });
            }, reject);
        });
    }

    deleteObject(s3RegionId: string, object: Object): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                s3.deleteObject({ Bucket: bucketId, Key: object.key }, (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            }, reject);
        });
    }

    putObject(s3RegionId: string, object: Object, data: Buffer, originalFileName: string,
              header?: SetObjectHeader, option?: PutObjectOption): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                let dataSource: Readable | Buffer;
                if (this.adapterOption.ucUrl?.startsWith("https://") ?? true) {
                    const reader = new ReadableStreamBuffer({ initialSize: data.length, chunkSize: 1 << 20 });
                    reader.put(data);
                    reader.stop();
                    if (option?.throttle) {
                        dataSource = reader.pipe(option.throttle);
                    } else {
                        dataSource = reader;
                    }
                } else {
                    dataSource = data;
                }
                const params: AWS.S3.Types.PutObjectRequest = {
                    Bucket: bucketId,
                    Key: object.key,
                    Body: dataSource,
                    ContentLength: data.length,
                    Metadata: header?.metadata,
                    ContentDisposition: makeContentDisposition(originalFileName),
                };
                if (header?.contentType) {
                    params.ContentType = header!.contentType;
                }
                const uploader = s3.putObject(params);
                if (option?.progressCallback) {
                    uploader.on('httpUploadProgress', (progress) => {
                        option.progressCallback!(progress.loaded, progress.total);
                    });
                }
                uploader.send((err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            }, reject);
        });
    }

    getObject(s3RegionId: string, object: Object, _domain?: Domain): Promise<ObjectGetResult> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                s3.getObject({ Bucket: bucketId, Key: object.key }, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve({
                            data: Buffer.from(data.Body!),
                            header: { size: data.ContentLength!, contentType: data.ContentType!, lastModified: data.LastModified!, metadata: data.Metadata! },
                        });
                    }
                });
            }, reject);
        });
    }

    getObjectStream(s3RegionId: string, object: Object, _domain?: Domain, option?: GetObjectStreamOption): Promise<Readable> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                let range: string | undefined = undefined;
                if (option?.rangeStart || option?.rangeEnd) {
                    range = `bytes=${option?.rangeStart ?? ''}-${option?.rangeEnd ?? ''}`;
                }

                resolve(s3.getObject({ Bucket: bucketId, Key: object.key, Range: range }).createReadStream());
            }, reject);
        });
    }

    getObjectURL(s3RegionId: string, object: Object, _domain?: Domain, deadline?: Date): Promise<URL> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                let expires: number;
                if (deadline) {
                    expires = ~~((deadline.getTime() - Date.now()) / 1000);
                } else {
                    expires = 7 * 24 * 60 * 60;
                }
                const url = s3.getSignedUrl('getObject', { Bucket: bucketId, Key: object.key, Expires: expires });
                resolve(new URL(url));
            }, reject);
        });
    }

    getObjectHeader(s3RegionId: string, object: Object, _domain?: Domain): Promise<ObjectHeader> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                s3.headObject({ Bucket: bucketId, Key: object.key }, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve({ size: data.ContentLength!, contentType: data.ContentType!, lastModified: data.LastModified!, metadata: data.Metadata! });
                    }
                });
            }, reject);
        });
    }

    moveObject(s3RegionId: string, transferObject: TransferObject): Promise<void> {
        return new Promise((resolve, reject) => {
            this.copyObject(s3RegionId, transferObject).then(() => {
                this.deleteObject(s3RegionId, transferObject.from).then(resolve, (err) => {
                    err.stage = 'delete';
                    reject(err);
                });
            }, (err) => {
                err.stage = 'copy';
                reject(err);
            });
        });
    }

    copyObject(s3RegionId: string, transferObject: TransferObject): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.all([
                this.getClient(s3RegionId),
                this.getObjectStorageClass(s3RegionId, transferObject.from),
                this.fromKodoBucketNameToS3BucketId(transferObject.from.bucket),
                this.fromKodoBucketNameToS3BucketId(transferObject.to.bucket),
            ]).then(([s3, storageClass, fromBucketId, toBucketId]) => {
                const params: AWS.S3.Types.CopyObjectRequest = {
                    Bucket: toBucketId, Key: transferObject.to.key,
                    CopySource: `/${fromBucketId}/${encodeURIComponent(transferObject.from.key)}`,
                    MetadataDirective: 'COPY',
                    StorageClass: storageClass,
                };
                s3.copyObject(params, (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            }, reject);
        });
    }

    private getObjectStorageClass(s3RegionId: string, object: Object): Promise<string | undefined> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                s3.headObject({ Bucket: bucketId, Key: object.key }, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(data.StorageClass);
                    }
                });
            }, reject);
        });
    }

    moveObjects(s3RegionId: string, transferObjects: Array<TransferObject>, callback?: BatchCallback): Promise<Array<PartialObjectError>> {
        return new Promise((resolve, reject) => {
            const semaphore = new Semaphore(5);
            const promises: Array<Promise<PartialObjectError>> = transferObjects.map((transferObject, index) => {
                return new Promise((resolve, reject) => {
                    semaphore.acquire().then((release) => {
                        this.moveObject(s3RegionId, transferObject).then(() => {
                            if (callback && callback(index) === false) {
                                reject(new Error('aborted'));
                                return;
                            }
                            resolve({ bucket: transferObject.from.bucket, key: transferObject.from.key });
                        }, (err) => {
                            if (callback && callback(index, err) === false) {
                                reject(new Error('aborted'));
                                return;
                            }
                            resolve({ bucket: transferObject.from.bucket, key: transferObject.from.key, error: err });
                        }).finally(() => {
                            release();
                        });
                    });
                });
            });
            Promise.all(promises).then(resolve, reject);
        });
    }

    copyObjects(s3RegionId: string, transferObjects: Array<TransferObject>, callback?: BatchCallback): Promise<Array<PartialObjectError>> {
        return new Promise((resolve, reject) => {
            const semaphore = new Semaphore(5);
            const promises: Array<Promise<PartialObjectError>> = transferObjects.map((transferObject, index) => {
                return new Promise((resolve, reject) => {
                    semaphore.acquire().then((release) => {
                        this.copyObject(s3RegionId, transferObject).then(() => {
                            if (callback && callback(index) === false) {
                                reject(new Error('aborted'));
                                return;
                            }
                            resolve({ bucket: transferObject.from.bucket, key: transferObject.from.key });
                        }, (err) => {
                            if (callback && callback(index, err) === false) {
                                reject(new Error('aborted'));
                                return;
                            }
                            resolve({ bucket: transferObject.from.bucket, key: transferObject.from.key, error: err });
                        }).finally(() => {
                            release();
                        });
                    });
                });
            });
            Promise.all(promises).then(resolve, reject);
        });
    }

    deleteObjects(s3RegionId: string, bucket: string, keys: Array<string>, callback?: BatchCallback): Promise<Array<PartialObjectError>> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(bucket)]).then(([s3, bucketId]) => {
                const semaphore = new Semaphore(5);
                const batchCount = 100;
                const batches: Array<Array<string>> = [];
                while (keys.length >= batchCount) {
                    batches.push(keys.splice(0, batchCount));
                }
                if (keys.length > 0) {
                    batches.push(keys);
                }
                let counter = 0;
                const promises: Array<Promise<Array<PartialObjectError>>> = batches.map((batch) => {
                    const firstIndexInCurrentBatch = counter;
                    const partialObjectErrors: Array<PartialObjectError> = new Array(batch.length);
                    counter += batch.length;
                    return new Promise((resolve, reject) => {
                        semaphore.acquire().then((release) => {
                            s3.deleteObjects({
                                Bucket: bucketId,
                                Delete: {
                                    Objects: batch.map((key) => { return { Key: key }; }),
                                },
                            }, (err, results) => {
                                let aborted = false;
                                if (err) {
                                    batch.forEach((key, index) => {
                                        if (callback && callback(index + firstIndexInCurrentBatch, err) === false) {
                                            aborted = true;
                                        }
                                        partialObjectErrors[index] = { bucket: bucket, key: key, error: err };
                                    });
                                } else {
                                    if (results.Deleted) {
                                        results.Deleted.forEach((deletedObject) => {
                                            const index = batch.findIndex((key) => key === deletedObject.Key);
                                            if (index < 0) {
                                                throw new Error('s3.deleteObjects deleted key which is not given');
                                            }
                                            if (callback && callback(index + firstIndexInCurrentBatch) === false) {
                                                aborted = true;
                                            }
                                            partialObjectErrors[index] = { bucket: bucket, key: deletedObject.Key! };
                                        });
                                    }
                                    if (results.Errors) {
                                        results.Errors.forEach((deletedObject) => {
                                            const error = new Error(deletedObject.Message);
                                            const index = batch.findIndex((key) => key === deletedObject.Key);
                                            if (index < 0) {
                                                throw new Error('s3.deleteObjects deleted key which is not given');
                                            }
                                            if (callback && callback(index + firstIndexInCurrentBatch, error) === false) {
                                                aborted = true;
                                            }
                                            partialObjectErrors[index] = { bucket: bucket, key: deletedObject.Key!, error: error };
                                        });
                                    }
                                }
                                if (aborted) {
                                    reject(new Error('aborted'));
                                } else {
                                    resolve(partialObjectErrors);
                                }
                                release();
                            });
                        });
                    });
                });
                Promise.all(promises).then((batches: Array<Array<PartialObjectError>>) => {
                    let results: Array<PartialObjectError> = [];
                    for (const batch of batches) {
                        results = results.concat(batch);
                    }
                    resolve(results);
                }, reject);
            }, reject);
        });
    }

    getFrozenInfo(s3RegionId: string, object: Object): Promise<FrozenInfo> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                s3.headObject({ Bucket: bucketId, Key: object.key }, (err, data) => {
                    if (err) {
                        reject(err);
                    } else if (data.StorageClass?.toLowerCase() === 'glacier') {
                        if (data.Restore) {
                            const restoreInfo = parseRestoreInfo(data.Restore);
                            if (restoreInfo.get('ongoing-request') === 'true') {
                                resolve({ status: 'Unfreezing' });
                            } else {
                                const frozenInfo: FrozenInfo = { status: 'Unfrozen' };
                                const expiryDate: string | undefined = restoreInfo.get('expiry-date');
                                if (expiryDate) {
                                    frozenInfo.expiryDate = new Date(expiryDate);
                                }
                                resolve(frozenInfo);
                            }
                        } else {
                            resolve({ status: 'Frozen' });
                        }
                    } else {
                        resolve({ status: 'Normal' });
                    }
                });
            }, reject);
        });
    }

    unfreeze(s3RegionId: string, object: Object, days: number): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                const params: AWS.S3.Types.RestoreObjectRequest = {
                    Bucket: bucketId, Key: object.key,
                    RestoreRequest: {
                        Days: days,
                        GlacierJobParameters: { Tier: 'Standard' },
                    },
                };
                s3.restoreObject(params, (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            }, reject);
        });
    }

    listObjects(s3RegionId: string, bucket: string, prefix: string, option?: ListObjectsOption): Promise<ListedObjects> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(bucket)]).then(([s3, bucketId]) => {
                const results: ListedObjects = { objects: [] };
                this._listObjects(s3RegionId, s3, bucket, bucketId, prefix, resolve, reject, results, option);
            }, reject);
        });
    }

    private _listObjects(s3RegionId: string, s3: AWS.S3, bucket: string, bucketId: string, prefix: string, resolve: any, reject: any, results: ListedObjects, option?: ListObjectsOption): void {
        const params: AWS.S3.Types.ListObjectsRequest = {
            Bucket: bucketId, Delimiter: option?.delimiter, Marker: option?.nextContinuationToken, MaxKeys: option?.maxKeys, Prefix: prefix,
        };
        const newOption: ListObjectsOption = {
            delimiter: option?.delimiter,
        };
        s3.listObjects(params, (err, data) => {
            if (err) {
                reject(err);
            } else {
                let isEmpty = true;
                if (data.Contents && data.Contents.length > 0) {
                    isEmpty = false;
                    results.objects = results.objects.concat(data.Contents.map((object: AWS.S3.Types.Object) => {
                        return {
                            bucket: bucket, key: object.Key!, size: object.Size!,
                            lastModified: object.LastModified!, storageClass: toStorageClass(object.StorageClass),
                        };
                    }));
                }
                if (data.CommonPrefixes && data.CommonPrefixes.length > 0) {
                    isEmpty = false;
                    if (!results.commonPrefixes) {
                        results.commonPrefixes = [];
                    }
                    results.commonPrefixes = results.commonPrefixes.concat(data.CommonPrefixes.map((commonPrefix: AWS.S3.Types.CommonPrefix) => {
                        return { bucket: bucket, key: commonPrefix.Prefix! };
                    }));
                }
                if (!isEmpty && data.NextMarker) {
                    newOption.nextContinuationToken = data.NextMarker;
                    if (option?.minKeys) {
                        let resultsSize = results.objects.length;
                        if (results.commonPrefixes) {
                            resultsSize += results.commonPrefixes.length;
                        }
                        if (resultsSize < option.minKeys) {
                            newOption.minKeys = option.minKeys;
                            newOption.maxKeys = option.minKeys - resultsSize;
                            this._listObjects(s3RegionId, s3, bucket, bucketId, prefix, resolve, reject, results, newOption);
                            return;
                        }
                    }
                }
                resolve(results);
            }
        });
    }

    createMultipartUpload(s3RegionId: string, object: Object, originalFileName: string, header?: SetObjectHeader): Promise<InitPartsOutput> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                const params: AWS.S3.Types.CreateMultipartUploadRequest = {
                    Bucket: bucketId, Key: object.key, Metadata: header?.metadata, ContentDisposition: makeContentDisposition(originalFileName),
                };
                if (header?.contentType) {
                    params.ContentType = header!.contentType;
                }
                s3.createMultipartUpload(params, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve({ uploadId: data.UploadId! });
                    }
                });
            }, reject);
        });
    }

    uploadPart(s3RegionId: string, object: Object, uploadId: string, partNumber: number, data: Buffer, option?: PutObjectOption): Promise<UploadPartOutput> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                let dataSource: Readable | Buffer;
                if (this.adapterOption.ucUrl?.startsWith("https://") ?? true) {
                    const reader = new ReadableStreamBuffer({ initialSize: data.length, chunkSize: 1 << 20 });
                    reader.put(data);
                    reader.stop();
                    if (option?.throttle) {
                        dataSource = reader.pipe(option.throttle);
                    } else {
                        dataSource = reader;
                    }
                } else {
                    dataSource = data;
                }
                const params: AWS.S3.Types.UploadPartRequest = {
                    Bucket: bucketId, Key: object.key, Body: dataSource, ContentLength: data.length,
                    ContentMD5: md5.hex(data), PartNumber: partNumber, UploadId: uploadId,
                };
                const uploader = s3.uploadPart(params);
                if (option?.progressCallback) {
                    uploader.on('httpUploadProgress', (progress) => {
                        option.progressCallback!(progress.loaded, progress.total);
                    });
                }
                uploader.send((err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve({ etag: data.ETag! });
                    }
                });
            }, reject);
        });
    }

    completeMultipartUpload(s3RegionId: string, object: Object, uploadId: string, parts: Array<Part>, _originalFileName: string, _header?: SetObjectHeader): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                const params: AWS.S3.Types.CompleteMultipartUploadRequest = {
                    Bucket: bucketId, Key: object.key, UploadId: uploadId,
                    MultipartUpload: {
                        Parts: parts.map((part) => {
                            return { PartNumber: part.partNumber, ETag: part.etag };
                        }),
                    },
                };
                s3.completeMultipartUpload(params, (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            }, reject);
        });
    }
}

function toStorageClass(storageClass?: AWS.S3.Types.ObjectStorageClass): StorageClass {
    const s = (storageClass ?? 'standard').toLowerCase();
    if (s === 'standard') {
        return 'Standard';
    } else if (s.includes('_ia') || s === 'line') {
        return 'InfrequentAccess';
    } else if (s === 'glacier') {
        return 'Glacier';
    }
    throw new Error(`Unknown file type: ${storageClass}`);
}

function parseRestoreInfo(s: string): Map<string, string> {
    const matches = s.match(/([\w\-]+)=\"([^\"]+)\"/g);
    const result = new Map<string, string>();
    if (matches) {
        matches.forEach((s) => {
            const pair = s.match(/([\w\-]+)=\"([^\"]+)\"/);
            if (pair && pair.length >= 3) {
                result.set(pair[1], pair[2]);
            }
        });
    }
    return result;
}

function makeContentDisposition(originalFileName: string): string {
    return `attachment; filename*=utf-8''${encodeURIComponent(originalFileName)}`;
}
