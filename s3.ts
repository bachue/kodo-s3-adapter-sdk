import AsyncLock from 'async-lock';
import AWS from 'aws-sdk';
import os from 'os';
import pkg from './package.json';
import md5 from 'js-md5';
import { URL } from 'url';
import { Readable } from 'stream';
import { Semaphore } from 'semaphore-promise';
import { Kodo } from './kodo';
import { ReadableStreamBuffer } from 'stream-buffers';
import {
    Adapter, AdapterOption, Bucket, Domain, StorageObject, SetObjectHeader, ObjectGetResult, ObjectHeader, ObjectInfo,
    TransferObject, PartialObjectError, BatchCallback, FrozenInfo, ListedObjects, ListObjectsOption, PutObjectOption,
    InitPartsOutput, UploadPartOutput, StorageClass, Part, GetObjectStreamOption, RequestInfo, ResponseInfo
} from './adapter';
import { LogType, RequestUplogEntry, SdkApiUplogEntry, getErrorTypeFromStatusCode, getErrorTypeFromS3Error } from './uplog';
import { RequestStats } from './http-client';
import { RegionRequestOptions } from './region';
import { generateReqId } from './req_id';
import { covertStorageClassToS3StorageClass } from './utils'

export const USER_AGENT = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/s3`;

interface RequestOptions {
    stats?: RequestStats,
}

export class S3 extends Kodo {
    private readonly bucketNameToIdCache: { [name: string]: string; } = {};
    private readonly bucketIdToNameCache: { [id: string]: string; } = {};
    private readonly clients: { [key: string]: AWS.S3; } = {};
    private readonly bucketNameToIdCacheLock = new AsyncLock();
    private readonly clientsLock = new AsyncLock();

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
                    this.regionService.getS3Endpoint(s3RegionId, this.getRegionRequestOptions()).then((s3IdEndpoint) => {
                        resolve(new AWS.S3({
                            apiVersion: "2006-03-01",
                            customUserAgent: userAgent,
                            computeChecksums: true,
                            region: s3IdEndpoint.s3Id,
                            endpoint: s3IdEndpoint.s3Endpoint,
                            accessKeyId: this.adapterOption.accessKey,
                            secretAccessKey: this.adapterOption.secretKey,
                            maxRetries: 10,
                            s3ForcePathStyle: true,
                            signatureVersion: "v4",
                            useDualstack: true,
                            httpOptions: {
                                connectTimeout: 30000,
                                timeout: 300000,
                            }
                        }));
                    }).catch(reject);
                });
            }).then((client: AWS.S3) => {
                this.clients[cacheKey] = client;
                resolve(client);
            }).catch(reject);
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
                    super.listBucketIdNames().then((buckets) => {
                        buckets.forEach((bucket) => {
                            this.bucketNameToIdCache[bucket.name] = bucket.id;
                            this.bucketIdToNameCache[bucket.id] = bucket.name;
                        });
                        resolve();
                    }).catch(reject);
                });
            }).then(() => {
                if (this.bucketNameToIdCache[bucketName]) {
                    resolve(this.bucketNameToIdCache[bucketName]);
                } else {
                    resolve(bucketName);
                }
            }).catch(reject);
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
                    super.listBucketIdNames().then((buckets) => {
                        buckets.forEach((bucket) => {
                            this.bucketNameToIdCache[bucket.name] = bucket.id;
                            this.bucketIdToNameCache[bucket.id] = bucket.name;
                        });
                        resolve();
                    }).catch(reject);
                });
            }).then(() => {
                if (this.bucketIdToNameCache[bucketId]) {
                    resolve(this.bucketIdToNameCache[bucketId]);
                } else {
                    reject(new Error(`Cannot find bucket name of bucket ${bucketId}`));
                }
            }).catch(reject);
        });
    }

    enter<T>(sdkApiName: string, f: (scope: Adapter, options: RegionRequestOptions) => Promise<T>): Promise<T> {
        const scope = new S3Scope(sdkApiName, this.adapterOption);
        return new Promise((resolve, reject) => {
            f(scope, scope.getRegionRequestOptions()).then((data) => {
                scope.done(true).finally(() => { resolve(data); });
            }).catch((err) => {
                scope.done(false).finally(() => { reject(err); });
            });
        });
    }

    private sendS3Request<D, E>(request: AWS.Request<D, E>): Promise<D> {
        let requestInfo: RequestInfo | undefined;
        const beginTime = new Date().getTime();
        const uplog: RequestUplogEntry = {
            log_type: LogType.Request,
            host: request.httpRequest.endpoint.host,
            port: request.httpRequest.endpoint.port,
            method: request.httpRequest.method,
            path: request.httpRequest.path,
            total_elapsed_time: 0,
        };

        const reqId = generateReqId({
            url: request.httpRequest.endpoint.href,
            method: request.httpRequest.method,
            headers: request.httpRequest.headers,
        });
        request.httpRequest.headers['X-Reqid'] = reqId;

        const options = this.getRequestsOption();

        return new Promise((resolve, reject) => {
            request.on('sign', (request) => {
                if (options.stats) {
                    options.stats.requestsCount += 1;
                }
                let url = request.httpRequest.endpoint.href;
                if (url.endsWith('/') && request.httpRequest.path.startsWith('/')) {
                    url += request.httpRequest.path.substring(1);
                } else {
                    url += request.httpRequest.path;
                }
                uplog.host = request.httpRequest.endpoint.host;
                uplog.port = request.httpRequest.endpoint.port;
                uplog.method = request.httpRequest.method;
                uplog.path = request.httpRequest.path;
                requestInfo = {
                    url,
                    method: request.httpRequest.method,
                    headers: request.httpRequest.headers,
                    data: request.httpRequest.body,
                };
                if (this.adapterOption.requestCallback) {
                    this.adapterOption.requestCallback(requestInfo);
                }
            });
            request.on('complete', (response) => {
                const responseInfo: ResponseInfo = {
                    request: requestInfo!,
                    statusCode: response.httpResponse.statusCode,
                    headers: response.httpResponse.headers,
                    data: response.data,
                    error: response.error,
                    interval: new Date().getTime() - beginTime,
                };
                if (this.adapterOption.responseCallback) {
                    this.adapterOption.responseCallback(responseInfo);
                }

                uplog.status_code = response.httpResponse.statusCode;
                uplog.total_elapsed_time = responseInfo.interval;
                if (response.requestId) {
                    uplog.req_id = response.requestId;
                }
                if (response.error) {
                    if (response.httpResponse.statusCode) {
                        uplog.error_type = getErrorTypeFromStatusCode(response.httpResponse.statusCode);
                    } else {
                        uplog.error_type = getErrorTypeFromS3Error(response.error);
                    }
                    uplog.error_description = (response.error as any).message || (response.error as any).code;
                    if (options.stats) {
                        options.stats.errorType = uplog.error_type;
                        options.stats.errorDescription = uplog.error_description;
                    }
                }
            });

            request.send((err, data) => {
                this.log(uplog).finally(() => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(data);
                    }
                });
            });
        });
    }

    createBucket(s3RegionId: string, bucket: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.getClient(s3RegionId).then((s3) => {
                this.sendS3Request(s3.createBucket({
                    Bucket: bucket,
                    CreateBucketConfiguration: {
                        LocationConstraint: s3RegionId,
                    },
                })).then(() => { resolve(); }).catch(reject);
            }, reject).catch(reject);
        });
    }

    deleteBucket(s3RegionId: string, bucket: string): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(bucket)]).then(([s3, bucketId]) => {
                this.sendS3Request(s3.deleteBucket({ Bucket: bucketId })).
                    then(() => { resolve(); }).catch(reject);
            }).catch(reject);
        });
    }

    getBucketLocation(bucket: string): Promise<string> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(), this.fromKodoBucketNameToS3BucketId(bucket)]).then(([s3, bucketId]) => {
                this._getBucketLocation(s3, bucketId).then(resolve).catch(reject);
            }).catch(reject);
        });
    }

    private _getBucketLocation(s3: AWS.S3, bucketId: string): Promise<string> {
        return new Promise((resolve, reject) => {
            this.sendS3Request(s3.getBucketLocation({ Bucket: bucketId })).then((data: any) => {
                const s3RegionId: string = data.LocationConstraint!;
                resolve(s3RegionId);
            }).catch(reject);
        });
    }

    listBuckets(): Promise<Bucket[]> {
        return new Promise((resolve, reject) => {
            this.getClient().then((s3) => {
                this.sendS3Request(s3.listBuckets()).then((data: any) => {
                    const bucketNamePromises: Promise<string>[] = data.Buckets!.map((info: any) => {
                        return this.fromS3BucketIdToKodoBucketName(info.Name);
                    });
                    const bucketLocationPromises: Promise<string | undefined>[] = data.Buckets!.map((info: any) => {
                        return new Promise((resolve) => {
                            this._getBucketLocation(s3, info.Name).then(resolve, () => { resolve(undefined); });
                        });
                    });
                    Promise.all([Promise.all(bucketNamePromises), Promise.all(bucketLocationPromises)])
                        .then(([bucketNames, bucketLocations]) => {
                            const bucketInfos: Bucket[] = data.Buckets!.map((info: any, index: number) => {
                                return {
                                    id: info.Name, name: bucketNames[index],
                                    createDate: info.CreationDate,
                                    regionId: bucketLocations[index],
                                };
                            });
                            resolve(bucketInfos);
                        }).catch(reject);
                }).catch(reject);
            }).catch(reject);
        });
    }

    listDomains(_s3RegionId: string, _bucket: string): Promise<Domain[]> {
        return Promise.resolve([]);
    }

    isExists(s3RegionId: string, object: StorageObject): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.getObjectInfo(s3RegionId, object).then(() => {
                resolve(true);
            }).catch((error: any) => {
                if (error.message === 'no such file or directory') {
                    resolve(false);
                } else {
                    reject(error);
                }
            });
        });
    }

    deleteObject(s3RegionId: string, object: StorageObject): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                this.sendS3Request(s3.deleteObject({ Bucket: bucketId, Key: object.key })).
                    then(() => { resolve(); }).catch(reject);
            }).catch(reject);
        });
    }

    putObject(s3RegionId: string, object: StorageObject, data: Buffer, originalFileName: string,
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
                    StorageClass: covertStorageClassToS3StorageClass(object.storageClassName),
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
                this.sendS3Request(uploader).then(() => { resolve(); }).catch(reject);
            }).catch(reject);
        });
    }

    getObject(s3RegionId: string, object: StorageObject, _domain?: Domain): Promise<ObjectGetResult> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                this.sendS3Request(s3.getObject({ Bucket: bucketId, Key: object.key })).then((data: any) => {
                    resolve({
                        data: Buffer.from(data.Body!),
                        header: { size: data.ContentLength!, contentType: data.ContentType!, lastModified: data.LastModified!, metadata: data.Metadata! },
                    });
                }).catch(reject);
            }).catch(reject);
        });
    }

    getObjectStream(s3RegionId: string, object: StorageObject, _domain?: Domain, option?: GetObjectStreamOption): Promise<Readable> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                let range: string | undefined;
                if (option?.rangeStart || option?.rangeEnd) {
                    range = `bytes=${option?.rangeStart ?? ''}-${option?.rangeEnd ?? ''}`;
                }

                resolve(s3.getObject({ Bucket: bucketId, Key: object.key, Range: range }).createReadStream());
            }).catch(reject);
        });
    }

    getObjectURL(s3RegionId: string, object: StorageObject, _domain?: Domain, deadline?: Date): Promise<URL> {
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
            }).catch(reject);
        });
    }

    getObjectInfo(s3RegionId: string, object: StorageObject): Promise<ObjectInfo> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                this.sendS3Request(s3.listObjects({ Bucket: bucketId, MaxKeys: 1, Prefix: object.key })).then((data: any) => {
                    if (data.Contents && data.Contents.length > 0) {
                        if (data.Contents[0].Key === object.key) {
                            resolve({
                                bucket: object.bucket, key: data.Contents[0].Key!, size: data.Contents[0].Size!,
                                lastModified: data.Contents[0].LastModified!, storageClass: toStorageClass(data.Contents[0].StorageClass),
                            });
                            return;
                        }
                    }
                    reject(new Error('no such file or directory'));
                }).catch(reject);
            }).catch(reject);
        });
    }

    getObjectHeader(s3RegionId: string, object: StorageObject, _domain?: Domain): Promise<ObjectHeader> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                this.sendS3Request(s3.headObject({ Bucket: bucketId, Key: object.key })).then((data: any) => {
                    resolve({ size: data.ContentLength!, contentType: data.ContentType!, lastModified: data.LastModified!, metadata: data.Metadata! });
                }).catch(reject);
            }).catch(reject);
        });
    }

    moveObject(s3RegionId: string, transferObject: TransferObject): Promise<void> {
        return new Promise((resolve, reject) => {
            this.copyObject(s3RegionId, transferObject).then(() => {
                this.deleteObject(s3RegionId, transferObject.from).then(resolve).catch((err) => {
                    err.stage = 'delete';
                    reject(err);
                });
            }).catch((err) => {
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
                this.sendS3Request(s3.copyObject(params)).
                    then(() => { resolve(); }).catch(reject);
            }).catch(reject);
        });
    }

    private getObjectStorageClass(s3RegionId: string, object: StorageObject): Promise<string | undefined> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                this.sendS3Request(s3.headObject({ Bucket: bucketId, Key: object.key })).then((data: any) => {
                    resolve(data.StorageClass);
                }).catch(reject);
            }).catch(reject);
        });
    }

    moveObjects(s3RegionId: string, transferObjects: TransferObject[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.s3BatchOps(transferObjects.map((to) => new MoveObjectOp(this, s3RegionId, to)), callback);
    }

    copyObjects(s3RegionId: string, transferObjects: TransferObject[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.s3BatchOps(transferObjects.map((to) => new CopyObjectOp(this, s3RegionId, to)), callback);
    }

    setObjectsStorageClass(s3RegionId: string, bucket: string, keys: string[], storageClass: StorageClass, callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.s3BatchOps(keys.map((key) => new SetObjectStorageClassOp(this, s3RegionId, { bucket, key }, storageClass)), callback);
    }

    restoreObjects(s3RegionId: string, bucket: string, keys: string[], days: number, callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.s3BatchOps(keys.map((key) => new RestoreObjectOp(this, s3RegionId, { bucket, key }, days)), callback);
    }

    private s3BatchOps(objectOps: ObjectOp[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        return new Promise((resolve, reject) => {
            const semaphore = new Semaphore(100);
            const promises: Promise<PartialObjectError>[] = objectOps.map((objectOp, index) => {
                return new Promise((resolve, reject) => {
                    semaphore.acquire().then((release) => {
                        objectOp.getOpPromise().then(() => {
                            if (callback && callback(index) === false) {
                                reject(new Error('aborted'));
                                return;
                            }
                            resolve(Object.assign({}, objectOp.getObject()));
                        }).catch((err) => {
                            if (callback && callback(index, err) === false) {
                                reject(new Error('aborted'));
                                return;
                            }
                            resolve(Object.assign({ error: err }, objectOp.getObject()));
                        }).finally(() => {
                            release();
                        });
                    });
                });
            });
            Promise.all(promises).then(resolve).catch(reject);
        });
    }

    deleteObjects(s3RegionId: string, bucket: string, keys: string[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(bucket)]).then(([s3, bucketId]) => {
                const semaphore = new Semaphore(100);
                const batchCount = 100;
                const batches: string[][] = [];
                while (keys.length >= batchCount) {
                    batches.push(keys.splice(0, batchCount));
                }
                if (keys.length > 0) {
                    batches.push(keys);
                }
                let counter = 0;
                const promises: Promise<PartialObjectError[]>[] = batches.map((batch) => {
                    const firstIndexInCurrentBatch = counter;
                    const partialObjectErrors: PartialObjectError[] = new Array(batch.length);
                    counter += batch.length;
                    return new Promise((resolve, reject) => {
                        semaphore.acquire().then((release) => {
                            this.sendS3Request(s3.deleteObjects({
                                Bucket: bucketId,
                                Delete: {
                                    Objects: batch.map((key) => { return { Key: key }; }),
                                },
                            })).then((results: any) => {
                                let aborted = false;
                                if (results.Deleted) {
                                    results.Deleted.forEach((deletedObject: any) => {
                                        const index = batch.findIndex((key) => key === deletedObject.Key);
                                        if (index < 0) {
                                            throw new Error('s3.deleteObjects deleted key which is not given');
                                        }
                                        if (callback && callback(index + firstIndexInCurrentBatch) === false) {
                                            aborted = true;
                                        }
                                        partialObjectErrors[index] = { bucket, key: deletedObject.Key! };
                                    });
                                }
                                if (results.Errors) {
                                    results.Errors.forEach((deletedObject: any) => {
                                        const error = new Error(deletedObject.Message);
                                        const index = batch.findIndex((key) => key === deletedObject.Key);
                                        if (index < 0) {
                                            throw new Error('s3.deleteObjects deleted key which is not given');
                                        }
                                        if (callback && callback(index + firstIndexInCurrentBatch, error) === false) {
                                            aborted = true;
                                        }
                                        partialObjectErrors[index] = { bucket, key: deletedObject.Key!, error };
                                    });
                                }
                                if (aborted) {
                                    reject(new Error('aborted'));
                                } else {
                                    resolve(partialObjectErrors);
                                }
                            }).catch((err: any) => {
                                let aborted = false;
                                if (err) {
                                    batch.forEach((key, index) => {
                                        if (callback && callback(index + firstIndexInCurrentBatch, err) === false) {
                                            aborted = true;
                                        }
                                        partialObjectErrors[index] = { bucket, key, error: err };
                                    });
                                }
                                if (aborted) {
                                    reject(new Error('aborted'));
                                } else {
                                    resolve(partialObjectErrors);
                                }
                            }).finally(release);
                        });
                    });
                });
                Promise.all(promises).then((batches: PartialObjectError[][]) => {
                    let results: PartialObjectError[] = [];
                    for (const batch of batches) {
                        results = results.concat(batch);
                    }
                    resolve(results);
                }).catch(reject);
            }).catch(reject);
        });
    }

    getFrozenInfo(s3RegionId: string, object: StorageObject): Promise<FrozenInfo> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                this.sendS3Request(s3.headObject({ Bucket: bucketId, Key: object.key })).then((data: any) => {
                    if (data.StorageClass?.toLowerCase() === 'glacier') {
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
                }).catch(reject);
            }).catch(reject);
        });
    }

    restoreObject(s3RegionId: string, object: StorageObject, days: number): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                const params: AWS.S3.Types.RestoreObjectRequest = {
                    Bucket: bucketId, Key: object.key,
                    RestoreRequest: {
                        Days: days,
                        GlacierJobParameters: { Tier: 'Standard' },
                    },
                };
                this.sendS3Request(s3.restoreObject(params)).
                    then(() => { resolve(); }).catch(reject);
            }).catch(reject);
        });
    }

    setObjectStorageClass(s3RegionId: string, object: StorageObject, storageClass: StorageClass): Promise<void> {
        return new Promise((resolve, reject) => {
            Promise.all([
                this.getClient(s3RegionId),
                this.fromKodoBucketNameToS3BucketId(object.bucket),
            ]).then(([s3, bucketId]) => {
                let storageClassParam: AWS.S3.StorageClass = 'STANDARD';
                switch (storageClass) {
                    case 'Standard':
                        storageClassParam = 'STANDARD';
                        break;
                    case 'InfrequentAccess':
                        storageClassParam = 'LINE';
                        break;
                    case 'Glacier':
                        storageClassParam = 'GLACIER';
                        break;
                }
                const params: AWS.S3.Types.CopyObjectRequest = {
                    Bucket: bucketId, Key: object.key,
                    CopySource: `/${bucketId}/${encodeURIComponent(object.key)}`,
                    MetadataDirective: 'COPY',
                    StorageClass: storageClassParam,
                };
                this.sendS3Request(s3.copyObject(params)).
                    then(() => { resolve(); }).catch(reject);
            }).catch(reject);
        });
    }

    listObjects(s3RegionId: string, bucket: string, prefix: string, option?: ListObjectsOption): Promise<ListedObjects> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(bucket)]).then(([s3, bucketId]) => {
                const results: ListedObjects = { objects: [] };
                this._listS3Objects(s3RegionId, s3, bucket, bucketId, prefix, results, option).
                    then(resolve).catch(reject);
            }).catch(reject);
        });
    }

    private _listS3Objects(s3RegionId: string, s3: AWS.S3, bucket: string, bucketId: string, prefix: string, results: ListedObjects, option?: ListObjectsOption): Promise<ListedObjects> {
        const params: AWS.S3.Types.ListObjectsRequest = {
            Bucket: bucketId, Delimiter: option?.delimiter, Marker: option?.nextContinuationToken, MaxKeys: option?.maxKeys, Prefix: prefix,
        };
        const newOption: ListObjectsOption = {
            delimiter: option?.delimiter,
        };
        return new Promise((resolve, reject) => {
            this.sendS3Request(s3.listObjects(params)).then((data: any) => {
                delete results.nextContinuationToken;
                if (data.Contents && data.Contents.length > 0) {
                    results.objects = results.objects.concat(data.Contents.map((object: AWS.S3.Types.Object) => {
                        return {
                            bucket, key: object.Key!, size: object.Size!,
                            lastModified: object.LastModified!, storageClass: toStorageClass(object.StorageClass),
                        };
                    }));
                }
                if (data.CommonPrefixes && data.CommonPrefixes.length > 0) {
                    if (!results.commonPrefixes) {
                        results.commonPrefixes = [];
                    }
                    const newCommonPrefixes = data.CommonPrefixes.map((commonPrefix: AWS.S3.Types.CommonPrefix) => {
                        return { bucket, key: commonPrefix.Prefix! };
                    });
                    for (const newCommonPrefix of newCommonPrefixes) {
                        let foundDup = false;
                        for (const commonPrefix of results.commonPrefixes) {
                            if (commonPrefix.key === newCommonPrefix.key) {
                                foundDup = true;
                                break;
                            }
                        }
                        if (!foundDup) {
                            results.commonPrefixes.push(newCommonPrefix);
                        }
                    }
                }

                results.nextContinuationToken = data.NextMarker;
                if (data.NextMarker) {
                    newOption.nextContinuationToken = data.NextMarker;
                    if (option?.minKeys) {
                        let resultsSize = results.objects.length;
                        if (results.commonPrefixes) {
                            resultsSize += results.commonPrefixes.length;
                        }
                        if (resultsSize < option.minKeys) {
                            newOption.minKeys = option.minKeys;
                            newOption.maxKeys = option.minKeys - resultsSize;
                            this._listS3Objects(s3RegionId, s3, bucket, bucketId, prefix, results, newOption).
                                then(resolve).catch(reject);
                            return;
                        }
                    }
                }
                resolve(results);
            }).catch(reject);
        });
    }

    createMultipartUpload(s3RegionId: string, object: StorageObject, originalFileName: string, header?: SetObjectHeader): Promise<InitPartsOutput> {
        return new Promise((resolve, reject) => {
            Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]).then(([s3, bucketId]) => {
                const params: AWS.S3.Types.CreateMultipartUploadRequest = {
                    Bucket: bucketId, Key: object.key, Metadata: header?.metadata, ContentDisposition: makeContentDisposition(originalFileName),
                };
                if (header?.contentType) {
                    params.ContentType = header!.contentType;
                }
                this.sendS3Request(s3.createMultipartUpload(params)).then((data: any) => {
                    resolve({ uploadId: data.UploadId! });
                }).catch(reject);
            }).catch(reject);
        });
    }

    uploadPart(s3RegionId: string, object: StorageObject, uploadId: string, partNumber: number, data: Buffer, option?: PutObjectOption): Promise<UploadPartOutput> {
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
                this.sendS3Request(uploader).then((data: any) => {
                    resolve({ etag: data.ETag! });
                }).catch(reject);
            }).catch(reject);
        });
    }

    completeMultipartUpload(s3RegionId: string, object: StorageObject, uploadId: string, parts: Part[], _originalFileName: string, _header?: SetObjectHeader): Promise<void> {
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
                this.sendS3Request(s3.completeMultipartUpload(params))
                    .then(() => { resolve(); }).catch(reject);
            }).catch(reject);
        });
    }

    clearCache() {
        Object.keys(this.bucketNameToIdCache).forEach((key) => { delete this.bucketNameToIdCache[key]; });
        Object.keys(this.bucketIdToNameCache).forEach((key) => { delete this.bucketIdToNameCache[key]; });
        Object.keys(this.clients).forEach((key) => { delete this.clients[key]; });
        super.clearCache();
        this.regionService.clearCache();
    }

    protected getRequestsOption(): RequestOptions {
        return {};
    }
}

class S3Scope extends S3 {
    private readonly requestStats: RequestStats;
    private readonly beginTime = new Date();

    constructor(sdkApiName: string, adapterOption: AdapterOption) {
        super(adapterOption);
        this.requestStats = {
            sdkApiName,
            requestsCount: 0,
        };
    }

    done(successful: boolean): Promise<void> {
        const uplog: SdkApiUplogEntry = {
            log_type: LogType.SdkApi,
            api_name: this.requestStats.sdkApiName,
            requests_count: this.requestStats.requestsCount,
            total_elapsed_time: new Date().getTime() - this.beginTime.getTime(),
        };
        if (!successful) {
            if (this.requestStats.errorType) {
                uplog.error_type = this.requestStats.errorType;
            }
            if (this.requestStats.errorDescription) {
                uplog.error_description = this.requestStats.errorDescription;
            }
        }
        this.requestStats.requestsCount = 0;
        this.requestStats.errorType = undefined;
        this.requestStats.errorDescription = undefined;
        return this.log(uplog);
    }

    protected getRequestsOption(): RequestOptions {
        const options = super.getRequestsOption();
        options.stats = this.requestStats;
        return options;
    }

    getRegionRequestOptions(): RegionRequestOptions {
        const options = super.getRegionRequestOptions();
        options.stats = this.requestStats;
        return options;
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

abstract class ObjectOp {
    abstract getOpPromise(): Promise<void>;
    abstract getObject(): StorageObject;
}

class MoveObjectOp extends ObjectOp {
    constructor(private readonly s3: S3, private readonly s3RegionId: string, private readonly transferObject: TransferObject) {
        super();
    }

    getOpPromise(): Promise<void> {
        return this.s3.moveObject(this.s3RegionId, this.transferObject);
    }

    getObject(): StorageObject {
        return this.transferObject.from;
    }
}

class CopyObjectOp extends ObjectOp {
    constructor(private readonly s3: S3, private readonly s3RegionId: string, private readonly transferObject: TransferObject) {
        super();
    }

    getOpPromise(): Promise<void> {
        return this.s3.copyObject(this.s3RegionId, this.transferObject);
    }

    getObject(): StorageObject {
        return this.transferObject.from;
    }
}

class SetObjectStorageClassOp extends ObjectOp {
    constructor(
        private readonly s3: S3,
        private readonly s3RegionId: string,
        private readonly object: StorageObject,
        private readonly storageClass: StorageClass) {
        super();
    }

    getOpPromise(): Promise<void> {
        return this.s3.setObjectStorageClass(this.s3RegionId, this.object, this.storageClass);
    }

    getObject(): StorageObject {
        return this.object;
    }
}

class RestoreObjectOp extends ObjectOp {
    constructor(
        private readonly s3: S3,
        private readonly s3RegionId: string,
        private readonly object: StorageObject,
        private readonly days: number) {
        super();
    }

    getOpPromise(): Promise<void> {
        return this.s3.restoreObject(this.s3RegionId, this.object, this.days);
    }

    getObject(): StorageObject {
        return this.object;
    }
}
