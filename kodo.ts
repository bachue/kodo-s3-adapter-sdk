import AsyncLock from 'async-lock';
import os from 'os';
import pkg from './package.json';
import FormData from 'form-data';
import CRC32 from 'buffer-crc32';
import md5 from 'js-md5';
import { Semaphore } from 'semaphore-promise';
import { RegionRequestOptions } from './region';
import { RegionService } from './region_service';
import { URL, URLSearchParams } from 'url';
import { Readable } from 'stream';
import { HttpClientResponse } from 'urllib';
import { encode as base64Encode } from 'js-base64';
import { base64ToUrlSafe, newUploadPolicy, makeUploadToken, signPrivateURL } from './kodo-auth';
import {
    Adapter, AdapterOption, Bucket, Domain, Object, SetObjectHeader, ObjectGetResult, ObjectHeader, ObjectInfo,
    TransferObject, PartialObjectError, BatchCallback, FrozenInfo, ListObjectsOption, ListedObjects, PutObjectOption,
    InitPartsOutput, UploadPartOutput, StorageClass, Part, GetObjectStreamOption,
} from './adapter';
import { KodoHttpClient, ServiceName, RequestOptions } from './kodo-http-client';
import { URLRequestOptions, RequestStats } from './http-client';
import { UplogEntry, SdkApiUplogEntry, LogType } from './uplog';
import { convertStorageClassToFileType } from './utils'

export const USER_AGENT = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/kodo`;

export class Kodo implements Adapter {
    protected readonly client: KodoHttpClient;
    protected readonly regionService: RegionService;
    private readonly bucketDomainsCache: { [bucketName: string]: Domain[]; } = {};
    private readonly bucketDomainsCacheLock = new AsyncLock();

    constructor(protected adapterOption: AdapterOption) {
        let userAgent: string = USER_AGENT;
        if (adapterOption.appendedUserAgent) {
            userAgent += `/${adapterOption.appendedUserAgent}`;
        }
        this.client = new KodoHttpClient({
            accessKey: adapterOption.accessKey,
            secretKey: adapterOption.secretKey,
            ucUrl: adapterOption.ucUrl,
            regions: adapterOption.regions,
            appendedUserAgent: adapterOption.appendedUserAgent,
            appName: adapterOption.appName,
            appVersion: adapterOption.appVersion,
            uplogBufferSize: adapterOption.uplogBufferSize,
            userAgent,
            timeout: [30000, 300000],
            retry: 10,
            retryDelay: 500,
            requestCallback: adapterOption.requestCallback,
            responseCallback: adapterOption.responseCallback,
        });
        this.regionService = new RegionService(adapterOption);
    }

    enter<T>(sdkApiName: string, f: (scope: Adapter, options: RegionRequestOptions) => Promise<T>): Promise<T> {
        const scope = new KodoScope(sdkApiName, this.adapterOption);
        return new Promise((resolve, reject) => {
            f(scope, scope.getRegionRequestOptions()).then((data) => {
                scope.done(true).finally(() => { resolve(data); });
            }).catch((err) => {
                scope.done(false).finally(() => { reject(err); });
            });
        });
    }

    createBucket(s3RegionId: string, bucket: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.regionService.fromS3IdToKodoRegionId(s3RegionId, this.getRegionRequestOptions()).then((kodoRegionId) => {
                this.call({
                    method: 'POST',
                    serviceName: ServiceName.Uc,
                    path: `mkbucketv3/${bucket}/region/${kodoRegionId}`,
                }).then(() => {
                    resolve();
                }).catch(reject);
            }).catch(reject);
        });
    }

    deleteBucket(_region: string, bucket: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.call({
                method: 'POST',
                serviceName: ServiceName.Uc,
                bucketName: bucket,
                path: `drop/${bucket}`,
            }).then(() => {
                resolve();
            }).catch(reject);
        });
    }

    getBucketLocation(bucket: string): Promise<string> {
        return new Promise((resolve, reject) => {
            this.call({
                method: 'GET',
                serviceName: ServiceName.Uc,
                bucketName: bucket,
                path: `bucket/${bucket}`,
                dataType: 'json',
            }).then((response) => {
                const kodoRegionId = response.data.region;
                this.regionService.fromKodoRegionIdToS3Id(kodoRegionId, this.getRegionRequestOptions())
                    .then(resolve).catch(reject);
            }).catch(reject);
        });
    }

    listBuckets(): Promise<Bucket[]> {
        return new Promise((resolve, reject) => {
            const bucketsQuery = new URLSearchParams();
            bucketsQuery.set('shared', 'rd');

            this.call({
                method: 'GET',
                serviceName: ServiceName.Uc,
                path: 'v2/buckets',
                dataType: 'json',
                query: bucketsQuery,
            }).then((response) => {
                if (!response.data) {
                    resolve([]);
                    return;
                }
                const regionsPromises: Promise<string | undefined>[] = response.data.map((info: any) => {
                    return new Promise((resolve) => {
                        this.regionService.fromKodoRegionIdToS3Id(info.region, this.getRegionRequestOptions())
                            .then(resolve).catch(() => { resolve(undefined); });
                    });
                });
                Promise.all(regionsPromises).then((regionsInfo: (string | undefined)[]) => {
                    const bucketInfos: Bucket[] = response.data.map((info: any, index: number) => {
                        let grantedPermission: string | undefined;
                        switch (info.perm) {
                            case 1:
                                grantedPermission = 'readonly';
                                break;
                            case 2:
                                grantedPermission = 'readwrite';
                                break;
                        }
                        return {
                            id: info.id, name: info.tbl,
                            createDate: new Date(info.ctime * 1000),
                            regionId: regionsInfo[index], grantedPermission,
                        };
                    });
                    resolve(bucketInfos);
                }).catch(reject);
            }).catch(reject);
        });
    }

    listDomains(s3RegionId: string, bucket: string): Promise<Domain[]> {
        return new Promise((resolve, reject) => {
            const domainsQuery = new URLSearchParams();
            domainsQuery.set('sourceTypes', 'qiniuBucket');
            domainsQuery.set('sourceQiniuBucket', bucket);
            domainsQuery.set('operatingState', 'success');
            domainsQuery.set('limit', '50');

            const getBucketInfoQuery = new URLSearchParams();
            getBucketInfoQuery.set('bucket', bucket);

            const bucketDefaultDomainQuery = new URLSearchParams();
            bucketDefaultDomainQuery.set('bucket', bucket);

            const promises = [
                this.call({
                    method: 'GET',
                    serviceName: ServiceName.Qcdn,
                    path: 'domain',
                    query: domainsQuery,
                    dataType: 'json',
                    s3RegionId,
                }),
                this.call({
                    method: 'POST',
                    serviceName: ServiceName.Uc,
                    path: 'v2/bucketInfo',
                    query: getBucketInfoQuery,
                    dataType: 'json',
                    s3RegionId,
                }),
                this.call({
                    method: 'GET',
                    serviceName: ServiceName.Portal,
                    path: 'api/kodov2/domain/default/get',
                    query: bucketDefaultDomainQuery,
                    dataType: 'json',
                    s3RegionId,
                }),
            ];

            Promise.all(promises).then(([domainResponse, bucketResponse, defaultDomainQuery]) => {
                if (bucketResponse.data.perm && bucketResponse.data.perm > 0) {
                    const result = defaultDomainQuery.data;
                    const domains: Domain[] = [];
                    if (result.domain && result.protocol) {
                        domains.push({
                            name: result.domain, protocol: result.protocol,
                            type: 'normal', private: bucketResponse.data.private != 0,
                        });
                    }
                    resolve(domains);
                } else {
                    const domains: Domain[] = domainResponse.data.domains.filter((domain: any) => {
                        switch (domain.type) {
                            case 'normal':
                            case 'pan':
                            case 'test':
                                return true;
                            default:
                                return false;
                        }
                    }).map((domain: any) => {
                        return {
                            name: domain.name, protocol: domain.protocol, type: domain.type,
                            private: bucketResponse.data.private != 0,
                        };
                    });
                    resolve(domains);
                }
            }).catch(reject);
        });
    }

    private _listDomains(s3RegionId: string, bucket: string): Promise<Domain[]> {
        return new Promise((resolve, reject) => {
            if (this.bucketDomainsCache[bucket]) {
                resolve(this.bucketDomainsCache[bucket]);
                return;
            }

            this.bucketDomainsCacheLock.acquire(bucket, (): Promise<Domain[]> => {
                if (this.bucketDomainsCache[bucket]) {
                    return Promise.resolve(this.bucketDomainsCache[bucket]);
                }
                return this.listDomains(s3RegionId, bucket);
            }).then((domains: Domain[]) => {
                this.bucketDomainsCache[bucket] = domains;
                resolve(domains);
            }).catch(reject);
        });
    }

    listBucketIdNames(): Promise<BucketIdName[]> {
        return new Promise((resolve, reject) => {
            this.call({
                method: 'GET',
                serviceName: ServiceName.Uc,
                path: 'v2/buckets',
                dataType: 'json',
            }).then((response) => {
                const bucketInfos = response.data.map((info: any) => {
                    return { id: info.id, name: info.tbl };
                });
                resolve(bucketInfos);
            }).catch(reject);
        });
    }

    isExists(s3RegionId: string, object: Object): Promise<boolean> {
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

    deleteObject(s3RegionId: string, object: Object): Promise<void> {
        return new Promise((resolve, reject) => {
            this.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `delete/${encodeObject(object)}`,
                dataType: 'json',
                s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }).catch(reject);
        });
    }

    putObject(s3RegionId: string, object: Object, data: Buffer, originalFileName: string,
        header?: SetObjectHeader, option?: PutObjectOption): Promise<void> {
        return new Promise((resolve, reject) => {
            const token = makeUploadToken(
                this.adapterOption.accessKey,
                this.adapterOption.secretKey,
                newUploadPolicy({
                    bucket: object.bucket,
                    key: object.key,
                    storageClassName: object?.storageClassName,
                }),
            );
            const form = new FormData();
            form.append('key', object.key);
            form.append('token', token);
            if (header?.metadata) {
                for (const [metaKey, metaValue] of Object.entries(header!.metadata)) {
                    form.append(`x-qn-meta-${metaKey}`, metaValue);
                }
            }
            form.append('crc32', CRC32.unsigned(data));

            const fileOption: FormData.AppendOptions = {
                filename: originalFileName,
            };
            if (header?.contentType) {
                fileOption.contentType = header!.contentType;
            }
            form.append('file', data, fileOption);
            this.call({
                method: 'POST',
                serviceName: ServiceName.Up,
                dataType: 'json',
                s3RegionId,
                contentType: form.getHeaders()['content-type'],
                form,
                uploadProgress: option?.progressCallback,
                uploadThrottle: option?.throttle,
            }).then(() => { resolve(); }).catch(reject);
        });
    }

    getObject(s3RegionId: string, object: Object, domain?: Domain): Promise<ObjectGetResult> {
        return new Promise((resolve, reject) => {
            this.getObjectURL(s3RegionId, object, domain).then((url) => {
                this.callUrl([url.toString()], {
                    fullUrl: true,
                    appendAuthorization: false,
                    method: 'GET',
                }).then((response: HttpClientResponse<Buffer>) => {
                    resolve({ data: response.data, header: getObjectHeader(response) });
                }).catch(reject);
            }).catch(reject);
        });
    }

    getObjectStream(s3RegionId: string, object: Object, domain?: Domain, option?: GetObjectStreamOption): Promise<Readable> {
        const headers: { [headerName: string]: string; } = {};
        if (option?.rangeStart || option?.rangeEnd) {
            headers.Range = `bytes=${option?.rangeStart ?? ''}-${option?.rangeEnd ?? ''}`;
        }

        return new Promise((resolve, reject) => {
            this.getObjectURL(s3RegionId, object, domain).then((url) => {
                this.callUrl([url.toString()], {
                    fullUrl: true,
                    appendAuthorization: false,
                    method: 'GET',
                    headers,
                    streaming: true,
                }).then((response: HttpClientResponse<any>) => {
                    resolve(response.res);
                }).catch(reject);
            }).catch(reject);
        });
    }

    getObjectURL(s3RegionId: string, object: Object, domain?: Domain, deadline?: Date): Promise<URL> {
        return new Promise((resolve, reject) => {
            const domainPromise: Promise<Domain> = new Promise((resolve, reject) => {
                if (domain) {
                    resolve(domain);
                    return;
                }
                this._listDomains(s3RegionId, object.bucket).then((domains) => {
                    if (domains.length === 0) {
                        reject(new Error('no domain found'));
                        return;
                    }
                    const domainTypeScope = (domain: Domain): number => {
                        switch (domain.type) {
                            case 'normal': return 1;
                            case 'pan': return 2;
                            case 'test': return 3;
                        }
                    };
                    domains = domains.sort((domain1, domain2) => domainTypeScope(domain1) - domainTypeScope(domain2));
                    resolve(domains[0]);
                }).catch(reject);
            });

            domainPromise.then((domain: Domain) => {
                let url = new URL(`${domain.protocol}://${domain.name}`);
                url.pathname = object.key;
                if (domain.private) {
                    url = signPrivateURL(this.adapterOption.accessKey, this.adapterOption.secretKey, url, deadline);
                }
                resolve(url);
            }).catch(reject);
        });
    }

    getObjectInfo(s3RegionId: string, object: Object): Promise<ObjectInfo> {
        return new Promise((resolve, reject) => {
            this.call({
                method: 'GET',
                serviceName: ServiceName.Rs,
                path: `stat/${encodeObject(object)}`,
                dataType: 'json',
                s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then((response) => {
                resolve({
                    bucket: object.bucket, key: response.data.key, size: response.data.fsize,
                    lastModified: new Date(response.data.putTime / 10000), storageClass: toStorageClass(response.data.type),
                });
            }).catch(reject);
        });
    }

    getObjectHeader(s3RegionId: string, object: Object, domain?: Domain): Promise<ObjectHeader> {
        return new Promise((resolve, reject) => {
            this.getObjectURL(s3RegionId, object, domain).then((url) => {
                this.callUrl([url.toString()], {
                    fullUrl: true,
                    appendAuthorization: false,
                    method: 'HEAD',
                }).then((response: HttpClientResponse<Buffer>) => {
                    resolve(getObjectHeader(response));
                }).catch(reject);
            }).catch(reject);
        });
    }

    moveObject(s3RegionId: string, transferObject: TransferObject): Promise<void> {
        return new Promise((resolve, reject) => {
            this.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `move/${encodeObject(transferObject.from)}/${encodeObject(transferObject.to)}/force/true`,
                dataType: 'json',
                s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }).catch(reject);
        });
    }

    copyObject(s3RegionId: string, transferObject: TransferObject): Promise<void> {
        return new Promise((resolve, reject) => {
            this.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `copy/${encodeObject(transferObject.from)}/${encodeObject(transferObject.to)}/force/true`,
                dataType: 'json',
                s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }).catch(reject);
        });
    }

    moveObjects(s3RegionId: string, transferObjects: TransferObject[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.batchOps(transferObjects.map((to) => new MoveObjectOp(to)), 100, s3RegionId, callback);
    }

    copyObjects(s3RegionId: string, transferObjects: TransferObject[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.batchOps(transferObjects.map((to) => new CopyObjectOp(to)), 100, s3RegionId, callback);
    }

    deleteObjects(s3RegionId: string, bucket: string, keys: string[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.batchOps(keys.map((key) => new DeleteObjectOp({ bucket, key })), 100, s3RegionId, callback);
    }

    setObjectsStorageClass(s3RegionId: string, bucket: string, keys: string[], storageClass: StorageClass, callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.batchOps(keys.map((key) => new SetObjectStorageClassOp({ bucket, key }, storageClass)), 100, s3RegionId, callback);
    }

    restoreObjects(s3RegionId: string, bucket: string, keys: string[], days: number, callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.batchOps(keys.map((key) => new RestoreObjectsOp({ bucket, key }, days)), 100, s3RegionId, callback);
    }

    private batchOps(ops: ObjectOp[], batchCount: number, s3RegionId: string, callback?: BatchCallback): Promise<PartialObjectError[]> {
        const semaphore = new Semaphore(20);
        const opsBatches: ObjectOp[][] = [];

        while (ops.length >= batchCount) {
            const batch: ObjectOp[] = ops.splice(0, batchCount);
            opsBatches.push(batch);
        }
        if (ops.length > 0) {
            opsBatches.push(ops);
        }

        let counter = 0;
        const promises: Promise<PartialObjectError[]>[] = opsBatches.map((batch) => {
            const firstIndexInCurrentBatch = counter;
            counter += batch.length;
            return new Promise((resolve, reject) => {
                const params = new URLSearchParams();
                for (const op of batch) {
                    params.append('op', op.getOp());
                }
                semaphore.acquire().then((release) => {
                    this.call({
                        method: 'POST',
                        serviceName: ServiceName.Rs,
                        path: 'batch',
                        dataType: 'json',
                        s3RegionId,
                        contentType: 'application/x-www-form-urlencoded',
                        data: params.toString(),
                    }).then((response) => {
                        let aborted = false;
                        const results: PartialObjectError[] = response.data.map((item: any, index: number) => {
                            const currentIndex = firstIndexInCurrentBatch + index;
                            const result: PartialObjectError = batch[index].getObject();
                            if (item?.data?.error) {
                                const error = new Error(item?.data?.error);
                                if (callback && callback(currentIndex, error) === false) {
                                    aborted = true;
                                }
                                result.error = error;
                            } else if (callback && callback(currentIndex) === false) {
                                aborted = true;
                            }
                            return result;
                        });
                        if (aborted) {
                            reject(new Error('aborted'));
                        } else {
                            resolve(results);
                        }
                    }).catch((error) => {
                        let aborted = false;
                        const results: PartialObjectError[] = batch.map((op, index) => {
                            const currentIndex = firstIndexInCurrentBatch + index;
                            if (callback && callback(currentIndex, error) === false) {
                                aborted = true;
                            }
                            return Object.assign({}, op.getObject());
                        });
                        if (aborted) {
                            reject(new Error('aborted'));
                        } else {
                            resolve(results);
                        }
                    }).finally(() => {
                        release();
                    });
                });
            });
        });

        return new Promise((resolve, reject) => {
            Promise.all(promises).then((batches: PartialObjectError[][]) => {
                let results: PartialObjectError[] = [];
                for (const batch of batches) {
                    results = results.concat(batch);
                }
                resolve(results);
            }).catch(reject);
        });
    }

    getFrozenInfo(s3RegionId: string, object: Object): Promise<FrozenInfo> {
        return new Promise((resolve, reject) => {
            this.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `stat/${encodeObject(object)}`,
                dataType: 'json',
                s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then((response) => {
                if (response.data.type === 2) {
                    if (response.data.restoreStatus) {
                        if (response.data.restoreStatus === 1) {
                            resolve({ status: 'Unfreezing' });
                        } else {
                            resolve({ status: 'Unfrozen' });
                        }
                    } else {
                        resolve({ status: 'Frozen' });
                    }
                } else {
                    resolve({ status: 'Normal' });
                }
            }).catch(reject);
        });
    }

    restoreObject(s3RegionId: string, object: Object, days: number): Promise<void> {
        return new Promise((resolve, reject) => {
            this.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `restoreAr/${encodeObject(object)}/freezeAfterDays/${days}`,
                dataType: 'json',
                s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }).catch(reject);
        });
    }

    setObjectStorageClass(s3RegionId: string, object: Object, storageClass: StorageClass): Promise<void> {
        return new Promise((resolve, reject) => {
            this.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `chtype/${encodeObject(object)}/type/${convertStorageClassToFileType(storageClass)}`,
                dataType: 'json',
                s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }).catch(reject);
        });
    }

    listObjects(s3RegionId: string, bucket: string, prefix: string, option?: ListObjectsOption): Promise<ListedObjects> {
        return new Promise((resolve, reject) => {
            const results: ListedObjects = { objects: [] };
            this._listObjects(s3RegionId, bucket, prefix, resolve, reject, results, option);
        });
    }

    private _listObjects(s3RegionId: string, bucket: string, prefix: string, resolve: any, reject: any, results: ListedObjects, option?: ListObjectsOption): void {
        const query = new URLSearchParams();
        query.set('bucket', bucket);
        query.set('prefix', prefix);
        if (option?.nextContinuationToken) {
            query.set('marker', option.nextContinuationToken);
        }
        if (option?.maxKeys) {
            query.set('limit', option.maxKeys.toString());
        }
        if (option?.delimiter) {
            query.set('delimiter', option.delimiter);
        }
        const newOption: ListObjectsOption = {
            delimiter: option?.delimiter,
        };

        this.call({
            method: 'POST',
            serviceName: ServiceName.Rsf,
            path: 'v2/list',
            s3RegionId,
            query,
            dataType: 'multijson',
            contentType: 'application/x-www-form-urlencoded',
        }).then((response) => {
            let marker: string | undefined;
            delete results.nextContinuationToken;

            response.data.forEach((data: { [key: string]: any; }) => {
                if (data.item) {
                    results.objects.push({
                        bucket, key: data.item.key, size: data.item.fsize,
                        lastModified: new Date(data.item.putTime / 10000), storageClass: toStorageClass(data.item.type),
                    });
                } else if (data.dir) {
                    if (results.commonPrefixes === undefined) {
                        results.commonPrefixes = [];
                    }
                    let foundDup = false;
                    for (const commonPrefix of results.commonPrefixes) {
                        if (commonPrefix.key === data.dir) {
                            foundDup = true;
                            break;
                        }
                    }
                    if (!foundDup) {
                        results.commonPrefixes.push({
                            bucket, key: data.dir,
                        });
                    }
                }
                marker = data.marker;
            });

            if (marker) {
                newOption.nextContinuationToken = marker;
                results.nextContinuationToken = marker;
                if (option?.minKeys) {
                    let resultsSize = results.objects.length;
                    if (results.commonPrefixes) {
                        resultsSize += results.commonPrefixes.length;
                    }
                    if (resultsSize < option.minKeys) {
                        newOption.minKeys = option.minKeys;
                        newOption.maxKeys = option.minKeys - resultsSize;
                        this._listObjects(s3RegionId, bucket, prefix, resolve, reject, results, newOption);
                        return;
                    }
                }
            }
            resolve(results);
        }).catch(reject);
    }

    createMultipartUpload(s3RegionId: string, object: Object, _originalFileName: string, _header?: SetObjectHeader): Promise<InitPartsOutput> {
        return new Promise((resolve, reject) => {
            const token = makeUploadToken(
                this.adapterOption.accessKey,
                this.adapterOption.secretKey,
                newUploadPolicy({
                    bucket: object.bucket,
                    key: object.key,
                    storageClassName: object?.storageClassName,
                })
            );
            const path = `/buckets/${object.bucket}/objects/${urlSafeBase64(object.key)}/uploads`;
            this.call({
                method: 'POST',
                serviceName: ServiceName.Up,
                path,
                dataType: 'json',
                s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
                headers: { 'authorization': `UpToken ${token}` },
            }).then((response) => {
                resolve({ uploadId: response.data.uploadId });
            }).catch(reject);
        });
    }

    uploadPart(s3RegionId: string, object: Object, uploadId: string, partNumber: number, data: Buffer, option?: PutObjectOption): Promise<UploadPartOutput> {
        return new Promise((resolve, reject) => {
            const token = makeUploadToken(
                this.adapterOption.accessKey,
                this.adapterOption.secretKey,
                newUploadPolicy({
                    bucket: object.bucket,
                    key: object.key,
                    storageClassName: object?.storageClassName,
                })
            );
            const path = `/buckets/${object.bucket}/objects/${urlSafeBase64(object.key)}/uploads/${uploadId}/${partNumber}`;
            this.call({
                method: 'PUT',
                serviceName: ServiceName.Up,
                path,
                data,
                dataType: 'json',
                s3RegionId,
                contentType: 'application/octet-stream',
                headers: {
                    'authorization': `UpToken ${token}`,
                    'content-md5': md5.hex(data),
                },
                uploadProgress: option?.progressCallback,
                uploadThrottle: option?.throttle,
            }).then((response) => {
                resolve({ etag: response.data.etag });
            }).catch(reject);
        });
    }

    completeMultipartUpload(s3RegionId: string, object: Object, uploadId: string, parts: Part[], originalFileName: string, header?: SetObjectHeader): Promise<void> {
        return new Promise((resolve, reject) => {
            const token = makeUploadToken(
                this.adapterOption.accessKey,
                this.adapterOption.secretKey,
                newUploadPolicy({
                    bucket: object.bucket,
                    key: object.key,
                    storageClassName: object.storageClassName,
                })
            );
            const path = `/buckets/${object.bucket}/objects/${urlSafeBase64(object.key)}/uploads/${uploadId}`;
            const metadata: { [metaKey: string]: string; } = {};
            if (header?.metadata) {
                for (const [metaKey, metaValue] of Object.entries(header!.metadata)) {
                    metadata[`x-qn-meta-${metaKey}`] = metaValue;
                }
            }
            const data: any = { fname: originalFileName, parts, metadata };
            if (header?.contentType) {
                data.mimeType = header!.contentType;
            }

            this.call({
                method: 'POST',
                serviceName: ServiceName.Up,
                path,
                data: JSON.stringify(data),
                dataType: 'json',
                s3RegionId,
                headers: { 'authorization': `UpToken ${token}` },
            }).then(() => { resolve(); }).catch(reject);
        });
    }

    clearCache() {
        Object.keys(this.bucketDomainsCache).forEach((key) => { delete this.bucketDomainsCache[key]; });
        this.client.clearCache();
        this.regionService.clearCache();
    }

    protected call<T = any>(options: RequestOptions): Promise<HttpClientResponse<T>> {
        return this.client.call(options);
    }

    protected callUrl<T = any>(urls: string[], options: URLRequestOptions): Promise<HttpClientResponse<T>> {
        return this.client.callUrls(urls, options);
    }

    protected getRegionRequestOptions(): RegionRequestOptions {
        return {
            timeout: [30000, 300000],
            retry: 10,
            retryDelay: 500,
        };
    }

    protected log(entry: UplogEntry): Promise<void> {
        return this.client.log(entry);
    }
}

class KodoScope extends Kodo {
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

    protected call<T = any>(options: RequestOptions): Promise<HttpClientResponse<T>> {
        options.stats = this.requestStats;
        return super.call(options);
    }

    getRegionRequestOptions(): RegionRequestOptions {
        const options = super.getRegionRequestOptions();
        options.stats = this.requestStats;
        return options;
    }
}

function toStorageClass(type?: number): StorageClass {
    switch (type ?? 0) {
        case 0:
            return 'Standard';
        case 1:
            return 'InfrequentAccess';
        case 2:
            return 'Glacier';
        default:
            throw new Error(`Unknown file type: ${type}`);
    }
}

function encodeObject(object: Object): string {
    return encodeBucketKey(object.bucket, object.key);
}

function encodeBucketKey(bucket: string, key?: string): string {
    let data: string = bucket;
    if (key) {
        data += `:${key}`;
    }
    return urlSafeBase64(data);
}

function urlSafeBase64(data: string): string {
    return base64ToUrlSafe(base64Encode(data));
}

function getObjectHeader(response: HttpClientResponse<Buffer>): ObjectHeader {
    const size: number = parseInt(response.headers['content-length']! as string);
    const contentType: string = response.headers['content-type']! as string;
    const lastModified: Date = new Date(response.headers['last-modified']! as string);
    const metadata: { [key: string]: string; } = {};
    for (const [metaKey, metaValue] of Object.entries(response.headers)) {
        if (metaKey?.startsWith('x-qn-meta-')) {
            metadata[metaKey.substring('x-qn-meta-'.length) as string] = (metaValue as string);
        }
    }
    return { size, contentType, lastModified, metadata };
}


export interface BucketIdName {
    id: string;
    name: string;
}

abstract class ObjectOp {
    abstract getObject(): Object;
    abstract getOp(): string;
}

class MoveObjectOp extends ObjectOp {
    constructor(private readonly object: TransferObject) {
        super();
    }

    getObject(): Object {
        return this.object.from;
    }

    getOp(): string {
        return `/move/${encodeObject(this.object.from)}/${encodeObject(this.object.to)}/force/true`;
    }
}

class CopyObjectOp extends ObjectOp {
    constructor(private readonly object: TransferObject) {
        super();
    }

    getObject(): Object {
        return this.object.from;
    }

    getOp(): string {
        return `/copy/${encodeObject(this.object.from)}/${encodeObject(this.object.to)}/force/true`;
    }
}

class DeleteObjectOp extends ObjectOp {
    constructor(private readonly object: Object) {
        super();
    }

    getObject(): Object {
        return this.object;
    }

    getOp(): string {
        return `/delete/${encodeObject(this.object)}`;
    }
}

class SetObjectStorageClassOp extends ObjectOp {
    constructor(private readonly object: Object, private readonly storageClass: StorageClass) {
        super();
    }

    getObject(): Object {
        return this.object;
    }

    getOp(): string {
        return `chtype/${encodeObject(this.object)}/type/${convertStorageClassToFileType(this.storageClass)}`;
    }
}

class RestoreObjectsOp extends ObjectOp {
    constructor(private readonly object: Object, private readonly days: number) {
        super();
    }

    getObject(): Object {
        return this.object;
    }

    getOp(): string {
        return `restoreAr/${encodeObject(this.object)}/freezeAfterDays/${this.days}`;
    }
}
