import AsyncLock from 'async-lock';
import os from 'os';
import pkg from './package.json';
import FormData from 'form-data';
import CRC32 from 'buffer-crc32';
import md5 from 'js-md5';
import { Semaphore } from 'semaphore-promise';
import { RegionService } from './region_service';
import { URL, URLSearchParams } from 'url';
import { HttpClient2, HttpClientResponse } from 'urllib';
import { encode as base64Encode } from 'js-base64';
import { base64ToUrlSafe, newUploadPolicy, makeUploadToken, signPrivateURL } from './kodo-auth';
import { Adapter, AdapterOption, Bucket, Domain, Object, SetObjectHeader, ObjectGetResult, ObjectHeader,
         TransferObject, PartialObjectError, BatchCallback, FrozenInfo, ListObjectsOption, ListedObjects,
         InitPartsOutput, UploadPartOutput, StorageClass, Part, ProgressCallback } from './adapter';
import { KodoHttpClient, ServiceName } from './kodo-http-client';

export const USER_AGENT: string = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/kodo`;

export class Kodo implements Adapter {
    private static readonly httpClient: HttpClient2 = new HttpClient2();
    private readonly client: KodoHttpClient;
    private readonly bucketDomainsCache: { [bucketName: string]: Array<Domain>; } = {};
    private readonly bucketDomainsCacheLock = new AsyncLock();
    private readonly regionService: RegionService;

    constructor(private adapterOption: AdapterOption) {
        let userAgent: string = USER_AGENT;
        if (adapterOption.appendedUserAgent) {
            userAgent += `/${adapterOption.appendedUserAgent}`;
        }
        this.client = new KodoHttpClient({
            accessKey: adapterOption.accessKey,
            secretKey: adapterOption.secretKey,
            ucUrl: adapterOption.ucUrl,
            userAgent: userAgent,
            timeout: [30000, 300000],
            retry: 10,
            retryDelay: 500,
        });
        this.regionService = new RegionService(adapterOption);
    }

    createBucket(s3RegionId: string, bucket: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.regionService.fromS3IdToKodoRegionId(s3RegionId).then((kodoRegionId) => {
                this.client.call({
                    method: 'POST',
                    serviceName: ServiceName.Uc,
                    path: `mkbucketv3/${bucket}/region/${kodoRegionId}`,
                }).then(() => {
                    resolve();
                }, reject);
            });
        });
    }

    deleteBucket(_region: string, bucket: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Uc,
                bucketName: bucket,
                path: `drop/${bucket}`,
            }).then(() => {
                resolve();
            }, reject);
        });
    }

    getBucketLocation(bucket: string): Promise<string> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'GET',
                serviceName: ServiceName.Uc,
                bucketName: bucket,
                path: `bucket/${bucket}`,
                dataType: 'json',
            }).then((response) => {
                const kodoRegionId = response.data.region;
                this.regionService.fromKodoRegionIdToS3Id(kodoRegionId)
                                  .then(resolve, reject);
            }, reject);
        });
    }

    listBuckets(): Promise<Array<Bucket>> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'GET',
                serviceName: ServiceName.Uc,
                path: 'v2/buckets',
                dataType: 'json',
            }).then((response) => {
                const regionsPromises: Array<Promise<string | undefined>> = response.data.map((info: any) => {
                    return new Promise((resolve) => {
                        this.regionService.fromKodoRegionIdToS3Id(info.region)
                                          .then(resolve, () => { resolve(undefined); });
                    });
                });
                Promise.all(regionsPromises).then((regionsInfo: Array<string | undefined>) => {
                    const bucketInfos: Array<Bucket> = response.data.map((info: any, index: number) => {
                        return {
                            id: info.id, name: info.tbl,
                            createDate: new Date(info.ctime * 1000),
                            regionId: regionsInfo[index],
                        };
                    });
                    resolve(bucketInfos);
                });
            }, reject);
        });
    }

    listDomains(s3RegionId: string, bucket: string): Promise<Array<Domain>> {
        return new Promise((resolve, reject) => {
            const domainsQuery = new URLSearchParams();
            domainsQuery.set('sourceTypes', 'qiniuBucket');
            domainsQuery.set('sourceQiniuBucket', bucket);
            domainsQuery.set('operatingState', 'success');
            domainsQuery.set('limit', '50');

            const getBucketInfoQuery = new URLSearchParams();
            getBucketInfoQuery.set('bucket', bucket);

            const promises = [
                this.client.call({
                    method: 'GET',
                    serviceName: ServiceName.Api,
                    path: 'domain',
                    query: domainsQuery,
                    dataType: 'json',
                    s3RegionId: s3RegionId,
                }),
                this.client.call({
                    method: 'POST',
                    serviceName: ServiceName.Uc,
                    path: 'v2/bucketInfo',
                    query: getBucketInfoQuery,
                    dataType: 'json',
                    s3RegionId: s3RegionId,
                }),
            ];

            Promise.all(promises).then(([domainResponse, bucketResponse]) => {
                const domains: Array<Domain> = domainResponse.data.domains.filter((domain: any) => {
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
            }, reject);
        });
    }

    _listDomains(s3RegionId: string, bucket: string): Promise<Array<Domain>> {
        return new Promise((resolve, reject) => {
            if (this.bucketDomainsCache[bucket]) {
                resolve(this.bucketDomainsCache[bucket]);
                return;
            }

            this.bucketDomainsCacheLock.acquire(bucket, (): Promise<Array<Domain>> => {
                if (this.bucketDomainsCache[bucket]) {
                    return new Promise((resolve) => { resolve(this.bucketDomainsCache[bucket]); });
                }
                return this.listDomains(s3RegionId, bucket);
            }).then((domains: Array<Domain>) => {
                this.bucketDomainsCache[bucket] = domains;
                resolve(domains);
            }, reject);
        });
    }

    listBucketIdNames(): Promise<Array<BucketIdName>> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'GET',
                serviceName: ServiceName.Uc,
                path: 'v2/buckets',
                dataType: 'json',
            }).then((response) => {
                const bucketInfos = response.data.map((info: any) => {
                    return { id: info.id, name: info.tbl };
                });
                resolve(bucketInfos);
            }, reject);
        });
    }

    isExists(s3RegionId: string, object: Object): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'GET',
                serviceName: ServiceName.Rs,
                path: `stat/${encodeObject(object)}`,
                dataType: 'json',
                s3RegionId: s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then((_response) => {
                resolve(true);
            }, (error) => {
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
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `delete/${encodeObject(object)}`,
                dataType: 'json',
                s3RegionId: s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }, reject);
        });
    }

    putObject(s3RegionId: string, object: Object, data: Buffer, originalFileName: string, header?: SetObjectHeader, progressCallback?: ProgressCallback): Promise<void> {
        return new Promise((resolve, reject) => {
            const token = makeUploadToken(this.adapterOption.accessKey, this.adapterOption.secretKey, newUploadPolicy(object.bucket, object.key));
            const form =  new FormData();
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
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Up,
                dataType: 'json',
                s3RegionId: s3RegionId,
                contentType: form.getHeaders()['content-type'],
                form: form,
                uploadProgress: progressCallback,
            }).then(() => { resolve(); }, reject);
        });
    }

    getObject(s3RegionId: string, object: Object, domain?: Domain): Promise<ObjectGetResult> {
        return new Promise((resolve, reject) => {
            this.getObjectURL(s3RegionId, object, domain).then((url) => {
                Kodo.httpClient.request(url.toString(), {
                    method: 'GET',
                    timeout: [30000, 300000],
                    retry: 10,
                    retryDelay: 500,
                    followRedirect: true,
                }).then((response: HttpClientResponse<Buffer>) => {
                    if (response.status === 200) {
                        resolve({ data: response.data, header: getObjectHeader(response)});
                    } else {
                        reject(new Error(response.res.statusMessage));
                    }
                }, reject);
            }, reject);
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
                }, reject);
            });

            domainPromise.then((domain: Domain) => {
                let url = new URL(`${domain.protocol}://${domain.name}/${object.key}`);
                if (domain.private) {
                    url = signPrivateURL(this.adapterOption.accessKey, this.adapterOption.secretKey, url, deadline);
                }
                resolve(url);
            }, reject);
        });
    }

    getObjectHeader(s3RegionId: string, object: Object, domain?: Domain): Promise<ObjectHeader> {
        return new Promise((resolve, reject) => {
            this.getObjectURL(s3RegionId, object, domain).then((url) => {
                Kodo.httpClient.request(url.toString(), {
                    method: 'HEAD',
                    timeout: [30000, 300000],
                    retry: 10,
                    retryDelay: 500,
                    followRedirect: true,
                }).then((response: HttpClientResponse<Buffer>) => {
                    if (response.status === 200) {
                        resolve(getObjectHeader(response));
                    } else {
                        reject(new Error(response.res.statusMessage));
                    }
                }, reject);
            }, reject);
        });
    }

    moveObject(s3RegionId: string, transferObject: TransferObject): Promise<void> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `move/${encodeObject(transferObject.from)}/${encodeObject(transferObject.to)}`,
                dataType: 'json',
                s3RegionId: s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }, reject);
        });
    }

    copyObject(s3RegionId: string, transferObject: TransferObject): Promise<void> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `copy/${encodeObject(transferObject.from)}/${encodeObject(transferObject.to)}`,
                dataType: 'json',
                s3RegionId: s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }, reject);
        });
    }

    moveObjects(s3RegionId: string, transferObjects: Array<TransferObject>, callback?: BatchCallback): Promise<Array<PartialObjectError>> {
        return this.moveOrCopyObjects('move', 100, s3RegionId, transferObjects, callback);
    }

    copyObjects(s3RegionId: string, transferObjects: Array<TransferObject>, callback?: BatchCallback): Promise<Array<PartialObjectError>> {
        return this.moveOrCopyObjects('copy', 100, s3RegionId, transferObjects, callback);
    }

    private moveOrCopyObjects(op: string, batchCount: number, s3RegionId: string, transferObjects: Array<TransferObject>, callback?: BatchCallback): Promise<Array<PartialObjectError>> {
        const semaphore = new Semaphore(5);
        const transferObjectsBatches: Array<Array<TransferObject>> = [];

        while (transferObjects.length >= batchCount) {
            const batch: Array<TransferObject> = transferObjects.splice(0, batchCount);
            transferObjectsBatches.push(batch);
        }
        if (transferObjects.length > 0) {
            transferObjectsBatches.push(transferObjects);
        }

        let counter = 0;
        const promises: Array<Promise<Array<PartialObjectError>>> = transferObjectsBatches.map((batch) => {
            const firstIndexInCurrentBatch = counter;
            counter += batch.length;
            return new Promise((resolve, reject) => {
                const params = new URLSearchParams();
                for (const transferObject of batch) {
                    params.append('op', `/${op}/${encodeObject(transferObject.from)}/${encodeObject(transferObject.to)}`);
                }
                semaphore.acquire().then((release) => {
                    this.client.call({
                        method: 'POST',
                        serviceName: ServiceName.Rs,
                        path: 'batch',
                        dataType: 'json',
                        s3RegionId: s3RegionId,
                        contentType: 'application/x-www-form-urlencoded',
                        data: params.toString(),
                    }).then((response) => {
                        let aborted = false;
                        const results: Array<PartialObjectError> = response.data.map((item: any, index: number) => {
                            const currentIndex = firstIndexInCurrentBatch + index;
                            const result: PartialObjectError = { bucket: batch[index].from.bucket, key: batch[index].from.key };
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
                    }, (error) => {
                        let aborted = false;
                        const results: Array<PartialObjectError> = batch.map((transferObject, index) => {
                            const currentIndex = firstIndexInCurrentBatch + index;
                            if (callback && callback(currentIndex, error) === false) {
                                aborted = true;
                            }
                            return { bucket: transferObject.from.bucket, key: transferObject.from.key, error: error };
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
            Promise.all(promises).then((batches: Array<Array<PartialObjectError>>) => {
                let results: Array<PartialObjectError> = [];
                for (const batch of batches) {
                    results = results.concat(batch);
                }
                resolve(results);
            }, reject);
        });
    }

    deleteObjects(s3RegionId: string, bucket: string, keys: Array<string>, callback?: BatchCallback): Promise<Array<PartialObjectError>> {
        const semaphore = new Semaphore(5);
        const keysBatches: Array<Array<string>> = [];
        const batchCount = 1000;

        while (keys.length >= batchCount) {
            const batch: Array<string> = keys.splice(0, batchCount);
            keysBatches.push(batch);
        }
        if (keys.length > 0) {
            keysBatches.push(keys);
        }

        let counter = 0;
        const promises: Array<Promise<Array<PartialObjectError>>> = keysBatches.map((batch) => {
            const firstIndexInCurrentBatch = counter;
            counter += batch.length;
            return new Promise((resolve, reject) => {
                const params = new URLSearchParams();
                for (const key of batch) {
                    params.append('op', `/delete/${encodeObject({ bucket: bucket, key: key })}`);
                }
                semaphore.acquire().then((release) => {
                    this.client.call({
                        method: 'POST',
                        serviceName: ServiceName.Rs,
                        path: 'batch',
                        dataType: 'json',
                        s3RegionId: s3RegionId,
                        contentType: 'application/x-www-form-urlencoded',
                        data: params.toString(),
                    }).then((response) => {
                        let aborted = false;
                        const results: Array<PartialObjectError> = response.data.map((item: any, index: number) => {
                            const currentIndex = firstIndexInCurrentBatch + index;
                            const result: PartialObjectError = { bucket: bucket, key: batch[index] };
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
                    }, (error) => {
                        let aborted = false;
                        const results: Array<PartialObjectError> = batch.map((key, index) => {
                            const currentIndex = firstIndexInCurrentBatch + index;
                            if (callback && callback(currentIndex, error) === false) {
                                aborted = true;
                            }
                            return { bucket: bucket, key: key, error: error };
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
            Promise.all(promises).then((batches: Array<Array<PartialObjectError>>) => {
                let results: Array<PartialObjectError> = [];
                for (const batch of batches) {
                    results = results.concat(batch);
                }
                resolve(results);
            }, reject);
        });
    }

    freeze(s3RegionId: string, object: Object): Promise<void> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `chtype/${encodeObject(object)}/type/2`,
                dataType: 'json',
                s3RegionId: s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }, reject);
        });
    }

    getFrozenInfo(s3RegionId: string, object: Object): Promise<FrozenInfo> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `stat/${encodeObject(object)}`,
                dataType: 'json',
                s3RegionId: s3RegionId,
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
            }, reject);
        });
    }

    unfreeze(s3RegionId: string, object: Object, days: number): Promise<void> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `restoreAr/${encodeObject(object)}/freezeAfterDays/${days}`,
                dataType: 'json',
                s3RegionId: s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }, reject);
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

        this.client.call({
            method: 'POST',
            serviceName: ServiceName.Rsf,
            path: 'list',
            dataType: 'json',
            s3RegionId: s3RegionId,
            query: query,
            contentType: 'application/x-www-form-urlencoded',
        }).then((response) => {
            let isEmpty = true;
            if (response.data.items && response.data.items.length > 0) {
                isEmpty = false;
                results.objects = results.objects.concat(response.data.items.map((item: any) => {
                    return {
                        bucket: bucket, key: item.key, size: item.fsize,
                        lastModified: new Date(item.putTime / 10000), storageClass: toStorageClass(item.type),
                    };
                }));
            }
            if (response.data.commonPrefixes && response.data.commonPrefixes.length > 0) {
                isEmpty = false;
                if (!results.commonPrefixes) {
                    results.commonPrefixes = [];
                }
                results.commonPrefixes = results.commonPrefixes.concat(response.data.commonPrefixes.map((commonPrefix: string) => {
                    return { bucket: bucket, key: commonPrefix };
                }));
            }
            if (!isEmpty && response.data.marker) {
                newOption.nextContinuationToken = response.data.marker;
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
        }, reject);
    }

    createMultipartUpload(s3RegionId: string, object: Object, _originalFileName: string, _header?: SetObjectHeader): Promise<InitPartsOutput> {
        return new Promise((resolve, reject) => {
            const token = makeUploadToken(this.adapterOption.accessKey, this.adapterOption.secretKey, newUploadPolicy(object.bucket, object.key));
            const path = `/buckets/${object.bucket}/objects/${urlSafeBase64(object.key)}/uploads`;
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Up,
                path: path,
                dataType: 'json',
                s3RegionId: s3RegionId,
                contentType: 'application/x-www-form-urlencoded',
                headers: { 'authorization': `UpToken ${token}` },
            }).then((response) => {
                resolve({ uploadId: response.data.uploadId });
            }, reject);
        });
    }

    uploadPart(s3RegionId: string, object: Object, uploadId: string, partNumber: number, data: Buffer, progressCallback?: ProgressCallback): Promise<UploadPartOutput> {
        return new Promise((resolve, reject) => {
            const token = makeUploadToken(this.adapterOption.accessKey, this.adapterOption.secretKey, newUploadPolicy(object.bucket, object.key));
            const path = `/buckets/${object.bucket}/objects/${urlSafeBase64(object.key)}/uploads/${uploadId}/${partNumber}`;
            this.client.call({
                method: 'PUT',
                serviceName: ServiceName.Up,
                path: path,
                data: data,
                dataType: 'json',
                s3RegionId: s3RegionId,
                contentType: 'application/octet-stream',
                headers: {
                    'authorization': `UpToken ${token}`,
                    'content-md5': md5.hex(data),
                },
                uploadProgress: progressCallback,
            }).then((response) => {
                resolve({ etag: response.data.etag });
            }, reject);
        });
    }

    completeMultipartUpload(s3RegionId: string, object: Object, uploadId: string, parts: Array<Part>, originalFileName: string, header?: SetObjectHeader): Promise<void> {
        return new Promise((resolve, reject) => {
            const token = makeUploadToken(this.adapterOption.accessKey, this.adapterOption.secretKey, newUploadPolicy(object.bucket, object.key));
            const path = `/buckets/${object.bucket}/objects/${urlSafeBase64(object.key)}/uploads/${uploadId}`;
            const metadata: { [metaKey: string]: string; } = {};
            if (header?.metadata) {
                for (const [metaKey, metaValue] of Object.entries(header!.metadata)) {
                    metadata[`x-qn-meta-${metaKey}`] = metaValue;
                }
            }
            const data: any = { fname: originalFileName, parts: parts, metadata: metadata };
            if (header?.contentType) {
                data.mimeType = header!.contentType;
            }

            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Up,
                path: path,
                data: JSON.stringify(data),
                dataType: 'json',
                s3RegionId: s3RegionId,
                headers: { 'authorization': `UpToken ${token}` },
            }).then(() => { resolve(); }, reject);
        });
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
            metadata[<string>metaKey.substring('x-qn-meta-'.length)] = <string>metaValue;
        }
    }
    return { size: size, contentType: contentType, lastModified: lastModified, metadata: metadata };
}


export interface BucketIdName {
    id: string;
    name: string;
}
