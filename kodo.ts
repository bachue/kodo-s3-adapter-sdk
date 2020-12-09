import AsyncLock from 'async-lock';
import os from 'os';
import pkg from './package.json';
import FormData from 'form-data';
import CRC32 from 'buffer-crc32';
import { Semaphore } from 'semaphore-promise';
import { URL, URLSearchParams } from 'url';
import { HttpClient2, HttpClientResponse } from 'urllib';
import { encode as base64Encode } from 'js-base64';
import { base64ToUrlSafe, newUploadPolicy, makeUploadToken, signPrivateURL } from './kodo-auth';
import { Adapter, AdapterOption, Bucket, Domain, Object, SetObjectHeader, ObjectGetResult, ObjectHeader, TransferObject, PartialObjectError, BatchCallback } from './adapter';
import { KodoHttpClient, ServiceName } from './kodo-http-client';

export const USER_AGENT: string = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/kodo`;

export class Kodo implements Adapter {
    private static readonly httpClient: HttpClient2 = new HttpClient2();
    private readonly client: KodoHttpClient;
    private readonly bucketDomainsCache: { [bucketName: string]: Array<Domain>; } = {};
    private readonly bucketDomainsCacheLock = new AsyncLock();

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
    }

    createBucket(region: string, bucket: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Uc,
                path: `mkbucketv3/${bucket}/region/${region}`,
            }).then(() => {
                resolve();
            }, reject);
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
                resolve(response.data.region);
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
                const bucketInfos: Array<Bucket> = response.data.map((info: any) => {
                    return {
                        id: info.id, name: info.tbl,
                        createDate: new Date(info.ctime * 1000),
                        regionId: info.region,
                    };
                });
                resolve(bucketInfos);
            }, reject);
        });
    }

    listDomains(region: string, bucket: string): Promise<Array<Domain>> {
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
                    regionId: region,
                }),
                this.client.call({
                    method: 'POST',
                    serviceName: ServiceName.Uc,
                    path: 'v2/bucketInfo',
                    query: getBucketInfoQuery,
                    dataType: 'json',
                    regionId: region,
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

    _listDomains(region: string, bucket: string): Promise<Array<Domain>> {
        return new Promise((resolve, reject) => {
            if (this.bucketDomainsCache[bucket]) {
                resolve(this.bucketDomainsCache[bucket]);
                return;
            }

            this.bucketDomainsCacheLock.acquire(bucket, (): Promise<Array<Domain>> => {
                if (this.bucketDomainsCache[bucket]) {
                    return new Promise((resolve) => { resolve(this.bucketDomainsCache[bucket]); });
                }
                return this.listDomains(region, bucket);
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

    isExists(region: string, object: Object): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'GET',
                serviceName: ServiceName.Rs,
                path: `stat/${encodeObject(object)}`,
                dataType: 'json',
                regionId: region,
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

    deleteObject(region: string, object: Object): Promise<void> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `delete/${encodeObject(object)}`,
                dataType: 'json',
                regionId: region,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }, reject);
        });
    }

    putObject(region: string, object: Object, data: Buffer, header?: SetObjectHeader): Promise<void> {
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
            form.append('file', data);
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Up,
                dataType: 'json',
                regionId: region,
                contentType: form.getHeaders()['content-type'],
                form: form,
            }).then(() => { resolve(); }, reject);
        });
    }

    getObject(region: string, object: Object, domain?: Domain): Promise<ObjectGetResult> {
        return new Promise((resolve, reject) => {
            this.getObjectURL(region, object, domain).then((url) => {
                Kodo.httpClient.request(url.toString(), {
                    method: 'GET',
                    timeout: [30000, 300000],
                    retry: 10,
                    retryDelay: 500,
                    followRedirect: true,
                    gzip: true,
                }).then((response: HttpClientResponse<Buffer>) => {
                    resolve({ data: response.data, header: this._getObjectHeader(response)});
                }, reject);
            }, reject);
        });
    }

    getObjectURL(region: string, object: Object, domain?: Domain, deadline?: Date): Promise<URL> {
        return new Promise((resolve, reject) => {
            const domainPromise: Promise<Domain> = new Promise((resolve, reject) => {
                if (domain) {
                    resolve(domain);
                    return;
                }
                this._listDomains(region, object.bucket).then((domains) => {
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

    getObjectHeader(region: string, object: Object, domain?: Domain): Promise<ObjectHeader> {
        return new Promise((resolve, reject) => {
            this.getObjectURL(region, object, domain).then((url) => {
                Kodo.httpClient.request(url.toString(), {
                    method: 'HEAD',
                    timeout: [30000, 300000],
                    retry: 10,
                    retryDelay: 500,
                    followRedirect: true,
                }).then((response: HttpClientResponse<Buffer>) => {
                    resolve(this._getObjectHeader(response));
                }, reject);
            }, reject);
        });
    }

    private _getObjectHeader(response: HttpClientResponse<Buffer>): ObjectHeader {
        const size: number = parseInt(response.headers['content-length']! as string);
        const lastModified: Date = new Date(response.headers['last-modified']! as string);
        const metadata: { [key: string]: string; } = {};
        for (const [metaKey, metaValue] of Object.entries(response.headers)) {
            if (metaKey?.startsWith('x-qn-meta-')) {
                metadata[<string>metaKey.substring('x-qn-meta-'.length)] = <string>metaValue;
            }
        }
        return { size: size, lastModified: lastModified, metadata: metadata };
    }

    moveObject(region: string, transferObject: TransferObject): Promise<void> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `move/${encodeObject(transferObject.from)}/${encodeObject(transferObject.to)}`,
                dataType: 'json',
                regionId: region,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }, reject);
        });
    }

    copyObject(region: string, transferObject: TransferObject): Promise<void> {
        return new Promise((resolve, reject) => {
            this.client.call({
                method: 'POST',
                serviceName: ServiceName.Rs,
                path: `copy/${encodeObject(transferObject.from)}/${encodeObject(transferObject.to)}`,
                dataType: 'json',
                regionId: region,
                contentType: 'application/x-www-form-urlencoded',
            }).then(() => { resolve(); }, reject);
        });
    }

    moveObjects(region: string, transferObjects: Array<TransferObject>, callback?: BatchCallback): Promise<Array<PartialObjectError>> {
        return this.moveOrCopyObjects('move', 100, region, transferObjects, callback);
    }

    copyObjects(region: string, transferObjects: Array<TransferObject>, callback?: BatchCallback): Promise<Array<PartialObjectError>> {
        return this.moveOrCopyObjects('copy', 100, region, transferObjects, callback);
    }

    private moveOrCopyObjects(op: string, batchCount: number, region: string, transferObjects: Array<TransferObject>, callback?: BatchCallback): Promise<Array<PartialObjectError>> {
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
            return new Promise((resolve) => {
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
                        regionId: region,
                        contentType: 'application/x-www-form-urlencoded',
                        data: params.toString(),
                    }).then((response) => {
                        const results: Array<PartialObjectError> = response.data.map((item: any, index: number) => {
                            const currentIndex = firstIndexInCurrentBatch + index;
                            const result: PartialObjectError = { bucket: batch[index].from.bucket, key: batch[index].from.key };
                            if (item?.data?.error) {
                                const error = new Error(item?.data?.error);
                                if (callback) {
                                    callback(currentIndex, error);
                                }
                                result.error = error;
                            } else if (callback) {
                                callback(currentIndex);
                            }
                            return result;
                        });
                        resolve(results);
                    }, (error) => {
                        const results: Array<PartialObjectError> = batch.map((transferObject, index) => {
                            const currentIndex = firstIndexInCurrentBatch + index;
                            if (callback) {
                                callback(currentIndex, error);
                            }
                            return { bucket: transferObject.from.bucket, key: transferObject.from.key, error: error };
                        });
                        resolve(results);
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

    deleteObjects(region: string, bucket: string, keys: Array<string>, callback?: BatchCallback): Promise<Array<PartialObjectError>> {
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
            return new Promise((resolve) => {
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
                        regionId: region,
                        contentType: 'application/x-www-form-urlencoded',
                        data: params.toString(),
                    }).then((response) => {
                        const results: Array<PartialObjectError> = response.data.map((item: any, index: number) => {
                            const currentIndex = firstIndexInCurrentBatch + index;
                            const result: PartialObjectError = { bucket: bucket, key: batch[index] };
                            if (item?.data?.error) {
                                const error = new Error(item?.data?.error);
                                if (callback) {
                                    callback(currentIndex, error);
                                }
                                result.error = error;
                            } else if (callback) {
                                callback(currentIndex);
                            }
                            return result;
                        });
                        resolve(results);
                    }, (error) => {
                        const results: Array<PartialObjectError> = batch.map((key, index) => {
                            const currentIndex = firstIndexInCurrentBatch + index;
                            if (callback) {
                                callback(currentIndex, error);
                            }
                            return { bucket: bucket, key: key, error: error };
                        });
                        resolve(results);
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

export interface BucketIdName {
    id: string;
    name: string;
}
