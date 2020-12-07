import AsyncLock from 'async-lock';
import os from 'os';
import pkg from './package.json';
import FormData from 'form-data';
import CRC32 from 'buffer-crc32';
import { URL, URLSearchParams } from 'url';
import { HttpClient2, HttpClientResponse } from 'urllib';
import { encode as base64Encode } from 'js-base64';
import { base64ToUrlSafe, newUploadPolicy, makeUploadToken, signPrivateURL } from './kodo-auth';
import { Adapter, AdapterOption, Bucket, Domain, Object, SetObjectHeader, ObjectGetResult } from './adapter';
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

            Promise.all(promises).then((responses) => {
                const domains: Array<Domain> = responses[0].data.domains.filter((domain: any) => {
                    switch (domain.type) {
                    case 'normal':
                    case 'pan':
                    case 'test':
                        return true;
                    default:
                        return false;
                    }
                }).map((domain: any) => {
                    return { name: domain.name, protocol: domain.protocol, type: domain.type, private: responses[1].data.private != 0 };
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
                    const data: Buffer = response.data;
                    const size: number = parseInt(response.headers['content-length']! as string);
                    const lastModified: Date = new Date(response.headers['last-modified']! as string);
                    const metadata: { [key: string]: string; } = {};
                    for (const [metaKey, metaValue] of Object.entries(response.headers)) {
                        if (metaKey?.startsWith('x-qn-meta-')) {
                            metadata[<string>metaKey] = <string>metaValue;
                        }
                    }
                    resolve({ data: data, header: { size: size, lastModified: lastModified, metadata: metadata }});
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
