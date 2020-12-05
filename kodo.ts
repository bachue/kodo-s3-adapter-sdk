import os from 'os';
import pkg from './package.json';
import FormData from 'form-data';
import CRC32 from 'buffer-crc32';
import { URLSearchParams } from 'url';
import { encode as base64Encode } from 'js-base64';
import { base64ToUrlSafe, NewUploadPolicy, MakeUploadToken } from './kodo-auth';
import { Adapter, AdapterOption, Bucket, Domain, Object, SetObjectHeader } from './adapter';
import { KodoHttpClient, ServiceName } from './kodo-http-client';

export const USER_AGENT: string = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/kodo`;

export class Kodo implements Adapter {
    private client: KodoHttpClient;

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
                const domains: Array<Domain> = responses[0].data.domains.map((domain: any) => {
                    return { domain: domain.name, protocol: domain.protocol, private: responses[1].data.private != 0 };
                });
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
            const token = MakeUploadToken(this.adapterOption.accessKey, this.adapterOption.secretKey, NewUploadPolicy(object.bucket, object.key));
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

    // getObject(region: string, object: Object): Promise<ObjectGetResult> {
    //     return new Promise((resolve, reject) => {

    //     });
    // }

    // getObjectURL(region: string, object: Object): Promise<URL> {
    //     return new Promise((resolve, reject) => {
    //     });
    // }
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
