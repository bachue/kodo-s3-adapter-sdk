import os from 'os';
import pkg from './package.json';
import { Adapter, AdapterOption } from './adapter';
import { KodoHttpClient, ServiceName } from './kodo-http-client';

export const USER_AGENT: string = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/kodo`;

export class Kodo implements Adapter {
    private client: KodoHttpClient;

    constructor(adapterOption: AdapterOption) {
        let userAgent: string = USER_AGENT;
        if (adapterOption.appendedUserAgent) {
            userAgent += `/${adapterOption.appendedUserAgent}`;
        }
        this.client = new KodoHttpClient({
            accessKey: adapterOption.accessKey,
            secretKey: adapterOption.secretKey,
            ucUrl: adapterOption.ucUrl,
            userAgent: userAgent,
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
}

export interface BucketIdName {
    id: string;
    name: string;
}
