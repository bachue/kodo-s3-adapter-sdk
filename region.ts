import { HttpClient2, HttpClientResponse } from 'urllib';
import os from 'os';
import pkg from './package.json';
import { generateAccessTokenV2 } from './kodo-auth';

export const USER_AGENT: string = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/kodo/region`;
export const DEFAULT_UC_URL: string = 'https://uc.qbox.me';

export class Region {
    private static readonly httpClient: HttpClient2 = new HttpClient2();
    upUrls: Array<string> = [];
    ucUrls: Array<string> = [];
    rsUrls: Array<string> = [];
    rsfUrls: Array<string> = [];
    apiUrls: Array<string> = [];
    s3Urls: Array<string> = [];
    constructor(readonly id: string, readonly s3Id: string) {
    }

    static getAll(options: { accessKey: string, secretKey: string, ucUrl?: string }): Promise<Array<Region>> {
        const ucUrl: string = options.ucUrl ?? DEFAULT_UC_URL;
        const requestURI: string = `${ucUrl}/regions`;
        return new Promise((resolve, reject) => {
            Region.httpClient.request(requestURI,
                {
                    method: 'GET',
                    dataType: 'json',
                    headers: {
                        'user-agent': USER_AGENT,
                        'authorization': generateAccessTokenV2(options.accessKey, options.secretKey, requestURI, 'GET'),
                    },
                    retry: 5,
                    retryDelay: 500,
                    isRetry: Region.isRetry,
                }).then((response) => {
                    if (response.status >= 200 && response.status < 400) {
                        const regions: Array<Region> = response.data.regions.map((r: any) => Region.fromResponseBody(ucUrl, r));
                        resolve(regions);
                    } else if (response.data.error) {
                        reject(new Error(response.data.error));
                    } else {
                        try {
                            const data = JSON.parse(response.data);
                            if (data.error) {
                                reject(new Error(data.error));
                            } else {
                                reject(new Error(response.res.statusMessage));
                            }
                        } catch {
                            reject(new Error(response.res.statusMessage));
                        }
                    }
                }, reject);
        });
    }

    static query(options: { accessKey: string, bucketName: string, ucUrl?: string }): Promise<Region> {
        const ucUrl: string = options.ucUrl ?? DEFAULT_UC_URL;
        return new Promise((resolve, reject) => {
            const requestURI = `${ucUrl}/v4/query`;
            Region.httpClient.request(requestURI,
                {
                    data: { ak: options.accessKey, bucket: options.bucketName },
                    dataAsQueryString: true,
                    dataType: 'json',
                    headers: { 'user-agent': USER_AGENT },
                    retry: 5,
                    retryDelay: 500,
                    isRetry: Region.isRetry,
                }).then((response) => {
                    if (response.status >= 200 && response.status < 400) {
                        let r: any = null;
                        try {
                            r = response.data.hosts[0];
                        } catch {
                            reject(new Error('Invalid uc query v4 body'));
                            return;
                        };
                        const region: Region = Region.fromResponseBody(ucUrl, r);
                        resolve(region);
                    } else if (response.data.error) {
                        reject(new Error(response.data.error));
                    } else {
                        try {
                            const data = JSON.parse(response.data);
                            if (data.error) {
                                reject(new Error(data.error));
                            } else {
                                reject(new Error(response.res.statusMessage));
                            }
                        } catch {
                            reject(new Error(response.res.statusMessage));
                        }
                    }
                }, reject);
        });
    }

    private static isRetry(response: HttpClientResponse<any>): boolean {
        const dontRetryStatusCodes: Array<number> = [501, 579, 599, 608, 612, 614, 616,
                                                     618, 630, 631, 632, 640, 701];
        return !response.headers['x-reqid'] ||
            response.status >= 500 && !dontRetryStatusCodes.find((status) => status === response.status);
    }

    private static fromResponseBody(ucUrl: string, r: any): Region {
        const region: Region = new Region(r.region ?? r.id, r.s3.region_alias);
        const domain2Url = (domain: string) => {
            const url = new URL(ucUrl);
            url.host = domain;
            return url.toString();
        };
        region.upUrls = r.up.domains.map(domain2Url);
        region.ucUrls = r.uc.domains.map(domain2Url);
        region.rsUrls = r.rs.domains.map(domain2Url);
        region.rsfUrls = r.rsf.domains.map(domain2Url);
        region.apiUrls = r.api.domains.map(domain2Url);
        region.s3Urls = r.s3.domains.map(domain2Url);
        return region;
    }
}

