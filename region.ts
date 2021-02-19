import { HttpClient2, HttpClientResponse } from 'urllib';
import os from 'os';
import pkg from './package.json';
import { generateAccessTokenV2 } from './kodo-auth';
import { RequestInfo, ResponseInfo } from './adapter';

export const USER_AGENT: string = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/kodo/region`;
export const DEFAULT_UC_URL: string = 'https://uc.qbox.me';

interface Options {
    accessKey: string;
    ucUrl?: string;
    requestCallback?: (request: RequestInfo) => void;
    responseCallback?: (response: ResponseInfo) => void;
}
export interface GetAllOptions extends Options {
    secretKey: string;
}
export interface QueryOptions extends Options {
    bucketName: string;
}

export class Region {
    private static readonly httpClient: HttpClient2 = new HttpClient2();
    upUrls: Array<string> = [];
    ucUrls: Array<string> = [];
    rsUrls: Array<string> = [];
    rsfUrls: Array<string> = [];
    apiUrls: Array<string> = [];
    s3Urls: Array<string> = [];
    constructor(readonly id: string,
                readonly s3Id: string,
                readonly label?: string,
                readonly translatedLabels?: { [lang: string]: string; }) {
    }

    static getAll(options: GetAllOptions): Promise<Array<Region>> {
        const ucUrl: string = options.ucUrl ?? DEFAULT_UC_URL;
        const requestURI: string = `${ucUrl}/regions`;
        return new Promise((resolve, reject) => {
            let requestInfo: RequestInfo | undefined = undefined;
            const beginTime = new Date().getTime();

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
                    beforeRequest: (info) => {
                        requestInfo = {
                            url: requestURI,
                            method: info.method,
                            headers: info.headers,
                        };
                        if (options.requestCallback) {
                            options.requestCallback(requestInfo);
                        }
                    },
                }).then((response) => {
                    const responseInfo: ResponseInfo = {
                        request: requestInfo!,
                        statusCode: response.status,
                        headers: response.headers,
                        data: response.data,
                        interval: new Date().getTime() - beginTime,
                    };

                    try {
                        if (response.status >= 200 && response.status < 400) {
                            const regions: Array<Region> = response.data.regions.map((r: any) => Region.fromResponseBody(ucUrl, r));
                            resolve(regions);
                        } else {
                            const error = Region.extraError(response);
                            responseInfo.error = error;
                            reject(error);
                        }
                    } finally {
                        if (options.responseCallback) {
                            options.responseCallback(responseInfo);
                        }
                    }
                }).catch((err) => {
                    const responseInfo: ResponseInfo = {
                        request: requestInfo!,
                        interval: new Date().getTime() - beginTime,
                        error: err,
                    };

                    if (options.responseCallback) {
                        options.responseCallback(responseInfo);
                    }

                    reject(err);
                });
        });
    }

    static query(options: QueryOptions): Promise<Region> {
        const ucUrl: string = options.ucUrl ?? DEFAULT_UC_URL;
        return new Promise((resolve, reject) => {
            const requestURI = `${ucUrl}/v4/query`;
            let requestInfo: RequestInfo | undefined = undefined;
            const beginTime = new Date().getTime();

            Region.httpClient.request(requestURI,
                {
                    data: { ak: options.accessKey, bucket: options.bucketName },
                    dataAsQueryString: true,
                    dataType: 'json',
                    headers: { 'user-agent': USER_AGENT },
                    retry: 5,
                    retryDelay: 500,
                    isRetry: Region.isRetry,
                    beforeRequest: (info) => {
                        requestInfo = {
                            url: requestURI,
                            method: info.method,
                            headers: info.headers,
                        };
                        if (options.requestCallback) {
                            options.requestCallback(requestInfo);
                        }
                    },
                }).then((response) => {
                    const responseInfo: ResponseInfo = {
                        request: requestInfo!,
                        statusCode: response.status,
                        headers: response.headers,
                        data: response.data,
                        interval: new Date().getTime() - beginTime,
                    };

                    try {
                        if (response.status >= 200 && response.status < 400) {
                            let r: any = null;
                            try {
                                r = response.data.hosts[0];
                            } catch {
                                const error = new Error('Invalid uc query v4 body');
                                responseInfo.error = error;
                                reject(error);
                                return;
                            };
                            const region: Region = Region.fromResponseBody(ucUrl, r);
                            resolve(region);
                        } else {
                            const error = Region.extraError(response);
                            responseInfo.error = error;
                            reject(error);
                        }
                    } finally {
                        if (options.responseCallback) {
                            options.responseCallback(responseInfo);
                        }
                    }
                }).catch((err) => {
                    const responseInfo: ResponseInfo = {
                        request: requestInfo!,
                        interval: new Date().getTime() - beginTime,
                        error: err,
                    };

                    if (options.responseCallback) {
                        options.responseCallback(responseInfo);
                    }

                    reject(err);
                });
        });
    }

    private static extraError(response: HttpClientResponse<any>): Error {
        let error: Error;

        if (response.data.error) {
            error = new Error(response.data.error);
        } else {
            try {
                const data = JSON.parse(response.data);
                if (data.error) {
                    error = new Error(data.error);
                } else {
                    error = new Error(response.res.statusMessage);
                }
            } catch {
                error = new Error(response.res.statusMessage);
            }
        }
        return error;
    }

    private static isRetry(response: HttpClientResponse<any>): boolean {
        const dontRetryStatusCodes: Array<number> = [501, 579, 599, 608, 612, 614, 616,
                                                     618, 630, 631, 632, 640, 701];
        return !response.headers['x-reqid'] ||
            response.status >= 500 && !dontRetryStatusCodes.find((status) => status === response.status);
    }

    private static fromResponseBody(ucUrl: string, r: any): Region {
        const translatedDescriptions: { [lang: string]: string; } = {};
        for (const fieldName in r) {
            if (fieldName.startsWith("description_")) {
                const langName = fieldName.substring("description_".length);
                translatedDescriptions[langName] = r[fieldName];
            }
        }

        const region: Region = new Region(r.region ?? r.id, r.s3.region_alias, r.description, translatedDescriptions);
        const domain2Url = (domain: string) => {
            const url = new URL(ucUrl);
            return new URL(`${url.protocol}//${domain}`).toString();
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

