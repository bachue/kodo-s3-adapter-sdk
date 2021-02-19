import AsyncLock from 'async-lock';
import http from 'http';
import FormData from 'form-data';
import { HttpClient2, RequestOptions2, HttpClientResponse, HttpMethod } from 'urllib';
import { URL, URLSearchParams } from 'url';
import { Region } from './region';
import { RegionService } from './region_service';
import { generateAccessTokenV2 } from './kodo-auth';
import { AdapterOption, RequestInfo, ResponseInfo } from './adapter';
import { ReadableStreamBuffer } from 'stream-buffers';
import { Throttle } from 'stream-throttle';

export type HttpProtocol = "http" | "https";

export interface SharedRequestOptions extends AdapterOption {
    protocol?: HttpProtocol,
    timeout?: number | number[];
    userAgent?: string;
    retry?: number;
    retryDelay?: number;
}

export interface RequestOptions {
    method?: HttpMethod;
    path?: string;
    query?: URLSearchParams;
    bucketName?: string;
    s3RegionId?: string;
    serviceName: ServiceName;
    data?: any;
    dataType?: string;
    form?: FormData;
    contentType?: string;
    headers?: { [headerName: string]: string; },
    uploadProgress?: (uploaded: number, total: number) => void,
    uploadThrottle?: Throttle,
}

export class KodoHttpClient {
    private static readonly httpClient: HttpClient2 = new HttpClient2();
    private readonly regionsCache: { [key: string]: Region; } = {};
    private readonly regionsCacheLock = new AsyncLock();
    private readonly regionService: RegionService;

    constructor(private readonly sharedOptions: SharedRequestOptions) {
        this.regionService = new RegionService(sharedOptions);
    }

    call<T = any>(options: RequestOptions): Promise<HttpClientResponse<T>> {
        return new Promise((resolve, reject) => {
            this.getServiceUrls(options.serviceName, options.bucketName, options.s3RegionId).then((urls) => {
                this.callForOneUrl(urls, options, resolve, reject);
            }).catch(reject);
        });
    }

    private callForOneUrl(urls: Array<string>, options: RequestOptions, resolve: (value?: any) => void, reject: (error?: any) => void): void {
        const urlString: string | undefined = urls.shift();
        if (!urlString) {
            reject(new Error('urls is empty'));
            return;
        }

        const url: string = this.makeUrl(urlString, options);
        const headers: http.IncomingHttpHeaders = {
            'authorization': this.makeAuthorization(url, options),
            'user-agent': this.sharedOptions.userAgent,
        };
        if (options.contentType !== 'json') {
            headers['content-type'] = options.contentType;
        }
        if (options.headers) {
            for (const [headerName, headerValue] of Object.entries(options.headers)) {
                headers[headerName] = headerValue;
            }
        }

        let requestInfo: RequestInfo | undefined = undefined;
        const beginTime = new Date().getTime();

        const requestOption: RequestOptions2 = {
            method: options.method,
            dataType: options.dataType,
            contentType: options.contentType,
            headers: headers,
            timeout: this.sharedOptions.timeout,
            followRedirect: true,
            retry: this.sharedOptions.retry,
            retryDelay: this.sharedOptions.retryDelay,
            isRetry: this.isRetry,
            beforeRequest: (info) => {
                requestInfo = {
                    url: url,
                    method: info.method,
                    headers: info.headers,
                };
                if (this.sharedOptions.requestCallback) {
                    this.sharedOptions.requestCallback(requestInfo);
                }
            },
        };
        const data: any = options.data ?? options.form?.getBuffer();
        let callbackError: Error | undefined = undefined;
        if (data) {
            if (options.uploadProgress) {
                const stream = new ReadableStreamBuffer({ initialSize: data.length, chunkSize: 1 << 20 });
                stream.put(data);
                stream.stop();
                let uploaded = 0;
                let total = data.length;
                stream.on('data', (chunk) => {
                    uploaded += chunk.length;
                    try {
                        options.uploadProgress!(uploaded, total);
                    } catch (err) {
                        if (!stream.destroyed) {
                            stream.destroy(err);
                        }
                        callbackError = err;
                        reject(err);
                    }
                });
                if (options.uploadThrottle) {
                    requestOption.stream = stream.pipe(options.uploadThrottle);
                } else {
                    requestOption.stream = stream;
                }
            } else if (options.uploadThrottle) {
                const stream = new ReadableStreamBuffer({ initialSize: data.length, chunkSize: 1 << 20 });
                stream.put(data);
                stream.stop();
                requestOption.stream = stream.pipe(options.uploadThrottle);
            } else {
                requestOption.data = data;
            }
        }

        KodoHttpClient.httpClient.request(url, requestOption).then((response) => {
            const responseInfo: ResponseInfo = {
                request: requestInfo!,
                statusCode: response.status,
                headers: response.headers,
                data: response.data,
                interval: new Date().getTime() - beginTime,
            };

            try {
                if (callbackError) {
                    return;
                } else if (response.status >= 200 && response.status < 400) {
                    resolve(response);
                } else if (urls.length > 0) {
                    this.callForOneUrl(urls, options, resolve, reject);
                } else if (response.data.error) {
                    const error = new Error(response.data.error);
                    responseInfo.error = error;
                    reject(error);
                } else {
                    try {
                        const data: any = JSON.parse(response.data);
                        if (data.error) {
                            const error = new Error(data.error);
                            responseInfo.error = error;
                            reject(error);
                        } else {
                            const error = new Error(response.res.statusMessage);
                            responseInfo.error = error;
                            reject(error);
                        }
                    } catch {
                        const error = new Error(response.res.statusMessage);
                        responseInfo.error = error;
                        reject(error);
                    }
                }
            } finally {
                if (this.sharedOptions.responseCallback) {
                    this.sharedOptions.responseCallback(responseInfo);
                }
            }
        }).catch((err) => {
            const responseInfo: ResponseInfo = {
                request: requestInfo!,
                interval: new Date().getTime() - beginTime,
                error: err,
            };

            if (this.sharedOptions.responseCallback) {
                this.sharedOptions.responseCallback(responseInfo);
            }

            if (callbackError) {
                return;
            } else if (urls.length > 0) {
                this.callForOneUrl(urls, options, resolve, reject);
            } else {
                reject(err);
            }
        });
    }

    private isRetry(response: HttpClientResponse<any>): boolean {
        const dontRetryStatusCodes: Array<number> = [501, 579, 599, 608, 612, 614, 616,
                                                     618, 630, 631, 632, 640, 701];
        return !response.headers['x-reqid'] ||
            response.status >= 500 && !dontRetryStatusCodes.find((status) => status === response.status);
    }

    private makeUrl(base: string, options: RequestOptions): string {
        const url = new URL(base);
        if (options.path) {
            url.pathname = options.path;
        }

        let protocol: HttpProtocol | undefined = this.sharedOptions.protocol;
        if (protocol) {
            switch (protocol) {
            case "http":
                url.protocol = "http";
                break;
            case "https":
                url.protocol = "https";
                break;
            }
        }
        if (options.query) {
            options.query.forEach((value, name) => {
                url.searchParams.append(name, value);
            });
        }
        return url.toString();
    }

    private makeAuthorization(url: string, options: RequestOptions): string {
        let data: string | undefined = undefined;
        if (options.data) {
            if (options.dataType === 'json') {
                data = JSON.stringify(options.data);
            }
            data = options.data.toString();
        }

        return generateAccessTokenV2(
            this.sharedOptions.accessKey, this.sharedOptions.secretKey, url.toString(),
            options.method ?? 'GET', options.contentType, data);
    }

    private getServiceUrls(serviceName: ServiceName, bucketName?: string, s3RegionId?: string): Promise<Array<string>> {
        return new Promise((resolve, reject) => {
            let key: string;
            if (s3RegionId) {
                key = `${this.sharedOptions.ucUrl}/${s3RegionId}`;
            } else {
                key = `${this.sharedOptions.ucUrl}/${this.sharedOptions.accessKey}/${bucketName}`;
            }
            if (this.regionsCache[key]) {
                resolve(this.getUrlsFromRegion(serviceName, this.regionsCache[key]));
                return;
            }
            this.regionsCacheLock.acquire(key, (): Promise<Region> => {
                if (this.regionsCache[key]) {
                    return Promise.resolve(this.regionsCache[key]);
                } else if (bucketName) {
                    return Region.query({
                        bucketName,
                        accessKey: this.sharedOptions.accessKey,
                        ucUrl: this.sharedOptions.ucUrl,
                    });
                } else {
                    return new Promise((resolve, reject) => {
                        this.regionService.getAllRegions().then((regions) => {
                            if (regions.length == 0) {
                                reject(Error('regions is empty'));
                                return;
                            }
                            if (s3RegionId) {
                                const region = regions.find((region) => region.s3Id === s3RegionId);
                                if (!region) {
                                    reject(new Error(`Cannot find region of ${s3RegionId}`));
                                    return;
                                }
                                resolve(region);
                            } else {
                                resolve(regions[0]);
                            }
                        }).catch(reject);
                    });
                }
            }).then((region: Region) => {
                this.regionsCache[key] = region;
                resolve(this.getUrlsFromRegion(serviceName, region));
            }).catch(reject);
        });
    }

    private getUrlsFromRegion(serviceName: ServiceName, region: Region): Array<string> {
        switch (serviceName) {
        case ServiceName.Up:
            return [...region.upUrls];
        case ServiceName.Uc:
            return [...region.ucUrls];
        case ServiceName.Rs:
            return [...region.rsUrls];
        case ServiceName.Rsf:
            return [...region.rsfUrls];
        case ServiceName.Api:
            return [...region.apiUrls];
        case ServiceName.S3:
            return [...region.s3Urls];
        case ServiceName.Portal:
            return ['https://portal.qiniu.com'];
        }
    }
}

export enum ServiceName {
    Up,
    Uc,
    Rs,
    Rsf,
    Api,
    S3,
    Portal,
}

