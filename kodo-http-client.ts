import AsyncLock from 'async-lock';
import http from 'http';
import FormData from 'form-data';
import { HttpClient2, RequestOptions2, HttpClientResponse, HttpMethod } from 'urllib';
import { URL, URLSearchParams } from 'url';
import { Region } from './region';
import { generateAccessTokenV2 } from './kodo-auth';
import { ReadableStreamBuffer } from 'stream-buffers';
import { Throttle } from 'stream-throttle';

export type HttpProtocol = "http" | "https";

export interface SharedRequestOptions {
    ucUrl?: string;
    accessKey: string;
    secretKey: string;
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

    constructor(private readonly sharedOptions: SharedRequestOptions) {
    }

    call<T = any>(options: RequestOptions): Promise<HttpClientResponse<T>> {
        return new Promise((resolve, reject) => {
            this.getServiceUrls(options.serviceName, options.bucketName, options.s3RegionId).then((urls) => {
                this.callForOneUrl(urls, options, resolve, reject);
            }, reject);
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
            if (callbackError) {
                return;
            } else if (response.status >= 200 && response.status < 400) {
                resolve(response);
            } else if (urls.length > 0) {
                this.callForOneUrl(urls, options, resolve, reject);
            } else if (response.data.error) {
                reject(new Error(response.data.error));
            } else {
                try {
                    const data: any = JSON.parse(response.data);
                    if (data.error) {
                        reject(new Error(data.error));
                    } else {
                        reject(new Error(response.res.statusMessage));
                    }
                } catch {
                    reject(new Error(response.res.statusMessage));
                }
            }
        }).catch((err) => {
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
                    return new Promise((resolve) => { resolve(this.regionsCache[key]); });
                } else if (bucketName) {
                    return Region.query({
                        bucketName,
                        accessKey: this.sharedOptions.accessKey,
                        ucUrl: this.sharedOptions.ucUrl,
                    });
                } else {
                    return new Promise((resolve, reject) => {
                        Region.getAll({
                            accessKey: this.sharedOptions.accessKey,
                            secretKey: this.sharedOptions.secretKey,
                            ucUrl: this.sharedOptions.ucUrl,
                        }).then((regions) => {
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
                        }, reject);
                    });
                }
            }).then((region: Region) => {
                this.regionsCache[key] = region;
                resolve(this.getUrlsFromRegion(serviceName, region));
            }, reject);
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
}

