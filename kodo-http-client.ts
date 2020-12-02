import http from 'http';
import { HttpClient2, HttpClientResponse, HttpMethod } from 'urllib';
import { URL, URLSearchParams } from 'url';
import { Region } from './region';
import { generateAccessTokenV2 } from './kodo-auth';

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
    serviceName: ServiceName;
    data?: any;
    dataType?: string;
    contentType?: string;
}

export class KodoHttpClient {
    private static readonly httpClient: HttpClient2 = new HttpClient2();
    private readonly regionsCache: { [key: string]: Region; } = {};

    constructor(private readonly sharedOptions: SharedRequestOptions) {
    }

    call<T = any>(options: RequestOptions): Promise<HttpClientResponse<T>> {
        return new Promise((resolve, reject) => {
            this.getServiceUrls(options.serviceName, options.bucketName).then((urls) => {
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

        KodoHttpClient.httpClient.request(url, {
            method: options.method,
            data: options.data,
            dataType: options.dataType,
            contentType: options.contentType,
            headers: headers,
            timeout: this.sharedOptions.timeout,
            followRedirect: true,
            retry: this.sharedOptions.retry,
            retryDelay: this.sharedOptions.retryDelay,
            isRetry: this.isRetry,
        }).then((response) => {
            if (response.status >= 200 && response.status < 400) {
                resolve(response);
            } else if (urls.length > 0) {
                this.callForOneUrl(urls, options, resolve, reject);
            } else if (response.data.error) {
                reject(new Error(response.data.error));
            } else {
                reject(new Error('Unknown response error'));
            }
        }).catch((err) => {
            if (urls.length > 0) {
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
            options.method ?? 'GET',
            options.contentType ?? 'application/octet-stream',
            data);
    }

    private getServiceUrls(serviceName: ServiceName, bucketName?: string): Promise<Array<string>> {
        return new Promise((resolve, reject) => {
            const key: string = `${this.sharedOptions.ucUrl}/${this.sharedOptions.accessKey}/${bucketName}`;
            const region: Region | null = this.regionsCache[key];
            if (region) {
                resolve(this.getUrlsFromRegion(serviceName, region));
            } else if (bucketName) {
                Region.query({
                    bucketName,
                    accessKey: this.sharedOptions.accessKey,
                    ucUrl: this.sharedOptions.ucUrl,
                }).then((region) => {
                    this.regionsCache[key] = region;
                    resolve(this.getUrlsFromRegion(serviceName, region));
                }, reject);
            } else {
                Region.getAll({
                    accessKey: this.sharedOptions.accessKey,
                    secretKey: this.sharedOptions.secretKey,
                    ucUrl: this.sharedOptions.ucUrl,
                }).then((regions) => {
                    if (regions.length == 0) {
                        throw new Error('regions is empty');
                    }
                    const region = regions[0];
                    this.regionsCache[key] = region;
                    resolve(this.getUrlsFromRegion(serviceName, region));
                }, reject);
            }
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

