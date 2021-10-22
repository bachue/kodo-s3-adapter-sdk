import AsyncLock from 'async-lock';
import FormData from 'form-data';
import { HttpClientResponse, HttpMethod } from 'urllib';
import { URLSearchParams } from 'url';
import { AdapterOption } from './adapter';
import { Region } from './region';
import { RegionService } from './region_service';
import { makeUploadToken, newUploadPolicy } from './kodo-auth';
import { Throttle } from 'stream-throttle';
import zlib from 'zlib';
import { UplogBuffer, UplogEntry } from './uplog';
import { HttpClient, RequestStats, URLRequestOptions } from './http-client';

export type HttpProtocol = "http" | "https";

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
    stats?: RequestStats,
}

export interface SharedRequestOptions extends AdapterOption {
    protocol?: HttpProtocol,
    timeout?: number | number[];
    userAgent?: string;
    retry?: number;
    retryDelay?: number;
}

export class KodoHttpClient {
    private readonly regionsCache: { [key: string]: Region; } = {};
    private readonly regionsCacheLock = new AsyncLock();
    private readonly regionService: RegionService;
    private static logClientId: string | undefined = undefined;
    private readonly uplogBuffer: UplogBuffer;
    private readonly httpClient: HttpClient;

    constructor(private readonly sharedOptions: SharedRequestOptions) {
        this.regionService = new RegionService(sharedOptions);
        this.uplogBuffer = new UplogBuffer({
            appName: sharedOptions.appName, appVersion: sharedOptions.appVersion,
            bufferSize: sharedOptions.uplogBufferSize,
            onBufferFull: (buffer: Buffer): Promise<void> => {
                return this.sendUplog(buffer);
            }
        });
        this.httpClient = new HttpClient(sharedOptions, this.uplogBuffer);
    }

    call<T = any>(options: RequestOptions): Promise<HttpClientResponse<T>> {
        return new Promise((resolve, reject) => {
            this.getServiceUrls(options.serviceName, options.bucketName, options.s3RegionId, options.stats).then((urls) => {
                this.callUrls(urls, {
                    method: options.method,
                    path: options.path,
                    query: options.query,
                    data: options.data,
                    dataType: options.dataType,
                    form: options.form,
                    contentType: options.contentType,
                    headers: options.headers,
                    uploadProgress: options.uploadProgress,
                    uploadThrottle: options.uploadThrottle,
                    stats: options.stats,
                }).then(resolve, reject);
            }).catch(reject);
        });
    }

    callUrls<T = any>(urls: Array<string>, options: URLRequestOptions): Promise<HttpClientResponse<T>> {
        return this.httpClient.call(urls, options);
    }

    clearCache() {
        Object.keys(this.regionsCache).forEach((key) => { delete this.regionsCache[key]; });
        this.regionService.clearCache();
    }

    private getServiceUrls(serviceName: ServiceName, bucketName?: string, s3RegionId?: string, stats?: RequestStats): Promise<Array<string>> {
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
                        timeout: this.sharedOptions.timeout,
                        retry: this.sharedOptions.retry,
                        retryDelay: this.sharedOptions.retryDelay,
                        appName: this.sharedOptions.appName,
                        appVersion: this.sharedOptions.appVersion,
                        uplogBufferSize: this.sharedOptions.uplogBufferSize,
                        requestCallback: this.sharedOptions.requestCallback,
                        responseCallback: this.sharedOptions.responseCallback,
                        stats: stats,
                    });
                } else {
                    return new Promise((resolve, reject) => {
                        this.regionService.getAllRegions({
                            timeout: this.sharedOptions.timeout,
                            retry: this.sharedOptions.retry,
                            retryDelay: this.sharedOptions.retryDelay,
                            stats: stats,
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
                        }).catch(reject);
                    });
                }
            }).then((region: Region) => {
                this.regionsCache[key] = region;
                resolve(this.getUrlsFromRegion(serviceName, region));
            }).catch(reject);
        });
    }

    log(entry: UplogEntry): Promise<void> {
        return this.uplogBuffer.log(entry);
    }

    private sendUplog(logBuffer: Buffer): Promise<void> {
        return new Promise((resolve, reject) => {
            const query = new URLSearchParams();
            query.set("compressed", "gzip");
            const token = makeUploadToken(
                this.sharedOptions.accessKey,
                this.sharedOptions.secretKey,
                newUploadPolicy({
                    bucket: "testbucket",
                }));
            let headers: { [headerName: string]: string; } = { 'authorization': `UpToken ${token}` };
            if (KodoHttpClient.logClientId) {
                headers['X-Log-Client-Id'] = KodoHttpClient.logClientId;
            }

            zlib.gzip(logBuffer, { level: zlib.constants.Z_BEST_COMPRESSION }, (err, compressedLog) => {
                if (err) {
                    reject(err);
                    return;
                }
                this.call({
                    method: 'POST',
                    serviceName: ServiceName.Uplog,
                    path: 'log/4',
                    query: query,
                    headers: headers,
                    data: compressedLog,
                }).then((response) => {
                    if (!KodoHttpClient.logClientId && response.headers['X-Log-Client-Id']) {
                        KodoHttpClient.logClientId = response.headers['X-Log-Client-Id'].toString();
                    }
                    resolve();
                }).catch(reject);
            });
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
            case ServiceName.Qcdn:
                return ['https://api.qiniu.com'];
            case ServiceName.Portal:
                return ['https://portal.qiniu.com'];
            case ServiceName.Uplog:
                return ['http://uplog.qbox.me'];
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
    Qcdn,
    Portal,
    Uplog,
}
