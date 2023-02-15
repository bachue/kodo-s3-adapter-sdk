import AsyncLock from 'async-lock';
import FormData from 'form-data';
import { HttpClientResponse, HttpMethod } from 'urllib';
import { URLSearchParams } from 'url';
import { AdapterOption } from './adapter';
import { Region } from './region';
import { RegionService } from './region_service';
import { makeUploadToken, newUploadPolicy } from './kodo-auth';
import zlib from 'zlib';
import { UplogBuffer, UplogEntry } from './uplog';
import { HttpClient, RequestStats, URLRequestOptions } from './http-client';

export type HttpProtocol = 'http' | 'https';

export interface RequestOptions {
    method: HttpMethod;
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
    abortSignal?: AbortSignal,
    stats?: RequestStats,
    appendAuthorization?: boolean,

    // for uplog
    apiName: string;
    targetBucket?: string,
    targetKey?: string,
}

export interface SharedRequestOptions extends AdapterOption {
    protocol?: HttpProtocol,
    timeout?: number | number[];
    userAgent?: string;
    retry?: number;
    retryDelay?: number;

    // for uplog
    apiType: 'kodo' | 's3',
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
            bufferSize: sharedOptions.uplogBufferSize,
            onBufferFull: (buffer: Buffer): Promise<void> => {
                return this.sendUplog(buffer);
            }
        });
        this.httpClient = new HttpClient(sharedOptions, this.uplogBuffer);
    }

    async call<T = any>(options: RequestOptions): Promise<HttpClientResponse<T>> {
        const urls = await this.getServiceUrls(options.serviceName, options.bucketName, options.s3RegionId, options.stats);
        return await this.callUrls(urls, {
            method: options.method,
            path: options.path,
            query: options.query,
            data: options.data,
            dataType: options.dataType,
            form: options.form,
            contentType: options.contentType,
            headers: options.headers,
            stats: options.stats,
            appendAuthorization: options.appendAuthorization,
            abortSignal: options.abortSignal,

            // for uplog
            apiName: options.apiName,
            targetBucket: options.targetBucket,
            targetKey: options.targetKey,
        });
    }

    async callUrls<T = any>(urls: string[], options: URLRequestOptions): Promise<HttpClientResponse<T>> {
        return await this.httpClient.call(urls, options);
    }

    clearCache() {
        Object.keys(this.regionsCache).forEach((key) => { delete this.regionsCache[key]; });
        this.regionService.clearCache();
    }

    private async getServiceUrls(
        serviceName: ServiceName,
        bucketName?: string,
        s3RegionId?: string,
        stats?: RequestStats,
    ): Promise<string[]> {
        let key: string;
        if (s3RegionId) {
            key = `${this.sharedOptions.ucUrl}/${s3RegionId}`;
        } else {
            key = `${this.sharedOptions.ucUrl}/${this.sharedOptions.accessKey}/${bucketName}`;
        }
        if (this.regionsCache[key]) {
            return this.getUrlsFromRegion(serviceName, this.regionsCache[key]);
        }
        const region: Region = await this.regionsCacheLock.acquire(key, async (): Promise<Region> => {
            if (this.regionsCache[key]) {
                return this.regionsCache[key];
            }
            if (bucketName) {
                return await Region.query({
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
                    stats,
                });
            }
            const regions = await this.regionService.getAllRegions({
                timeout: this.sharedOptions.timeout,
                retry: this.sharedOptions.retry,
                retryDelay: this.sharedOptions.retryDelay,
                stats,
            });
            if (regions.length == 0) {
                throw new Error('regions is empty');
            }
            if (s3RegionId) {
                const region = regions.find((region) => region.s3Id === s3RegionId);
                if (!region) {
                    throw new Error(`Cannot find region of ${s3RegionId}`);
                }
                return region;
            } else {
                return regions[0];
            }
        });

        this.regionsCache[key] = region;
        return this.getUrlsFromRegion(serviceName, region);
    }

    log(entry: UplogEntry): Promise<void> {
        return this.uplogBuffer.log(entry);
    }

    private async sendUplog(logBuffer: Buffer): Promise<void> {
        const query = new URLSearchParams();
        query.set('compressed', 'gzip');
        const token = makeUploadToken(
            this.sharedOptions.accessKey,
            this.sharedOptions.secretKey,
            newUploadPolicy({
                bucket: 'testbucket',
            }));
        const headers: { [headerName: string]: string; } = { 'authorization': `UpToken ${token}` };
        if (KodoHttpClient.logClientId) {
            headers['X-Log-Client-Id'] = KodoHttpClient.logClientId;
        }

        const compressedLog = await new Promise<Buffer>((resolve, reject) => {
            zlib.gzip(logBuffer, { level: zlib.constants.Z_BEST_COMPRESSION }, (err, compressedLog) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(compressedLog);
            });
        });

        const response = await this.call({
            apiName: 'sendUplog',
            method: 'POST',
            serviceName: ServiceName.Uplog,
            path: 'log/4',
            query,
            headers,
            data: compressedLog,
        });
        if (!KodoHttpClient.logClientId && response.headers['X-Log-Client-Id']) {
            KodoHttpClient.logClientId = response.headers['X-Log-Client-Id'].toString();
        }
    }

    private getUrlsFromRegion(serviceName: ServiceName, region: Region): string[] {
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
