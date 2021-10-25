import os from 'os';
import pkg from './package.json';
import { generateAccessTokenV2 } from './kodo-auth';
import { RequestInfo, ResponseInfo } from './adapter';
import { UplogBuffer } from './uplog';
import { HttpClient, RequestStats } from './http-client';

export const USER_AGENT = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/kodo/region`;
export const DEFAULT_UC_URL = 'https://uc.qbox.me';

export interface RegionRequestOptions {
    timeout?: number | number[];
    retry?: number;
    retryDelay?: number;
    stats?: RequestStats,
}

interface RequestOptions extends RegionRequestOptions {
    accessKey: string;
    ucUrl?: string;
    appName?: string;
    appVersion?: string;
    uplogBufferSize?: number;
    requestCallback?: (request: RequestInfo) => void;
    responseCallback?: (response: ResponseInfo) => void;
}

export interface GetAllOptions extends RequestOptions {
    secretKey: string;
}
export interface QueryOptions extends RequestOptions {
    bucketName: string;
}

export class Region {
    upUrls: string[] = [];
    ucUrls: string[] = [];
    rsUrls: string[] = [];
    rsfUrls: string[] = [];
    apiUrls: string[] = [];
    s3Urls: string[] = [];
    constructor(readonly id: string,
        readonly s3Id: string,
        readonly label?: string,
        readonly translatedLabels?: { [lang: string]: string; }) {
    }

    static getAll(options: GetAllOptions): Promise<Region[]> {
        const ucUrl: string = options.ucUrl ?? DEFAULT_UC_URL;
        const requestURL = new URL(`${ucUrl}/regions`);
        const uplogBuffer = new UplogBuffer({
            appName: options.appName, appVersion: options.appVersion,
            bufferSize: options.uplogBufferSize,
        });
        const httpClient = new HttpClient({
            accessKey: options.accessKey,
            secretKey: options.secretKey,
            protocol: requestURL.protocol === 'https' ? 'https' : 'http',
            timeout: options.timeout,
            userAgent: USER_AGENT,
            retry: options.retry,
            retryDelay: options.retryDelay,
            requestCallback: options.requestCallback,
            responseCallback: options.responseCallback,
        }, uplogBuffer);

        return new Promise((resolve, reject) => {
            httpClient.call([requestURL.toString()], {
                fullUrl: true,
                appendAuthorization: false,
                method: 'GET',
                dataType: 'json',
                headers: {
                    'authorization': generateAccessTokenV2(
                        options.accessKey, options.secretKey, requestURL.toString(), 'GET'),
                },
                stats: options.stats,
            }).then((response) => {
                response.data.regions ??= [];
                const regions: Region[] = response.data.regions.map((r: any) => Region.fromResponseBody(ucUrl, r));
                resolve(regions);
            }, reject);
        });
    }

    static query(options: QueryOptions): Promise<Region> {
        const ucUrl: string = options.ucUrl ?? DEFAULT_UC_URL;
        const requestURL = new URL(`${ucUrl}/v4/query`);
        requestURL.searchParams.append('ak', options.accessKey);
        requestURL.searchParams.append('bucket', options.bucketName);

        const uplogBuffer = new UplogBuffer({
            appName: options.appName, appVersion: options.appVersion,
            bufferSize: options.uplogBufferSize,
        });
        const httpClient = new HttpClient({
            accessKey: options.accessKey,
            protocol: requestURL.protocol === 'https' ? 'https' : 'http',
            timeout: options.timeout,
            userAgent: USER_AGENT,
            retry: options.retry,
            retryDelay: options.retryDelay,
            requestCallback: options.requestCallback,
            responseCallback: options.responseCallback,
        }, uplogBuffer);

        return new Promise((resolve, reject) => {
            httpClient.call([requestURL.toString()], {
                fullUrl: true,
                appendAuthorization: false,
                method: 'GET',
                dataType: 'json',
                stats: options.stats,
            }).then((response) => {
                let r: any = null;
                try {
                    r = response.data.hosts[0];
                } catch {
                    const error = new Error('Invalid uc query v4 body');
                    reject(error);
                    return;
                }
                const region: Region = Region.fromResponseBody(ucUrl, r);
                resolve(region);
            }, reject);
        });
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

