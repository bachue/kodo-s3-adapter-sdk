import os from 'os';

import pkg from './package.json';
import { HttpClient } from './http-client';
import { RequestInfo, ResponseInfo } from './adapter';
import { UplogBuffer } from './uplog';
import { DEFAULT_CENTRAL_API_URL, DEFAULT_PORTAL_URL } from './region';

export const USER_AGENT = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/kodo/share`;

interface ErrorResult {
    error: string,
    error_code: string,
}

// create
export interface CreateShareOptions {
    bucket: string,
    prefix: string,
    durationSeconds: number,
    extractCode: string,
    permission: 'READONLY' | 'READWRITE',
}
export interface CreateShareResult {
    id: string,
    token: string,
    expires: string,
}
type CreateShareResData = CreateShareResult | ErrorResult;

// check
export interface CheckShareOptions {
    shareId: string,
    shareToken: string,
}
export type CheckShareResult = void;
type CheckShareResData = CheckShareResult | ErrorResult;

// verify
export interface VerifyShareOptions {
    shareId: string,
    shareToken: string,
    extractCode: string,
}
export interface VerifyShareResult {
    endpoint: string,
    region: string,
    bucket_name: string,
    bucket_id: string,
    prefix: string,
    federated_ak: string,
    federated_sk: string,
    session_token: string,
    permission: 'READONLY' | 'READWRITE',
    expires: string, // UTC Date string
}
type VerifyShareResData = VerifyShareResult | ErrorResult;

function isErrorResult(data: any): data is ErrorResult {
    return Object.prototype.hasOwnProperty.call(data, 'error') &&
        Object.prototype.hasOwnProperty.call(data, 'error_code');
}

interface ShareServiceOptions {
    apiUrls?: string[],
    ak?: string,
    sk?: string,

    // http client options
    appName: string,
    appVersion: string,
    timeout?: number | number[],
    retry?: number,
    retryDelay?: number,
    requestCallback?: (request: RequestInfo) => void,
    responseCallback?: (response: ResponseInfo) => void,
    disableQiniuTimestampSignature?: boolean,
}

export class AuthRequiredError extends Error {
    constructor(msg: string) {
        super(msg);
    }
}

export class ApiUrlsRequiredError extends Error {
    constructor(msg: string) {
        super(msg);
    }
}

export class RequestBaseError extends Error {
    code: number;
    constructor(code: number, msg?: string) {
        super(msg);
        this.code = code;
    }
}

export class ShareService {
    private httpClient: HttpClient;

    constructor(
        private readonly options: ShareServiceOptions,
    ) {
        const uplogBuffer = new UplogBuffer({
            bufferSize: -1,
        });
        const protocol = (options.apiUrls?.[0] || 'https').startsWith('https') ? 'https' : 'http';
        this.httpClient = new HttpClient(
            {
                accessKey: options.ak || '',
                secretKey: options.sk,
                protocol: protocol,
                timeout: options.timeout,
                userAgent: USER_AGENT,
                retry: options.retry,
                retryDelay: options.retryDelay,
                requestCallback: options.requestCallback,
                responseCallback: options.responseCallback,
                appName: options.appName,
                appVersion: options.appVersion,
                disableQiniuTimestampSignature: options.disableQiniuTimestampSignature,
                apiType: 'kodo',
            },
            uplogBuffer
        );
    }

    async getApiHosts(portalHosts: string[]): Promise<string[]> {
        if (portalHosts.includes(DEFAULT_PORTAL_URL)) {
            return [DEFAULT_CENTRAL_API_URL];
        }

        const requestURL = portalHosts.map(url => `${new URL(url)}api/kodov2/shares/config`);
        const res = await this.httpClient.call(
            requestURL,
            {
                fullUrl: true,
                appendAuthorization: false,
                method: 'GET',
                dataType: 'json',
                apiName: 'getShareConfig',
            }
        );

        if (res.status !== 200) {
            if (isErrorResult(res.data)) {
                throw new RequestBaseError(res.status, res.data.error);
            }
            throw new RequestBaseError(res.status);
        }

        return [res.data.apiHost];
    }

    async createShare(options: CreateShareOptions): Promise<CreateShareResult> {
        if (!this.options.ak || !this.options.sk) {
            throw new AuthRequiredError('ak and sk required');
        }
        const endpointUrls = this.options.apiUrls;
        if (!endpointUrls?.length) {
            throw new ApiUrlsRequiredError('api urls is required');
        }
        const requestURL = endpointUrls.map(url =>
          `${new URL(url)}shares/`
        );
        const requestBody = {
            bucket: options.bucket,
            prefix: options.prefix,
            duration_seconds: options.durationSeconds,
            extract_code: options.extractCode,
            permission: options.permission,
        };
        const res = await this.httpClient.call<CreateShareResData>(
            requestURL,
            {
                fullUrl: true,
                appendAuthorization: true,
                method: 'POST',
                contentType: 'application/json',
                data: JSON.stringify(requestBody),
                dataType: 'json',
                apiName: 'createShare',
            }
        );
        if (res.status !== 200) {
            if (isErrorResult(res.data)) {
                throw new RequestBaseError(res.status, res.data.error);
            }
            throw new RequestBaseError(res.status);
        }
        return res.data as CreateShareResult;
    }

    async checkShare(options: CheckShareOptions): Promise<CheckShareResult> {
        const endpointUrls = this.options.apiUrls;
        if (!endpointUrls?.length) {
            throw new ApiUrlsRequiredError('api urls is required');
        }

        const requestURL = endpointUrls.map(url =>
            `${new URL(url)}shares/${options.shareId}/check?token=${options.shareToken}`
        );
        const res = await this.httpClient.call<CheckShareResData>(
            requestURL,
            {
                fullUrl: true,
                appendAuthorization: false,
                method: 'POST',
                apiName: 'checkShare',
            }
        );
        if (res.status !== 200) {
            if (isErrorResult(res.data)) {
                throw new RequestBaseError(res.status, res.data.error);
            }
            throw new RequestBaseError(res.status);
        }
    }

    async verifyShare(options: VerifyShareOptions): Promise<VerifyShareResult> {
        const endpointUrls = this.options.apiUrls;
        if (!endpointUrls?.length) {
            throw new ApiUrlsRequiredError('api urls is required');
        }

        const requestURL = endpointUrls.map(url =>
            `${new URL(url)}shares/${options.shareId}/verify?token=${options.shareToken}`
        );
        const requestBody = {
            extract_code: options.extractCode,
        };
        const res = await this.httpClient.call<VerifyShareResData>(
            requestURL,
            {
                fullUrl: true,
                appendAuthorization: false,
                method: 'POST',
                contentType: 'application/json',
                data: JSON.stringify(requestBody),
                dataType: 'json',
                apiName: 'verifyShare',
            },
        );
        if (res.status !== 200) {
            if (isErrorResult(res.data)) {
                throw new RequestBaseError(res.status, res.data.error);
            }
            throw new RequestBaseError(res.status);
        }
        return res.data as VerifyShareResult;
    }
}
