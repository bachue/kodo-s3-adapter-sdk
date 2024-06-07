import os from 'os';

import pkg from './package.json';
import { HttpClient } from './http-client';
import { RequestInfo, ResponseInfo } from './adapter';
import { UplogBuffer } from './uplog';
import { DEFAULT_API_URL, Region } from './region';

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
    ucUrl?: string,
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

interface GetEndpointsOptions {
    bucketName: string,
}

export class AuthRequiredError extends Error {
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
        const protocol = (options.apiUrls?.[0] || options.ucUrl || 'https').startsWith('https') ? 'https' : 'http';
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
        const endpointUrls = await this.getEndpoints({
            bucketName: options.bucket,
        });

        const requestURL = endpointUrls.map(url => `${url}shares/`);
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
        const endpointUrls = await this.getEndpoints();

        const requestURL = endpointUrls.map(url =>
            `${url}shares/${options.shareId}/check?token=${options.shareToken}`
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
        const endpointUrls = await this.getEndpoints();

        const requestURL = endpointUrls.map(url =>
            `${url}shares/${options.shareId}/verify?token=${options.shareToken}`
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

    private async getEndpoints(options?: GetEndpointsOptions): Promise<string[]> {
        if (this.options.apiUrls?.length) {
            return this.options.apiUrls.map(u => new URL(u).toString());
        }

        if (!this.options.ak || !options?.bucketName) {
            // throw new AuthRequiredError('ak and bucket is required when doesn\'t provide api urls');
            return [DEFAULT_API_URL].map(u => new URL(u).toString());
        }

        const region = await Region.query({
            bucketName: options.bucketName,
            accessKey: this.options.ak,
            ucUrl: this.options.ucUrl,
            timeout: this.options.timeout,
            retry: this.options.retry,
            retryDelay: this.options.retryDelay,
            requestCallback: this.options.requestCallback,
            responseCallback: this.options.responseCallback,
            appName: this.options.appName,
            appVersion: this.options.appVersion,
            uplogBufferSize: -1,
        });

        return region.apiUrls;
    }
}
