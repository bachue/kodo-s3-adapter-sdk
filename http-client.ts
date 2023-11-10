import http from 'http';
import fs from 'fs';
import { parse as parseURL } from 'url';
import { HttpClient2, HttpClientResponse, HttpMethod, RequestOptions2 } from 'urllib';
import Agent from 'agentkeepalive';
import { Readable, Transform } from 'stream';
import {
    ErrorType,
    GenRequestUplogEntry,
    getErrorTypeFromRequestError,
    getErrorTypeFromStatusCode,
    UplogBuffer,
} from './uplog';
import FormData from 'form-data';
import { RequestInfo, ResponseInfo } from './adapter';
import { generateAccessTokenV2, getXQiniuDate } from './kodo-auth';
import { generateReqId } from './req_id';

export interface HttpClientOptions {
    accessKey: string;
    secretKey?: string;
    protocol?: 'http' | 'https',
    timeout?: number | number[];
    userAgent?: string;
    retry?: number;
    retryDelay?: number;
    requestCallback?: (request: RequestInfo) => void;
    responseCallback?: (response: ResponseInfo) => void;

    disableQiniuTimestampSignature?: boolean,

    // for uplog
    apiType: 's3' | 'kodo',
    appName: string,
    appVersion: string,
    targetBucket?: string,
    targetKey?: string,
}

export interface RequestStats {
    sdkApiName: string,
    requestsCount: number,
    bytesTotalSent: number,
    bytesTotalReceived: number,
    errorType?: ErrorType;
    errorDescription?: string;
}

export interface URLRequestOptions {
    fullUrl?: boolean;
    appendAuthorization?: boolean;
    method: HttpMethod;
    path?: string;
    query?: Record<string, string | number | undefined>;
    data?: string | Buffer | Readable;
    dataType?: string;
    form?: FormData;
    streaming?: boolean,
    contentType?: string;
    headers?: { [headerName: string]: string; },
    abortSignal?: AbortSignal,
    stats?: RequestStats,

    // uplog
    apiName: string,
    targetBucket?: string,
    targetKey?: string,
}

export class HttpClient {
    static readonly userCanceledError = new Error('User Canceled');
    static readonly urlEmptyError = new Error('URL Empty');
    static readonly httpKeepaliveAgent = new Agent();
    static readonly httpsKeepaliveAgent = (() => {
        const httpsAgent = new Agent.HttpsAgent();
        // @ts-ignore
        httpsAgent.on('keylog', (line: string) => {
            if (process.env.SSLKEYLOGFILE) {
                fs.appendFileSync(process.env.SSLKEYLOGFILE, line, { mode: 0o600 });
            }
        });
        return httpsAgent;
    })();
    private static readonly httpClient: HttpClient2 = new HttpClient2({
        // urllib index.d.ts not support agent and httpsAgent
        // @ts-ignore
        agent: HttpClient.httpKeepaliveAgent,
        httpsAgent: HttpClient.httpsKeepaliveAgent,
    });

    constructor(
        private readonly clientOptions: HttpClientOptions,
        private readonly uplogBuffer: UplogBuffer) {
    }

    call<T = any>(urls: string[], options: URLRequestOptions): Promise<HttpClientResponse<T>> {
        // check aborted
        if (options.abortSignal?.aborted) {
            return Promise.reject(HttpClient.userCanceledError);
        }

        // check url
        const urlString: string | undefined = urls.shift();
        if (!urlString) {
            return Promise.reject(HttpClient.urlEmptyError);
        }
        let url: URL;
        if (options.fullUrl ?? false) {
            url = new URL(urlString);
        } else {
            url = this.makeUrl(urlString, options);
        }

        // process headers
        const headers: http.IncomingHttpHeaders = {
            'user-agent': this.clientOptions.userAgent,
        };
        if (options.appendAuthorization ?? true) {
            headers.authorization = this.makeAuthorization(url, options);
        }
        if (options.contentType) {
            headers['content-type'] = options.contentType;
        }
        if (options.headers) {
            for (const [headerName, headerValue] of Object.entries(options.headers)) {
                headers[headerName] = headerValue;
            }
        }
        headers['x-reqid'] = generateReqId({
            url: url.toString(),
            method: options.method,
            dataType: options.dataType,
            contentType: options.contentType,
            headers,
        });

        // need refactoring
        return new Promise((resolve, reject) => {
            // create request Options
            let requestInfo: RequestInfo | undefined;
            let multiJsonEncoded = false;

            if (options.dataType === 'multijson') {
                multiJsonEncoded = true;
                delete options.dataType;
            }

            const data = options.data ?? options.form?.getBuffer();
            const requestOption: RequestOptions2 = {
                method: options.method,
                dataType: options.dataType,
                contentType: options.contentType,
                headers,
                timeout: this.clientOptions.timeout,
                followRedirect: true,
                streaming: options.streaming,
                retry: this.clientOptions.retry,
                retryDelay: this.clientOptions.retryDelay,
                isRetry: (res) => {
                    if (options.abortSignal?.aborted) {
                        return false;
                    }
                    return this.isRetry(res);
                },
                beforeRequest: (info) => {
                    if (options.stats) {
                        options.stats.requestsCount += 1;
                    }
                    requestInfo = {
                        url: url.toString(),
                        method: info.method,
                        headers: info.headers,
                        data,
                    };
                    if (options.abortSignal) {
                        info.signal = options.abortSignal;
                    }
                    this.clientOptions.requestCallback?.(requestInfo);
                },
            };
            let callbackError: Error | undefined;
            let dataLength: number;
            if (data instanceof Readable) {
                dataLength = 0;
                const dataLenCounter = new Transform({
                    transform(chunk, _encoding, callback) {
                        dataLength += chunk.length;
                        callback(null, chunk);
                    }
                });
                requestOption.stream = data.pipe(dataLenCounter);
            } else {
                requestOption.data = data;
                dataLength = data?.length ?? 0;
            }

            const uplogMaker = new GenRequestUplogEntry(
                options.apiName,
                {
                    apiType: this.clientOptions.apiType,
                    httpVersion: '2',
                    method: options.method,
                    sdkName: this.clientOptions.appName,
                    sdkVersion: this.clientOptions.appVersion,
                    targetBucket: options.targetBucket,
                    targetKey: options.targetKey,
                    url: url,
                }
            );

            const beginTime = new Date().getTime();
            HttpClient.httpClient.request(url.toString(), requestOption).then((response) => {
                if (multiJsonEncoded && response.data && response.data instanceof Buffer) {
                    try {
                        response.data = response.data.toString().split(/\s*\n+\s*/)
                            .filter((line: string) => line.length)
                            .map((line: string) => JSON.parse(line));
                    } catch {
                        // ignore
                    }
                }
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
                    }

                    if (response.status >= 200 && response.status < 400) {
                        const requestUplogEntry = uplogMaker.getRequestUplogEntry({
                            reqId: response.headers['x-reqid']?.toString(),
                            costDuration: responseInfo.interval,
                            // @ts-ignore
                            remoteIp: response.res.remoteAddress,
                            bytesSent: dataLength,
                            // @ts-ignore
                            bytesReceived: response.res.size,
                            statusCode: response.status,
                        });
                        if (options.stats) {
                            options.stats.bytesTotalSent += dataLength;
                            // @ts-ignore
                            options.stats.bytesTotalReceived += response.res.size;
                        }
                        this.uplogBuffer.log(requestUplogEntry).finally(() => {
                            resolve(response);
                        });
                    } else if (response.data && response.data.error) {
                        const error = new Error(response.data.error);
                        responseInfo.error = error;
                        const errorRequestUplogEntry = uplogMaker.getErrorRequestUplogEntry({
                            errorType: getErrorTypeFromStatusCode(response.status),
                            errorDescription: error.message,

                            costDuration: responseInfo.interval,

                            statusCode: response.status,
                            // @ts-ignore
                            remoteIp: response.res.remoteAddress,
                            reqId: response.headers['x-reqid']?.toString(),
                        });
                        if (options.stats) {
                            options.stats.errorType = errorRequestUplogEntry.error_type;
                            options.stats.errorDescription = errorRequestUplogEntry.error_description;
                        }
                        this.uplogBuffer.log(errorRequestUplogEntry).finally(() => {
                            if (urls.length > 0) {
                                this.call(urls, options).then(resolve, reject);
                            } else {
                                reject(error);
                            }
                        });
                    } else {
                        let error: Error | undefined;
                        if (response.data) {
                            try {
                                const data = JSON.parse(response.data);
                                if (data.error) {
                                    error = new Error(data.error);
                                }
                            } catch {
                                // Ignore
                            }
                        }
                        error ||= new Error(response.res.statusMessage);
                        responseInfo.error = error;
                        const errorRequestUplogEntry = uplogMaker.getErrorRequestUplogEntry({
                            errorType: getErrorTypeFromStatusCode(response.status),
                            errorDescription: error.message,

                            costDuration: responseInfo.interval,

                            statusCode: response.status,
                            // @ts-ignore
                            remoteIp: response.res.remoteAddress,
                            reqId: response.headers['x-reqid']?.toString(),
                        });
                        if (options.stats) {
                            options.stats.errorType = errorRequestUplogEntry.error_type;
                            options.stats.errorDescription = errorRequestUplogEntry.error_description;
                        }
                        this.uplogBuffer.log(errorRequestUplogEntry).finally(() => {
                            if (urls.length > 0) {
                                this.call(urls, options).then(resolve, reject);
                            } else {
                                reject(error);
                            }
                        });
                    }
                } finally {
                    if (this.clientOptions.responseCallback) {
                        this.clientOptions.responseCallback(responseInfo);
                    }
                }
            }).catch((err) => {
                const responseInfo: ResponseInfo = {
                    request: requestInfo!,
                    interval: new Date().getTime() - beginTime,
                    error: err,
                };
                if (this.clientOptions.responseCallback) {
                    this.clientOptions.responseCallback(responseInfo);
                }

                const errorRequestUplogEntry = uplogMaker.getErrorRequestUplogEntry({
                    errorType: getErrorTypeFromRequestError(err),
                    errorDescription: err.message,

                    costDuration: responseInfo.interval,
                });
                if (options.stats) {
                    options.stats.errorType = errorRequestUplogEntry.error_type;
                    options.stats.errorDescription = errorRequestUplogEntry.error_description;
                }
                this.uplogBuffer.log(errorRequestUplogEntry).finally(() => {
                    if (callbackError) {
                        return;
                    } else if (urls.length > 0) {
                        this.call(urls, options).then(resolve, reject);
                    } else {
                        reject(err);
                    }
                });
            });
        });
    }

    private makeUrl(base: string, options: URLRequestOptions): URL {
        // the `parseURL` is consistent with the `urllib` library
        const url = new URL(parseURL(base).href);
        if (options.path) {
            url.pathname = options.path;
        }

        const protocol = this.clientOptions.protocol;
        if (protocol) {
            switch (protocol) {
                case 'http':
                    url.protocol = 'http:';
                    break;
                case 'https':
                    url.protocol = 'https:';
                    break;
            }
        }
        if (options.query) {
            url.search = Object.entries(options.query).map(([name, value]) => {
                // DO NOT use `!value` by '', 0, ... is valid.
                if (value !== undefined) {
                    return [encodeURIComponent(name), encodeURIComponent(value)].join('=');
                } else {
                    return name;
                }
            }).join('&');
        }
        return url;
    }

    private makeAuthorization(url: URL, options: URLRequestOptions): string {
        let data: string | undefined;
        if (options.data) {
            if (options.dataType === 'json') {
                data = JSON.stringify(options.data);
            }
            data = options.data.toString();
        }

        if (!this.clientOptions.disableQiniuTimestampSignature) {
            options.headers = options.headers ?? {};
            options.headers['X-Qiniu-Date'] = getXQiniuDate();
        }

        return generateAccessTokenV2(
            this.clientOptions.accessKey,
            this.clientOptions.secretKey!,
            url.toString(),
            options.method ?? 'GET',
            options.contentType,
            options.headers,
            data,
        );
    }

    private isRetry(response: HttpClientResponse<any>): boolean {
        const dontRetryStatusCodes: number[] = [501, 579, 599, 608, 612, 614, 616,
            618, 630, 631, 632, 640, 701];
        return !response.headers['x-reqid'] ||
            response.status >= 500 && !dontRetryStatusCodes.find((status) => status === response.status);
    }
}
