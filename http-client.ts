import http from 'http';
import { HttpClient2, HttpClientResponse, HttpMethod, RequestOptions2 } from 'urllib';
import { Throttle } from 'stream-throttle';
import {
    ErrorType,
    GenRequestUplogEntry,
    getErrorTypeFromRequestError,
    getErrorTypeFromStatusCode,
    UplogBuffer,
} from './uplog';
import FormData from 'form-data';
import { ReadableStreamBuffer } from 'stream-buffers';
import { RequestInfo, ResponseInfo } from './adapter';
import { generateAccessTokenV2 } from './kodo-auth';
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
    reqBodyTotalLength: number,
    resBodyTotalLength: number,
    errorType?: ErrorType;
    errorDescription?: string;
}

export interface URLRequestOptions {
    fullUrl?: boolean;
    appendAuthorization?: boolean;
    method: HttpMethod;
    path?: string;
    query?: URLSearchParams;
    data?: string | Buffer;
    dataType?: string;
    form?: FormData;
    streaming?: boolean,
    contentType?: string;
    headers?: { [headerName: string]: string; },
    uploadProgress?: (uploaded: number, total: number) => void,
    uploadThrottle?: Throttle,
    stats?: RequestStats,

    // uplog
    apiName: string,
}

export class HttpClient {
    private static readonly httpClient: HttpClient2 = new HttpClient2();

    constructor(
        private readonly clientOptions: HttpClientOptions,
        private readonly uplogBuffer: UplogBuffer) {
    }

    call<T = any>(urls: string[], options: URLRequestOptions): Promise<HttpClientResponse<T>> {
        // check url
        const urlString: string | undefined = urls.shift();
        if (!urlString) {
            return Promise.reject(new Error('urls is empty'));
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
                isRetry: this.isRetry,
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
                    this.clientOptions.requestCallback?.(requestInfo);
                },
            };
            let callbackError: Error | undefined;
            if (data) {
                if (options.uploadProgress) {
                    const stream = new ReadableStreamBuffer({ initialSize: data.length, chunkSize: 1 << 20 });
                    stream.put(data);
                    stream.stop();
                    let uploaded = 0;
                    const total = data.length;
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

            const uplogMaker = new GenRequestUplogEntry(
                options.apiName,
                {
                    apiType: this.clientOptions.apiType,
                    httpVersion: '2',
                    method: options.method,
                    sdkName: this.clientOptions.appName,
                    sdkVersion: this.clientOptions.appVersion,
                    targetBucket: this.clientOptions.targetBucket,
                    targetKey: this.clientOptions.targetKey,
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
                    } else if (response.status >= 200 && response.status < 400) {
                        const requestUplogEntry = uplogMaker.getRequestUplogEntry({
                            reqId: response.headers['x-reqid']?.toString(),
                            costDuration: responseInfo.interval,
                            remoteIp: response.res.socket.remoteAddress!,
                            reqBodyLength: data?.length ?? 0,
                            resBodyLength: response.data.length ?? 0,
                            statusCode: 0
                        });

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
                            remoteIp: response.res.socket.remoteAddress,
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
                            remoteIp: response.res.socket.remoteAddress,
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
        const url = new URL(base);
        if (options.path) {
            url.pathname = options.path;
        }

        const protocol = this.clientOptions.protocol;
        if (protocol) {
            switch (protocol) {
                case 'http':
                    url.protocol = 'http';
                    break;
                case 'https':
                    url.protocol = 'https';
                    break;
            }
        }
        if (options.query) {
            options.query.forEach((value, name) => {
                url.searchParams.append(name, value);
            });
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

        return generateAccessTokenV2(
            this.clientOptions.accessKey, this.clientOptions.secretKey!, url.toString(),
            options.method ?? 'GET', options.contentType, data);
    }

    private isRetry(response: HttpClientResponse<any>): boolean {
        const dontRetryStatusCodes: number[] = [501, 579, 599, 608, 612, 614, 616,
            618, 630, 631, 632, 640, 701];
        return !response.headers['x-reqid'] ||
            response.status >= 500 && !dontRetryStatusCodes.find((status) => status === response.status);
    }
}
