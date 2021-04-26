import os from 'os';
import fs from 'fs';
import path from 'path';
import pkg from './package.json';
import { getCacheFolder } from 'platform-folders';
import lockFile from 'lockfile';

export interface UplogOption {
    appName?: string;
    appVersion?: string;
    bufferSize?: number;
    onBufferFull?: (buffer: Buffer) => Promise<void>,
}

export interface UplogEntry {
    log_type: LogType;
    os_name?: string;
    os_version?: string;
    sdk_name?: string;
    sdk_version?: string;
    http_client?: string;
    http_client_version?: string;
    up_time?: number;
}

export interface RequestUplogEntry extends UplogEntry {
    status_code?: number;
    req_id?: string;
    host: string;
    port: number;
    method: string;
    path: string;
    remote_ip?: string;
    total_elapsed_time: number;
    bytes_sent?: number;
    error_type?: ErrorType;
    error_description?: string;
}

export interface SdkApiUplogEntry extends UplogEntry {
    api_name: string;
    total_elapsed_time: number;
    requests_count: number;
    error_type?: ErrorType;
    error_description?: string;
}

export enum LogType {
    Request = 'request',
    SdkApi = 'sdkapi',
}

export enum ErrorType {
    UnknownError = 'unknown_error',
    NetworkError = 'network_error',
    Timeout = 'timeout',
    UnknownHost = 'unknown_host',
    CannotConnectToHost = 'cannot_connect_to_host',
    TransmissionError = 'transmission_error',
    ProxyError = 'proxy_error',
    SslError = 'ssl_error',
    ResponseError = 'response_error',
    ParseError = 'parse_error',
    MaliciousResponse = 'malicious_response',
    UserCanceled = 'user_canceled',
    BadRequest = 'bad_request',
    UnexpectedSyscallError = 'unexpected_syscall_error',
}

export const UplogBufferFilePath = path.join(getCacheFolder(), '.kodo-s3-adapter-sdk', 'uplog-buffer');
export const UplogBufferFileLockPath = path.join(getCacheFolder(), '.kodo-s3-adapter-sdk', 'uplog-buffer.lock');

export class UplogBuffer {
    private static uploadBufferedEntries: Array<string> = [];
    private static uploadBufferFd: number | undefined = undefined;

    constructor(private readonly option: UplogOption) {
        if (!UplogBuffer.uploadBufferFd) {
            fs.mkdirSync(path.dirname(UplogBufferFilePath), { recursive: true, mode: 0o700 });
            UplogBuffer.uploadBufferFd = fs.openSync(UplogBufferFilePath, 'a', 0o600);
        }
    }

    log(entry: UplogEntry): Promise<void> {
        UplogBuffer.uploadBufferedEntries.push(this.convertUplogEntryToJSON(entry) + "\n");
        return new Promise((resolve, reject) => {
            this.flushBufferToLogFile().then((fileSize) => {
                if (fileSize && fileSize >= (this.option.bufferSize ?? 1 << 20)) {
                    this.exportLogs().then(resolve, reject);
                } else {
                    resolve();
                }
            }, reject);
        });
    }

    private flushBufferToLogFile(): Promise<number | undefined> {
        return new Promise((resolve, reject) => {
            lockFile.lock(UplogBufferFileLockPath, {}, (err) => {
                if (err) {
                    if ((err as any).code === 'EEXIST') {
                        resolve(undefined);
                    } else {
                        console.warn("locked fail:", err);
                        reject(err);
                    }
                } else {
                    const uploadBufferedEntries = UplogBuffer.uploadBufferedEntries;
                    UplogBuffer.uploadBufferedEntries = [];
                    const writePromise = uploadBufferedEntries.reduce((writePromise, data) => {
                        return writePromise.then(() => {
                            return new Promise((resolve, reject) => {
                                fs.write(UplogBuffer.uploadBufferFd!, data, (err) => {
                                    if (err) {
                                        reject(err);
                                    } else {
                                        resolve();
                                    }
                                });
                            });
                        }, reject);
                    }, Promise.resolve());

                    writePromise.then(() => {
                        fs.fstat(UplogBuffer.uploadBufferFd!, (err, stats) => {
                            if (err) {
                                lockFile.unlock(UplogBufferFileLockPath, () => { reject(err); });
                                return;
                            }
                            lockFile.unlock(UplogBufferFileLockPath, (err) => {
                                if (err) {
                                    reject(err);
                                } else {
                                    resolve(stats.size);
                                }
                            });
                        });
                    }, (err) => {
                        lockFile.unlock(UplogBufferFileLockPath, () => { reject(err); });
                    });
                }
            });
        });
    }

    private exportLogs(): Promise<void> {
        if (!this.option.onBufferFull) {
            return Promise.resolve();
        }
        return new Promise((resolve, reject) => {
            lockFile.lock(UplogBufferFileLockPath, {}, (err) => {
                if (err) {
                    console.warn("locked fail:", err);
                    reject(err);
                } else {
                    fs.readFile(UplogBufferFilePath, (err, buffer) => {
                        if (err) {
                            lockFile.unlock(UplogBufferFileLockPath, () => { reject(err); });
                            return;
                        }
                        if (this.option.onBufferFull) {
                            this.option.onBufferFull(buffer).then(() => {
                                fs.truncate(UplogBufferFilePath, (err) => {
                                    if (err) {
                                        lockFile.unlock(UplogBufferFileLockPath, () => { reject(err); });
                                    } else {
                                        lockFile.unlock(UplogBufferFileLockPath, (err) => {
                                            if (err) {
                                                reject(err);
                                            } else {
                                                resolve();
                                            }
                                        });
                                    }
                                })
                            }, (err) => {
                                lockFile.unlock(UplogBufferFileLockPath, () => { reject(err); });
                            });
                        } else {
                            lockFile.unlock(UplogBufferFileLockPath, (err) => {
                                if (err) {
                                    reject(err);
                                } else {
                                    resolve();
                                }
                            });
                        }
                    });
                }
            });
        });
    }

    private convertUplogEntryToJSON(entry: UplogEntry) {
        entry.os_name = os.platform();
        entry.os_version = os.release();
        entry.sdk_name = this.option.appName;
        entry.sdk_version = this.option.appVersion;
        entry.http_client = `kodo-s3-adapter-sdk`;
        entry.http_client_version = pkg.version;
        entry.up_time = Math.trunc(Date.now() / 1000);
        return JSON.stringify(entry);
    }
}

export const getErrorTypeFromStatusCode = (statusCode: number): ErrorType => {
    if (statusCode > 399 && statusCode < 500 ||
        statusCode == 573 || statusCode == 579 ||
        statusCode == 608 || statusCode == 612 ||
        statusCode == 614 || statusCode == 630 ||
        statusCode == 631 || statusCode == 701) {
        return ErrorType.BadRequest;
    } else {
        return ErrorType.ResponseError;
    }
}

export const getErrorTypeFromRequestError = (err: any): ErrorType => {
    switch (err.code) {
        case 'ENOTFOUND':
            return ErrorType.UnknownHost;
        case 'ECONNREFUSED':
            return ErrorType.CannotConnectToHost;
        case 'ECONNRESET':
            return ErrorType.CannotConnectToHost;
        case 'EMFILE':
            return ErrorType.UnexpectedSyscallError;
        case 'EACCES':
            return ErrorType.UnexpectedSyscallError;
        case 'ETIMEDOUT':
            return ErrorType.Timeout;
        case 'EPIPE':
            return ErrorType.TransmissionError;
        case 'EPROTO':
            return ErrorType.NetworkError;
        case 'UNABLE_TO_VERIFY_LEAF_SIGNATURE':
            return ErrorType.SslError;
    }
    if (err.name && err.name.endsWith('TimeoutError')) {
        return ErrorType.Timeout;
    }
    switch (err.name) {
        case 'JSONResponseFormatError':
            return ErrorType.ParseError;
        default:
            return ErrorType.UnknownError;
    }
}

export const getErrorTypeFromS3Error = (err: any): ErrorType => {
    switch (err.code) {
        case 'TimeoutError':
            return ErrorType.Timeout;
        case 'NetworkingError':
            return ErrorType.NetworkError;
        case 'UnknownEndpoint':
            return ErrorType.UnknownHost;
        case 'XMLParserError':
            return ErrorType.ParseError;
        case 'CredentialsError':
            return ErrorType.BadRequest;
        case 'InvalidHeader':
            return ErrorType.BadRequest;
        case 'InvalidParameter':
            return ErrorType.BadRequest;
        case 'InvalidDigest':
            return ErrorType.BadRequest;
        case 'RequestAbortedError':
            return ErrorType.UserCanceled;
        default:
            return ErrorType.UnknownError;
    }
}
