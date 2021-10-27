import os from 'os';
import fs from 'fs';
import path from 'path';
import pkg from './package.json';
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

export const UplogBufferFilePath = path.join(os.homedir(), '.kodo-s3-adapter-sdk', 'uplog-buffer');
export const UplogBufferFileLockPath = path.join(os.homedir(), '.kodo-s3-adapter-sdk', 'uplog-buffer.lock');

export class UplogBuffer {
    private static uploadBufferedEntries: string[] = [];
    private static uploadBufferFd: number | undefined = undefined;

    constructor(private readonly option: UplogOption) {
        if (!UplogBuffer.uploadBufferFd) {
            const dirPath = path.dirname(UplogBufferFilePath);
            if (!fs.existsSync(dirPath)) {
                fs.mkdirSync(dirPath, { recursive: true, mode: 0o700 });
            }
            UplogBuffer.uploadBufferFd = fs.openSync(UplogBufferFilePath, 'a', 0o600);
        }
    }

    async log(entry: UplogEntry): Promise<void> {
        UplogBuffer.uploadBufferedEntries.push(this.convertUplogEntryToJSON(entry) + '\n');
        const fileSize = await this.flushBufferToLogFile();
        if (fileSize && fileSize >= (this.option.bufferSize ?? 1 << 20)) {
            await this.exportLogs();
        }
    }

    private async flushBufferToLogFile(): Promise<number | undefined> {
        try {
            await new Promise<any>((resolve, reject) => {
                lockFile.lock(UplogBufferFileLockPath, this.lockOptions(), err => !err ? resolve() : reject(err));
            });
        } catch (err) {
            if (err?.code === 'EEXIST') {
                return;
            }
            console.warn('locked fail:', err);
            throw err;
        }

        const uploadBufferedEntries = UplogBuffer.uploadBufferedEntries;
        UplogBuffer.uploadBufferedEntries = [];

        let stats: fs.Stats;
        try {
            for (const data of uploadBufferedEntries) {
                await new Promise((resolve, reject) => {
                    fs.write(UplogBuffer.uploadBufferFd!, data, err => !err ? resolve() : reject(err));
                });
            }
            stats = await new Promise<fs.Stats>((resolve, reject) => {
                fs.fstat(UplogBuffer.uploadBufferFd!, (err, stats) => {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve(stats);
                });
            });
        } catch (err) {
            await new Promise(resolve => {
                lockFile.unlock(UplogBufferFileLockPath, resolve);
            });
            throw err;
        }

        await new Promise((resolve, reject) => {
            lockFile.unlock(UplogBufferFileLockPath, (err) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve();
            });
        });
        return stats.size;
    }

    private async exportLogs(): Promise<void> {
        if (!this.option?.onBufferFull) {
            return;
        }
        try {
            await new Promise<any>((resolve, reject) => {
                lockFile.lock(UplogBufferFileLockPath, this.lockOptions(), err => !err ? resolve() : reject(err));
            });
        } catch (err) {
            if (err?.code === 'EEXIST') {
                return;
            }
            console.warn('locked fail:', err);
            throw err;
        }

        try {
            const buffer = await new Promise<Buffer>((resolve, reject) => {
                fs.readFile(UplogBufferFilePath, (err, buffer) => {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve(buffer);
                });
            });
            if (this.option.onBufferFull) {
                await this.option.onBufferFull(buffer);
                await new Promise((resolve, reject) => {
                    fs.truncate(UplogBufferFilePath, (err) => {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve();
                    });
                });
            }
        } catch (err) {
            await new Promise(resolve => {
                lockFile.unlock(UplogBufferFileLockPath, resolve);
            });
            throw err;
        }
        await new Promise((resolve, reject) => {
            lockFile.unlock(UplogBufferFileLockPath, (err) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve();
            });
        });
    }

    private convertUplogEntryToJSON(entry: UplogEntry) {
        entry.os_name = os.platform();
        entry.os_version = os.release();
        entry.sdk_name = this.option.appName;
        entry.sdk_version = this.option.appVersion;
        entry.http_client = 'kodo-s3-adapter-sdk';
        entry.http_client_version = pkg.version;
        entry.up_time = Math.trunc(Date.now() / 1000);
        return JSON.stringify(entry);
    }

    private lockOptions(): lockFile.Options {
        return { retries: 10, retryWait: 100 };
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
};

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
};

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
};
