import os from 'os';
import packageJson from '../package.json';

export interface SystemInfo {
    os_name: string,
    os_version: string,
    os_arch: string,
    /**
     * local timestamp(second) when call a sdk api or send a request
     */
    up_time: number,
}

export function getSystemInfo(): SystemInfo {
    return {
        os_name: os.platform(),
        os_version: os.release(),
        os_arch: process.arch,
        up_time: Math.trunc(Date.now() / 1000),
    };
}

export interface ClientInfo {
    sdk_name: string,
    sdk_version: string,
    http_client: 'kodo-s3-adapter-sdk',
    http_client_version: string,
}

export function getClientInfo(sdkName: string, sdkVersion: string): ClientInfo {
    return {
        sdk_name: sdkName,
        sdk_version: sdkVersion,
        http_client: 'kodo-s3-adapter-sdk',
        http_client_version: packageJson.version,
    };
}

export interface OperateTarget {
    target_bucket: string,
    target_key: string,
}

export function getOperateTarget(targetBucket: string, targetKey: string): OperateTarget {
    return {
        target_bucket: targetBucket,
        target_key: targetKey,
    };
}

export interface TransportInfo {
    total_elapsed_time: number,
    bytes_sent: number,
    bytes_received: number,
}

export function getTransportInfo(reqBodyLength: number, resBodyLength: number, costDuration: number): TransportInfo {
    return {
        total_elapsed_time: costDuration,
        bytes_sent: reqBodyLength,
        bytes_received: resBodyLength,
    };
}

export enum ErrorType {
    UnknownError = 'unknown_error',
    /** unknown network error */
    NetworkError = 'network_error',
    Timeout = 'timeout',
    /** DNS error */
    UnknownHost = 'unknown_host',
    CannotConnectToHost = 'cannot_connect_to_host',
    TransmissionError = 'transmission_error',
    ProxyError = 'proxy_error',
    SslError = 'ssl_error',
    /**
     * received but response code not 200,
     * even 3xx.
     */
    ResponseError = 'response_error',
    /** can't parse response */
    ParseError = 'parse_error',
    MaliciousResponse = 'malicious_response',
    UserCanceled = 'user_canceled',
    /**
     * can't retry send, because client config error.
     * for example:
     * - 4xx
     * - upload token error
     * - target bucket not exists
     * - file duplicated
     * - region error
     * - amount not enough
     * - ...
     */
    BadRequest = 'bad_request',
    /**
     * something not work with system.
     * usually due to not enough hardware sources.
     * for example:
     * - memory allocate error
     * - can't create thread
     * - ...
     */
    UnexpectedSyscallError = 'unexpected_syscall_error',
}

export interface ErrorInfo {
    error_type: ErrorType,
    error_description: string,
}

export function getErrorInfo(errorType: ErrorType, errorDescription: string) {
    return {
        error_type: errorType,
        error_description: errorDescription,
    };
}

type BaseInfo = SystemInfo
    & ClientInfo
    & Partial<TransportInfo>
    & Partial<OperateTarget>
    & Partial<ErrorInfo>;

export enum LogType {
    Request = 'request',
    SdkApi = 'sdkapi',
}

export interface BaseUplogEntry extends BaseInfo {
    log_type: LogType,
    api_name: string,
}

// enum NatureLanguage {
//     ChineseSimplified = 'zh-CN',
//     EnglishUS = 'en-US',
//     Japanese = 'ja-JP',
// }

// export interface SdkApiUplogEntry extends BaseUplogEntry {
//     requests_count: number,
//     /**
//      * - bytes/sec for upload/download
//      * - operations/sec for batch operation
//      * - items/sec for list items
//      */
//     perceptive_speed: number,
//     language: NatureLanguage,
// }
