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
