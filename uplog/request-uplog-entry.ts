import {
    ClientInfo,
    ErrorInfo,
    ErrorType,
    getClientInfo,
    getErrorInfo,
    getOperateTarget,
    getSystemInfo,
    getTransportInfo,
    LogType,
    OperateTarget,
    SystemInfo,
    BaseUplogEntry,
    TransportInfo,
} from './fields';

interface BaseRequestUplogEntry extends BaseUplogEntry {
    api_type: 'kodo' | 's3',
    http_version: '1.0' | '1.1' | '2' | '3',
    method: string,
    host: string,
    port: number,
    path: string,
}

interface RespondInfo {
    status_code: number,
    remote_ip: string,
    req_id?: string,
}

export type RequestUplogEntry = BaseRequestUplogEntry & RespondInfo & TransportInfo;

export type ErrorRequestUplogEntry = BaseRequestUplogEntry & Partial<RespondInfo> & ErrorInfo & Pick<TransportInfo, 'total_elapsed_time'>;

function parsePort(url: URL): number {
    const port: number = parseInt(url.port);
    if (port) {
        return port;
    }
    switch (url.protocol) {
        case 'http':
            return 80;
        case 'https':
            return 80;
        default:
            return 0;
    }
}

function getLogInfoFromUrl(
    url: URL,
): Pick<BaseRequestUplogEntry, 'host' | 'port' | 'path'> {
    return {
        host: url.hostname,
        port: parsePort(url),
        path: url.pathname,
    };
}

export class GenRequestUplogEntry {
    apiName: string;
    apiType: BaseRequestUplogEntry['api_type'];
    httpVersion: BaseRequestUplogEntry['http_version'];
    method: string;
    url: URL;
    logType: LogType = LogType.Request;

    // before send can get
    systemInfo: SystemInfo;
    clientInfo: ClientInfo;
    operateTarget?: OperateTarget;

    // after request can get
    transportInfo?: TransportInfo;

    constructor(
        apiName: string,
        options: {
            apiType: BaseRequestUplogEntry['api_type'],
            httpVersion: BaseRequestUplogEntry['http_version'];

            method: string,
            url: URL,

            sdkName: string,
            sdkVersion: string,

            // lihs: can these two fields be parsed with url?
            targetBucket?: string,
            targetKey?: string,
        },
    ) {
        this.apiName = apiName;

        this.apiType = options.apiType;
        this.httpVersion = options.httpVersion;
        this.method = options.method;
        this.url = options.url;

        this.systemInfo = getSystemInfo();
        this.clientInfo = getClientInfo(options.sdkName, options.sdkVersion);
        if (options.targetBucket && options.targetKey) {
            this.operateTarget = getOperateTarget(options.targetBucket, options.targetKey);
        }
        return this;
    }

    private get baseRequestUplogEntry(): BaseRequestUplogEntry {
        return {
            api_name: this.apiName,
            api_type: this.apiType,
            log_type: this.logType,
            http_version: this.httpVersion,
            method: this.method,
            ...this.systemInfo,
            ...this.clientInfo,
            ...this.operateTarget,
            ...getLogInfoFromUrl(this.url),
        };
    }

    getRequestUplogEntry(options: {
        reqBodyLength: RequestUplogEntry['bytes_sent'],
        resBodyLength: RequestUplogEntry['bytes_received'],
        costDuration: RequestUplogEntry['total_elapsed_time'],

        statusCode: RequestUplogEntry['status_code'],
        remoteIp: RequestUplogEntry['remote_ip'],
        reqId: RequestUplogEntry['req_id'],
    }): RequestUplogEntry {
        return {
            ...this.baseRequestUplogEntry,
            ...getTransportInfo(options.reqBodyLength, options.resBodyLength, options.costDuration),
            status_code: options.statusCode,
            remote_ip: options.remoteIp,
            req_id: options.reqId,
        };
    }

    getErrorRequestUplogEntry(options: {
        errorType: ErrorType,
        errorDescription: string,

        costDuration: ErrorRequestUplogEntry['total_elapsed_time'],

        statusCode?: ErrorRequestUplogEntry['status_code'],
        remoteIp?: ErrorRequestUplogEntry['remote_ip'],
        reqId?: ErrorRequestUplogEntry['req_id'],
    }): ErrorRequestUplogEntry {
        return {
            ...this.baseRequestUplogEntry,
            ...getErrorInfo(options.errorType, options.errorDescription),
            total_elapsed_time: options.costDuration,
        };
    }
}
