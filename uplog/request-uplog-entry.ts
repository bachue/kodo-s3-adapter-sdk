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

export type RespondedRequestUplogEntry = BaseRequestUplogEntry & TransportInfo & RespondInfo;

export type ErrorRequestUplogEntry = BaseRequestUplogEntry & Partial<RespondInfo> & ErrorInfo & Pick<TransportInfo, 'total_elapsed_time'>;

function parsePort(url: URL): number {
    const port: number = parseInt(url.port);
    if (port) {
        return port;
    }
    switch (url.protocol) {
        case 'http:':
            return 80;
        case 'https:':
            return 443;
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
    private readonly apiName: BaseRequestUplogEntry['api_name'];
    private readonly apiType: BaseRequestUplogEntry['api_type'];
    private readonly httpVersion: BaseRequestUplogEntry['http_version'];
    private readonly method: BaseRequestUplogEntry['method'];
    private readonly url: URL;
    private readonly logType: LogType = LogType.Request;

    // before send can get
    private readonly systemInfo: SystemInfo;
    private readonly clientInfo: ClientInfo;
    private readonly operateTarget?: OperateTarget;

    constructor(
        apiName: string,
        options: {
            apiType: BaseRequestUplogEntry['api_type'],
            httpVersion: BaseRequestUplogEntry['http_version'];

            method: string,
            url: URL,

            sdkName: string,
            sdkVersion: string,

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
        if (options.targetBucket) {
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
        bytesSent: RespondedRequestUplogEntry['bytes_sent'],
        bytesReceived: RespondedRequestUplogEntry['bytes_received'],
        costDuration: RespondedRequestUplogEntry['total_elapsed_time'],

        statusCode: RespondedRequestUplogEntry['status_code'],
        remoteIp: RespondedRequestUplogEntry['remote_ip'],
        reqId: RespondedRequestUplogEntry['req_id'],
    }): RespondedRequestUplogEntry {
        return {
            ...this.baseRequestUplogEntry,
            ...getTransportInfo(options.bytesSent, options.bytesReceived, options.costDuration),
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
