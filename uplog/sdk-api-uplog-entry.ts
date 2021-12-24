import {
    BaseUplogEntry,
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
    TransportInfo
} from './fields';

export type NatureLanguage = 'zh-CN' | 'en-US' | 'ja-JP';

interface BaseSdkApiUplogEntry extends BaseUplogEntry {
    language: NatureLanguage,
}

interface OperationInfo {
    requests_count: number,
    /**
     * - bytes/sec for upload/download
     * - TODO: operations/sec for batch operation
     * - TODO: items/sec for list items
     */
    perceptive_speed?: number,
}

export type RespondedSdkApiUplogEntry = BaseSdkApiUplogEntry & TransportInfo & OperationInfo;

export type ErrorSdkApiUplogEntry = BaseSdkApiUplogEntry & Partial<OperationInfo> & ErrorInfo;

export class GenSdkApiUplogEntry {
    private readonly apiName: string;
    private readonly language: BaseSdkApiUplogEntry['language'];

    // before send can get
    private readonly systemInfo: SystemInfo;
    private readonly clientInfo: ClientInfo;
    private readonly operateTarget?: OperateTarget;

    constructor(
        apiName: string,
        options: {
            language: NatureLanguage,

            sdkName: string,
            sdkVersion: string,

            targetBucket?: string,
            targetKey?: string,
        },
    ) {
        this.apiName = apiName;
        this.language = options.language;

        this.systemInfo = getSystemInfo();
        this.clientInfo = getClientInfo(options.sdkName, options.sdkVersion);
        if (options.targetBucket) {
            this.operateTarget = getOperateTarget(options.targetBucket, options.targetKey);
        }

        return this;
    }

    private get baseSdkApiUplogEntry(): BaseSdkApiUplogEntry {
        return {
            api_name: this.apiName,
            log_type: LogType.SdkApi,
            language: this.language,
            ...this.systemInfo,
            ...this.clientInfo,
            ...this.operateTarget,
        };
    }

    getSdkApiUplogEntry(options: {
        bytesSent: RespondedSdkApiUplogEntry['bytes_sent'],
        bytesReceived: RespondedSdkApiUplogEntry['bytes_received'],
        costDuration: RespondedSdkApiUplogEntry['total_elapsed_time'],

        requestsCount: number,
    },): RespondedSdkApiUplogEntry {
        return {
            ...this.baseSdkApiUplogEntry,
            ...getTransportInfo(options.bytesSent, options.bytesReceived, options.costDuration),
            perceptive_speed: calculatePerceptiveSpeed(this.apiName, options),
            requests_count: options.requestsCount,
        };
    }

    getErrorSdkApiUplogEntry(options: {
        errorType: ErrorType,
        errorDescription: string,

        requestsCount?: number,
    }): ErrorSdkApiUplogEntry {
        return {
            ...this.baseSdkApiUplogEntry,
            ...getErrorInfo(options.errorType, options.errorDescription),
            requests_count: options.requestsCount,
        };
    }
}

function calculatePerceptiveSpeed(
    sdkApiName: string,
    options: {
        costDuration: number, // ms
        bytesSent: number,
        bytesReceived: number,
        requestsCount: number,
    },
): number | undefined {
    if (sdkApiName === 'downloadFile') {
        return Math.trunc(options.bytesReceived / options.costDuration);
    }
    if (sdkApiName === 'uploadFile') {
        return Math.trunc(options.bytesSent / options.costDuration);
    }
    return;
}
