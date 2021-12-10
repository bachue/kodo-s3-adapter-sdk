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


export enum NatureLanguage {
    ChineseSimplified = 'zh-CN',
    EnglishUS = 'en-US',
    Japanese = 'ja-JP',
}

interface BaseSdkApiUplogEntry extends BaseUplogEntry {
    language: NatureLanguage,
}

interface OperationInfo {
    requests_count: number,
    /**
     * - bytes/sec for upload/download
     * - operations/sec for batch operation
     * - items/sec for list items
     */
    perceptive_speed: number,
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
        if (options.targetBucket && options.targetKey) {
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
        reqBodyLength: RespondedSdkApiUplogEntry['bytes_sent'],
        resBodyLength: RespondedSdkApiUplogEntry['bytes_received'],
        costDuration: RespondedSdkApiUplogEntry['total_elapsed_time'],

        perceptiveSpeed: number,
        requestsCount: number,
    }): RespondedSdkApiUplogEntry {
        return {
            ...this.baseSdkApiUplogEntry,
            ...getTransportInfo(options.reqBodyLength, options.resBodyLength, options.costDuration),
            perceptive_speed: options.perceptiveSpeed,
            requests_count: options.requestsCount,
        };
    }

    getErrorSdkApiUplogEntry(options: {
        errorType: ErrorType,
        errorDescription: string,

        perceptiveSpeed?: number,
        requestsCount?: number,
    }): ErrorSdkApiUplogEntry {
        return {
            ...this.baseSdkApiUplogEntry,
            ...getErrorInfo(options.errorType, options.errorDescription),
            perceptive_speed: options.perceptiveSpeed,
            requests_count: options.requestsCount,
        };
    }
}
