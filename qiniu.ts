import { Region } from './region';
import { Adapter, RequestInfo, ResponseInfo } from './adapter';
import { Kodo } from './kodo';
import { S3 } from './s3';

export const KODO_MODE: string = 'kodo';
export const S3_MODE: string = 's3';

interface Callbacks {
    requestCallback?: (request: RequestInfo) => void;
    responseCallback?: (response: ResponseInfo) => void;
}

export class Qiniu {
    private static readonly ADAPTERS: { [key: string]: typeof Adapter; } = {};
    static register(modeName: string, adapter: any) {
        Qiniu.ADAPTERS[modeName] = adapter;
    }

    private regions: Region[];

    constructor(private readonly accessKey: string,
                private readonly secretKey: string,
                private readonly ucUrl?: string,
                private readonly appendedUserAgent?: string,
                regions?: Region[]) {
        this.regions = regions || [];
    }

    mode(modeName: string, callbacks?: Callbacks): Adapter {
        const adapter: any = Qiniu.ADAPTERS[modeName];
        if (!adapter) {
            throw new Error(`Invalid qiniu mode: ${modeName}`);
        }
        return new adapter({
            accessKey: this.accessKey,
            secretKey: this.secretKey,
            regions: this.regions,
            ucUrl: this.ucUrl,
            appendedUserAgent: this.appendedUserAgent,
            requestCallback: callbacks?.requestCallback,
            responseCallback: callbacks?.responseCallback,
        });
    }
}

Qiniu.register(KODO_MODE, Kodo);
Qiniu.register(S3_MODE, S3);
