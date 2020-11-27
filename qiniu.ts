import { Region } from './region';
import { Adapter } from './adapter';
import { Kodo } from './kodo';

export const KODO_MODE: string = 'kodo';
export const S3_MODE: string = 's3';

export class Qiniu {
    private static ADAPTERS: { [key: string]: typeof Adapter; } = {};
    static register(modeName: string, adapter: any) {
        Qiniu.ADAPTERS[modeName] = adapter;
    }

    private regions: Region[] = []

    constructor(private accessKey: string, private secretKey: string, private ucUrl?: string) {
    }


    setRegions(regions: Region[]) {
        this.regions = regions;
    }

    mode(modeName: string): Adapter {
        const adapter: any = Qiniu.ADAPTERS[modeName];
        if (!adapter) {
            throw new Error(`Invalid qiniu mode: ${modeName}`);
        }
        return new adapter({
            accessKey: this.accessKey,
            secretKey: this.secretKey,
            regions: this.regions,
            ucUrl: this.ucUrl,
        });
    }
}

Qiniu.register(KODO_MODE, Kodo);
