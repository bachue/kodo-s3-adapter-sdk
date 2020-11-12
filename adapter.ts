import os from 'os'
import pkg from './package.json';
import { Region } from './region';

export const QINIU_MODE: string = 'qiniu';
export const S3_MODE: string = 's3';
export const USER_AGENT: string = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )`;

export interface AdapterOption {
    accessKey: string;
    secretKey: string;
    userAgent: string;
    regions: Region[];
}

export class Qiniu {
    private userAgent: string = USER_AGENT;
    private adapters: { [key: string]: any; } = {};
    private regions: Region[] = []

    constructor(private accessKey: string, private secretKey: string) {
    }

    register(modeName: string, adapter: any) {
        this.adapters[modeName] = adapter;
    }

    setRegions(regions: Region[]) {
        this.regions = regions;
    }

    mode(modeName: string): any {
        const Adapter: any = this.adapters[modeName];
        if (!Adapter) {
            throw new Error(`Invalid qiniu mode: ${modeName}`);
        }
        return new Adapter({
            accessKey: this.accessKey,
            secretKey: this.secretKey,
            userAgent: this.userAgent,
            regions: this.regions,
        });
    }
}
