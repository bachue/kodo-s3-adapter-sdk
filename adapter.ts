import { Region } from './region';

export abstract class Adapter {
    abstract createBucket(region: string, bucket: string): Promise<void>;
    abstract deleteBucket(region: string, bucket: string): Promise<void>;
    abstract getBucketLocation(bucket: string): Promise<string>;
}

export interface AdapterOption {
    accessKey: string;
    secretKey: string;
    regions: Array<Region>;
    ucUrl?: string;
}

