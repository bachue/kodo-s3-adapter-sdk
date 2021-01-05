import { Region } from './region';
import { URL } from 'url';

export abstract class Adapter {
    abstract createBucket(region: string, bucket: string): Promise<void>;
    abstract deleteBucket(region: string, bucket: string): Promise<void>;
    abstract getBucketLocation(bucket: string): Promise<string>;
    abstract listBuckets(): Promise<Array<Bucket>>;
    abstract listDomains(region: string, bucket: string): Promise<Array<Domain>>;

    abstract isExists(region: string, object: Object): Promise<boolean>;
    abstract getFrozenInfo(region: string, object: Object): Promise<FrozenInfo>;
    abstract unfreeze(region: string, object: Object, days: number): Promise<void>;

    abstract moveObject(region: string, transferObject: TransferObject): Promise<void>;
    abstract moveObjects(region: string, transferObjects: Array<TransferObject>, callback?: BatchCallback): Promise<Array<PartialObjectError>>;
    abstract copyObject(region: string, transferObject: TransferObject): Promise<void>;
    abstract copyObjects(region: string, transferObjects: Array<TransferObject>, callback?: BatchCallback): Promise<Array<PartialObjectError>>;
    abstract deleteObject(region: string, object: Object): Promise<void>;
    abstract deleteObjects(region: string, bucket: string, keys: Array<string>, callback?: BatchCallback): Promise<Array<PartialObjectError>>;

    abstract getObjectHeader(region: string, object: Object, domain?: Domain): Promise<ObjectHeader>;
    abstract getObject(region: string, object: Object, domain?: Domain): Promise<ObjectGetResult>;
    abstract getObjectURL(region: string, object: Object, domain?: Domain, deadline?: Date): Promise<URL>;
    abstract putObject(region: string, object: Object, data: Buffer, header?: SetObjectHeader, progressCallback?: ProgressCallback): Promise<void>;

    abstract createMultipartUpload(region: string, object: Object, header?: SetObjectHeader): Promise<InitPartsOutput>;
    abstract uploadPart(region: string, object: Object, uploadId: string, partNumber: number,
                       data: Buffer, progressCallback?: ProgressCallback): Promise<UploadPartOutput>;
    abstract completeMultipartUpload(region: string, object: Object, uploadId: string, parts: Array<Part>, header?: SetObjectHeader): Promise<void>;

    abstract listObjects(region: string, bucket: string, prefix: string, option?: ListObjectsOption): Promise<ListedObjects>;
}

export type BatchCallback = (index: number, error?: Error) => any;
export type ProgressCallback = (uploaded: number, total: number) => any;

export interface ListObjectsOption {
     delimiter?: string;
     minKeys?: number;
     maxKeys?: number;
     nextContinuationToken?: string;
}

export interface ListedObjects {
    objects: Array<ObjectInfo>;
    commonPrefixes?: Array<Object>;
    nextContinuationToken?: string,
}

export interface AdapterOption {
    accessKey: string;
    secretKey: string;
    regions: Array<Region>;
    ucUrl?: string;
    appendedUserAgent?: string;
}

export interface Bucket {
    id: string;
    name: string;
    createDate: Date;
    regionId?: string;
}

export interface Domain {
    name: string;
    protocol: string;
    private: boolean;
    type: 'normal' | 'pan' | 'test';
}

export interface FrozenInfo {
    status: FrozenStatus;
    expiryDate?: Date;
}

export type StorageClass = 'Standard' | 'InfrequentAccess' | 'Glacier';
export type FrozenStatus = 'Normal' | 'Frozen' | 'Unfreezing' | 'Unfrozen';

export interface TransferObject {
    from: Object;
    to: Object;
}

export interface Object {
    bucket: string;
    key: string;
}

export interface PartialObjectError extends Object {
    error?: Error;
}

export interface ObjectGetResult {
    data: Buffer;
    header: ObjectHeader;
}

export interface SetObjectHeader {
    metadata?: { [key: string]: string; };
}

export interface ObjectHeader extends SetObjectHeader {
    size: number;
    contentType: string;
    lastModified: Date;
    metadata: { [key: string]: string; };
}

export interface ObjectInfo extends Object {
    size: number;
    lastModified: Date;
    storageClass: StorageClass;
}

export interface InitPartsOutput {
    uploadId: string
}

export interface UploadPartOutput {
    etag: string
}

export interface Part {
    partNumber: number;
    etag: string;
}
