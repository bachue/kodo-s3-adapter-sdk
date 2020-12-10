import { Region } from './region';
import { URL } from 'url';
// import { FileHandle } from 'fs/promises';

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
    abstract putObject(region: string, object: Object, data: Buffer, header?: SetObjectHeader): Promise<void>;
    // abstract putObjectFromFile(region: string, object: Object, file: FileHandle, putCallback?: PutCallback): Promise<void>;

    abstract listFiles(region: string, bucket: string, prefix: string, option?: ListFilesOption): Promise<ListedFiles>;
}

export type BatchCallback = (index: number, error?: Error) => void;

export interface ListFilesOption {
     delimiter?: string;
     minKeys?: number;
     maxKeys?: number;
     nextContinuationToken?: string;
}

export interface ListedFiles {
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
    regionId: string;
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

export enum StorageClass {
    Standard,
    InfrequentAccess,
    Glacier,
}

export enum FrozenStatus {
    Normal,
    Frozen,
    Unfreezing,
    Unfrozen,
}

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

export interface PutCallback {
    progressCallback?: (uploaded: number, total: number) => void;
    partPutCallback?: (partNumber: number) => void;
}
