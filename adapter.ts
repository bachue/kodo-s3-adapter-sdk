import { Region } from './region';
import { URL } from 'url';
import { Throttle } from 'stream-throttle';
import { Readable } from 'stream';
import { OutgoingHttpHeaders } from 'http';

export abstract class Adapter {
    abstract enter<T>(sdkApiName: string, f: (scope: Adapter) => Promise<T>): Promise<T>;
    abstract createBucket(region: string, bucket: string): Promise<void>;
    abstract deleteBucket(region: string, bucket: string): Promise<void>;
    abstract getBucketLocation(bucket: string): Promise<string>;
    abstract listBuckets(): Promise<Array<Bucket>>;
    abstract listDomains(region: string, bucket: string): Promise<Array<Domain>>;

    abstract isExists(region: string, object: Object): Promise<boolean>;
    abstract getFrozenInfo(region: string, object: Object): Promise<FrozenInfo>;
    abstract restoreObject(region: string, object: Object, days: number): Promise<void>;
    abstract restoreObjects(s3RegionId: string, bucket: string, keys: Array<string>, days: number, callback?: BatchCallback): Promise<Array<PartialObjectError>>;
    abstract setObjectStorageClass(region: string, object: Object, storageClass: StorageClass): Promise<void>;
    abstract setObjectsStorageClass(s3RegionId: string, bucket: string, keys: Array<string>, storageClass: StorageClass, callback?: BatchCallback): Promise<Array<PartialObjectError>>;

    abstract moveObject(region: string, transferObject: TransferObject): Promise<void>;
    abstract moveObjects(region: string, transferObjects: Array<TransferObject>, callback?: BatchCallback): Promise<Array<PartialObjectError>>;
    abstract copyObject(region: string, transferObject: TransferObject): Promise<void>;
    abstract copyObjects(region: string, transferObjects: Array<TransferObject>, callback?: BatchCallback): Promise<Array<PartialObjectError>>;
    abstract deleteObject(region: string, object: Object): Promise<void>;
    abstract deleteObjects(region: string, bucket: string, keys: Array<string>, callback?: BatchCallback): Promise<Array<PartialObjectError>>;

    abstract getObjectInfo(region: string, object: Object): Promise<ObjectInfo>;
    abstract getObjectHeader(region: string, object: Object, domain?: Domain): Promise<ObjectHeader>;
    abstract getObject(region: string, object: Object, domain?: Domain): Promise<ObjectGetResult>;
    abstract getObjectURL(region: string, object: Object, domain?: Domain, deadline?: Date): Promise<URL>;
    abstract getObjectStream(s3RegionId: string, object: Object, domain?: Domain, option?: GetObjectStreamOption): Promise<Readable>;
    abstract putObject(region: string, object: Object, data: Buffer, originalFileName: string,
        header?: SetObjectHeader, option?: PutObjectOption): Promise<void>;

    abstract createMultipartUpload(region: string, object: Object, originalFileName: string, header?: SetObjectHeader): Promise<InitPartsOutput>;
    abstract uploadPart(region: string, object: Object, uploadId: string, partNumber: number,
        data: Buffer, option?: PutObjectOption): Promise<UploadPartOutput>;
    abstract completeMultipartUpload(region: string, object: Object, uploadId: string, parts: Array<Part>, originalFileName: string, header?: SetObjectHeader): Promise<void>;

    abstract listObjects(region: string, bucket: string, prefix: string, option?: ListObjectsOption): Promise<ListedObjects>;

    abstract clearCache(): void;
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
    appName?: string;
    appVersion?: string;
    uplogBufferSize?: number;
    requestCallback?: (request: RequestInfo) => void;
    responseCallback?: (response: ResponseInfo) => void;
}

export interface RequestInfo {
    url: string;
    method: string;
    headers: OutgoingHttpHeaders;
    data?: any;
}

export interface ResponseInfo {
    request: RequestInfo;
    interval: number;
    statusCode?: number;
    headers?: OutgoingHttpHeaders;
    data?: any;
    error?: any;
}

export interface Bucket {
    id: string;
    name: string;
    createDate: Date;
    regionId?: string;
    grantedPermission?: 'readonly' | 'readwrite';
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
    contentType?: string;
}

export interface ObjectHeader extends SetObjectHeader {
    size: number;
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

export interface PutObjectOption {
    progressCallback?: ProgressCallback;
    throttle?: Throttle;
}

export interface GetObjectStreamOption {
    rangeStart?: number;
    rangeEnd?: number;
}
