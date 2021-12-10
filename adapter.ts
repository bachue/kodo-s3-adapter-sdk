import { Region } from './region';
import { URL } from 'url';
import { Throttle } from 'stream-throttle';
import { Readable } from 'stream';
import { OutgoingHttpHeaders } from 'http';
import { NatureLanguage } from './uplog';

export abstract class Adapter {
    abstract enter<T>(sdkApiName: string, f: (scope: Adapter) => Promise<T>, sdkUplogOption: SdkUplogOption): Promise<T>;
    abstract createBucket(region: string, bucket: string): Promise<void>;
    abstract deleteBucket(region: string, bucket: string): Promise<void>;
    abstract getBucketLocation(bucket: string): Promise<string>;
    abstract listBuckets(): Promise<Bucket[]>;
    abstract listDomains(region: string, bucket: string): Promise<Domain[]>;

    abstract isExists(region: string, object: StorageObject): Promise<boolean>;
    abstract getFrozenInfo(region: string, object: StorageObject): Promise<FrozenInfo>;
    abstract restoreObject(region: string, object: StorageObject, days: number): Promise<void>;
    abstract restoreObjects(s3RegionId: string, bucket: string, keys: string[], days: number, callback?: BatchCallback): Promise<PartialObjectError[]>;
    abstract setObjectStorageClass(region: string, object: StorageObject, storageClass: StorageClass): Promise<void>;
    abstract setObjectsStorageClass(s3RegionId: string, bucket: string, keys: string[], storageClass: StorageClass, callback?: BatchCallback): Promise<PartialObjectError[]>;

    abstract moveObject(region: string, transferObject: TransferObject): Promise<void>;
    abstract moveObjects(region: string, transferObjects: TransferObject[], callback?: BatchCallback): Promise<PartialObjectError[]>;
    abstract copyObject(region: string, transferObject: TransferObject): Promise<void>;
    abstract copyObjects(region: string, transferObjects: TransferObject[], callback?: BatchCallback): Promise<PartialObjectError[]>;
    abstract deleteObject(region: string, object: StorageObject): Promise<void>;
    abstract deleteObjects(region: string, bucket: string, keys: string[], callback?: BatchCallback): Promise<PartialObjectError[]>;

    abstract getObjectInfo(region: string, object: StorageObject): Promise<ObjectInfo>;
    abstract getObjectHeader(region: string, object: StorageObject, domain?: Domain): Promise<ObjectHeader>;
    abstract getObject(region: string, object: StorageObject, domain?: Domain): Promise<ObjectGetResult>;
    abstract getObjectURL(region: string, object: StorageObject, domain?: Domain, deadline?: Date): Promise<URL>;
    abstract getObjectStream(s3RegionId: string, object: StorageObject, domain?: Domain, option?: GetObjectStreamOption): Promise<Readable>;
    abstract putObject(region: string, object: StorageObject, data: Buffer, originalFileName: string,
                       header?: SetObjectHeader, option?: PutObjectOption): Promise<void>;

    abstract createMultipartUpload(region: string, object: StorageObject, originalFileName: string, header?: SetObjectHeader): Promise<InitPartsOutput>;
    abstract uploadPart(region: string, object: StorageObject, uploadId: string, partNumber: number,
                        data: Buffer, option?: PutObjectOption): Promise<UploadPartOutput>;
    abstract completeMultipartUpload(region: string, object: StorageObject, uploadId: string, parts: Part[], originalFileName: string, header?: SetObjectHeader): Promise<void>;

    abstract listObjects(region: string, bucket: string, prefix: string, option?: ListObjectsOption): Promise<ListedObjects>;

    abstract clearCache(): void;
}

export type BatchCallback = (index: number, error?: Error) => any;
export type ProgressCallback = (uploaded: number, total: number) => any;

export interface SdkUplogOption {
    language: NatureLanguage,
    targetBucket?: string,
    targetKey?: string,
}

export interface ListObjectsOption {
    delimiter?: string;
    minKeys?: number;
    maxKeys?: number;
    nextContinuationToken?: string;
}

export interface ListedObjects {
    objects: ObjectInfo[];
    commonPrefixes?: StorageObject[];
    nextContinuationToken?: string,
}

export interface AdapterOption {
    accessKey: string;
    secretKey: string;
    regions: Region[];
    ucUrl?: string;
    appendedUserAgent?: string;
    appName: string;
    appVersion: string;
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
    from: StorageObject;
    to: StorageObject;
}

export interface StorageObject {
    bucket: string;
    key: string;
    storageClassName?: StorageClass;
}

export interface PartialObjectError extends StorageObject {
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

export interface ObjectInfo extends StorageObject {
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
