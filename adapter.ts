import { Region } from './region';
import { URL } from 'url';
import { Readable } from 'stream';
import { OutgoingHttpHeaders } from 'http';
import { NatureLanguage } from './uplog';
import { KodoHttpClient } from './kodo-http-client';

export abstract class Adapter {
    abstract readonly client: KodoHttpClient;

    abstract storageClasses: StorageClass[];

    abstract enter<T>(sdkApiName: string, f: (scope: Adapter) => Promise<T>, sdkUplogOption?: EnterUplogOption): Promise<T>;
    abstract createBucket(region: string, bucket: string): Promise<void>;
    abstract deleteBucket(region: string, bucket: string): Promise<void>;
    abstract getBucketLocation(bucket: string): Promise<string>;
    abstract listBuckets(): Promise<Bucket[]>;
    abstract listDomains(region: string, bucket: string): Promise<Domain[]>;
    abstract updateBucketRemark(bucket: string, remark: string): Promise<void>;

    abstract isExists(region: string, object: StorageObject): Promise<boolean>;
    abstract getFrozenInfo(region: string, object: StorageObject): Promise<FrozenInfo>;
    abstract restoreObject(region: string, object: StorageObject, days: number): Promise<void>;
    abstract restoreObjects(s3RegionId: string, bucket: string, keys: string[], days: number, callback?: BatchCallback): Promise<PartialObjectError[]>;
    abstract setObjectStorageClass(region: string, object: StorageObject, storageClass: StorageClass['kodoName']): Promise<void>;
    abstract setObjectsStorageClass(s3RegionId: string, bucket: string, keys: string[], storageClass: StorageClass['kodoName'], callback?: BatchCallback): Promise<PartialObjectError[]>;

    abstract moveObject(region: string, transferObject: TransferObject): Promise<void>;
    abstract moveObjects(region: string, transferObjects: TransferObject[], callback?: BatchCallback): Promise<PartialObjectError[]>;
    abstract copyObject(region: string, transferObject: TransferObject): Promise<void>;
    abstract copyObjects(region: string, transferObjects: TransferObject[], callback?: BatchCallback): Promise<PartialObjectError[]>;
    abstract deleteObject(region: string, object: StorageObject): Promise<void>;
    abstract deleteObjects(region: string, bucket: string, keys: string[], callback?: BatchCallback): Promise<PartialObjectError[]>;

    abstract getObjectInfo(region: string, object: StorageObject): Promise<ObjectInfo>;
    abstract getObjectHeader(region: string, object: StorageObject, domain?: Domain): Promise<ObjectHeader>;
    abstract getObject(
      region: string,
      object: StorageObject,
      domain?: Domain,
      style?: UrlStyle,
    ): Promise<ObjectGetResult>;
    abstract getObjectURL(
        region: string,
        object: StorageObject,
        domain?: Domain,
        deadline?: Date,
        style?: UrlStyle,
    ): Promise<URL>;
    abstract getObjectStream(s3RegionId: string, object: StorageObject, domain?: Domain, option?: GetObjectStreamOption): Promise<Readable>;
    abstract putObject(
        region: string,
        object: StorageObject,
        data: Buffer | Readable | (() => Buffer | Readable),
        originalFileName: string,
        header?: SetObjectHeader,
        option?: PutObjectOption,
    ): Promise<void>;
    abstract createMultipartUpload(
        region: string,
        object: StorageObject,
        originalFileName: string,
        header?: SetObjectHeader,
        abortSignal?: AbortSignal,
        accelerateUploading?: boolean,
    ): Promise<InitPartsOutput>;
    abstract uploadPart(
        region: string,
        object: StorageObject,
        uploadId: string,
        partNumber: number,
        data: Buffer | Readable | (() => Buffer | Readable),
        option?: PutObjectOption,
    ): Promise<UploadPartOutput>;
    abstract completeMultipartUpload(
        region: string,
        object: StorageObject,
        uploadId: string,
        parts: Part[],
        originalFileName: string,
        header?: SetObjectHeader,
        abortSignal?: AbortSignal,
        accelerateUploading?: boolean,
    ): Promise<void>;

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

export type EnterUplogOption = Omit<SdkUplogOption, 'language'>;

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
    sessionToken?: string;
    regions: Region[];
    ucUrl?: string;
    appendedUserAgent?: string;
    appName: string;
    appVersion: string;
    appNatureLanguage: NatureLanguage,
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
    remark?: string;
}

export interface BucketDetails {
    perm: number,
    private: number,
    protected: number,
    // still have others fields but not list in here
}

export interface Domain {
    name: string;
    protocol: string;
    private: boolean;
    protected: boolean;
    type: 'cdn' | 'origin' | 'others';
    apiScope: 'kodo' | 's3';
}
export type DomainWithoutShouldSign = Omit<Domain, 'private' | 'protected'>;

export interface FrozenInfo {
    status: FrozenStatus;
    expiryDate?: Date;
}

export type StorageClass = {
    fileType: number,
    kodoName: string,
    s3Name: string,
};

export const DEFAULT_STORAGE_CLASS: StorageClass = {
    fileType: 0,
    kodoName: 'Standard',
    s3Name: 'STANDARD',
};

export type FrozenStatus = 'Normal' | 'Frozen' | 'Unfreezing' | 'Unfrozen';

export interface TransferObject {
    from: StorageObject;
    to: StorageObject;
}

export interface StorageObject {
    bucket: string;
    key: string;
    storageClassName?: StorageClass['kodoName'];
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
    objectLockMode?: string;
    objectLockRetainUntilDate?: Date;
}

export interface ObjectInfo extends StorageObject {
    size: number;
    lastModified: Date;
    storageClass: StorageClass['kodoName'];
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

export interface FileStreamSetting {
    path: string,
    start: number,
    end: number,
}

export interface PutObjectOption {
    /**
     * control to abort upload request
     */
    abortSignal?: AbortSignal;

    /**
     * For both kodo and s3, they need to calculate md5 when uploading.
     *
     * Specially s3, it not supports non-file stream when use http protocol.
     * use this options to hack it like a file stream.
     * ref: https://github.com/aws/aws-sdk-js/blob/v2.1015.0/lib/util.js#L730-L755
     */
    fileStreamSetting?: FileStreamSetting;

    progressCallback?: ProgressCallback;

    accelerateUploading?: boolean;
}

export enum UrlStyle {
    Path = 'path',
    VirtualHost = 'virtualHost',
    BucketEndpoint = 'bucketEndpoint',
}

export interface GetObjectStreamOption {
    rangeStart?: number;
    rangeEnd?: number;
    abortSignal?: AbortSignal;
    urlStyle?: UrlStyle,
}
