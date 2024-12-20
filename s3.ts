import dns from 'dns';
import AsyncLock from 'async-lock';
import AWS, { AWSError } from 'aws-sdk';
import os from 'os';
import fs from 'fs';
import pkg from './package.json';
import md5 from 'js-md5';
import { URL } from 'url';
import { PassThrough, Readable, Writable } from 'stream';
import { Semaphore } from 'semaphore-promise';
import { Kodo } from './kodo';
import { ReadableStreamBuffer } from 'stream-buffers';
import {
    Adapter,
    AdapterOption,
    BatchCallback,
    Bucket,
    Domain,
    EnterUplogOption,
    FrozenInfo,
    GetObjectStreamOption,
    InitPartsOutput,
    ListedObjects,
    ListObjectsOption,
    ObjectGetResult,
    ObjectHeader,
    ObjectInfo,
    Part,
    PartialObjectError,
    PutObjectOption,
    RequestInfo,
    ResponseInfo,
    SdkUplogOption,
    SetObjectHeader,
    StorageClass,
    StorageObject,
    TransferObject,
    UploadPartOutput,
    UrlStyle,
} from './adapter';
import {
    ErrorRequestUplogEntry,
    ErrorType,
    GenRequestUplogEntry,
    GenSdkApiUplogEntry,
    getErrorTypeFromS3Error,
    getErrorTypeFromStatusCode,
    RequestUplogEntry,
    SdkApiUplogEntry,
} from './uplog';
import { HttpClient, RequestStats } from './http-client';
import { RegionRequestOptions } from './region';
import { generateReqId } from './req_id';

export const USER_AGENT = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/s3`;

interface RequestOptions {
    stats?: RequestStats,
    abortSignal?: AbortSignal,
}

export class S3 extends Kodo {
    protected bucketNameToIdCache: { [name: string]: string; } = {};
    protected bucketIdToNameCache: { [id: string]: string; } = {};
    protected clients: { [key: string]: AWS.S3; } = {};
    protected clientsLock = new AsyncLock();
    protected listKodoBucketsPromise?: Promise<Bucket[]>;

    /**
     * if domain exists, the urlStyle will be forced to 'bucketEndpoint'
     */
    private async getClient(
      s3RegionId?: string,
      urlStyle: UrlStyle = UrlStyle.Path,
      domain?: Domain,
    ): Promise<AWS.S3> {
        const cacheKey = [s3RegionId ?? '', urlStyle, domain?.name ?? ''].join(':');
        if (this.clients[cacheKey]) {
            return this.clients[cacheKey];
        }

        const client = await this.clientsLock.acquire(cacheKey, async (): Promise<AWS.S3> => {
            let userAgent = USER_AGENT;
            if (this.adapterOption.appendedUserAgent) {
                userAgent += `/${this.adapterOption.appendedUserAgent}`;
            }
            const s3IdEndpoint = await this.regionService.getS3Endpoint(s3RegionId, this.getRegionRequestOptions());
            const urlStyleOptions: {
                endpoint: string,
                s3ForcePathStyle?: boolean,
                s3BucketEndpoint?: boolean,
            } = {
                endpoint: !domain
                    ? s3IdEndpoint.s3Endpoint
                    : `${domain.protocol}://${domain.name}`,
            };
            if (urlStyle === UrlStyle.BucketEndpoint) {
                urlStyleOptions.s3BucketEndpoint = true;
            } else {
                urlStyleOptions.s3ForcePathStyle = urlStyle === UrlStyle.Path;
            }

            return new AWS.S3({
                apiVersion: '2006-03-01',
                customUserAgent: userAgent,
                computeChecksums: true,
                region: s3IdEndpoint.s3Id,
                maxRetries: 10,
                signatureVersion: 'v4',
                useDualstack: true,
                credentials: {
                    accessKeyId: this.adapterOption.accessKey,
                    secretAccessKey: this.adapterOption.secretKey,
                    sessionToken: this.adapterOption.sessionToken,
                },
                httpOptions: {
                    connectTimeout: 30000,
                    timeout: 300000,
                    agent: urlStyleOptions.endpoint.startsWith('https://')
                        ? HttpClient.httpsKeepaliveAgent
                        : HttpClient.httpKeepaliveAgent,
                },
                ...urlStyleOptions
            });
        });
        this.clients[cacheKey] = client;
        return client;
    }

    addBucketNameIdCache(bucketName: string, bucketId: string): void {
        this.bucketNameToIdCache[bucketName] = bucketId;
        this.bucketIdToNameCache[bucketId] = bucketName;
    }

    async fromKodoBucketNameToS3BucketId(bucketName: string): Promise<string> {
        if (this.bucketNameToIdCache[bucketName]) {
            return this.bucketNameToIdCache[bucketName];
        }

        await this.listKodoBuckets();

        if (this.bucketNameToIdCache[bucketName]) {
            return this.bucketNameToIdCache[bucketName];
        } else {
            return bucketName;
        }
    }

    async fromS3BucketIdToKodoBucketName(bucketId: string): Promise<string> {
        if (this.bucketIdToNameCache[bucketId]) {
            return this.bucketIdToNameCache[bucketId];
        }

        await this.listKodoBuckets();

        if (!this.bucketIdToNameCache[bucketId]) {
            throw new Error(`Cannot find bucket name of bucket ${bucketId}`);
        }

        return this.bucketIdToNameCache[bucketId];
    }

    async enter<T>(
        sdkApiName: string,
        f: (scope: Adapter, options: RegionRequestOptions) => Promise<T>,
        enterUplogOption?: EnterUplogOption,
    ): Promise<T> {
        const scope = new S3Scope(
            sdkApiName,
            this.adapterOption,
            {
                ...enterUplogOption,
                language: this.adapterOption.appNatureLanguage,
            },
            {
                bucketNameToIdCache: this.bucketNameToIdCache,
                bucketIdToNameCache: this.bucketIdToNameCache,
                clients: this.clients,
                clientsLock: this.clientsLock,
                listKodoBucketsPromise: this.listKodoBucketsPromise,
            },
        );

        try {
            const data = await f(scope, scope.getRegionRequestOptions());
            await scope.done(true).catch(() => null); // ignore error
            return data;
        } catch (err) {
            await scope.done(false).catch(() => null); // ignore error
            throw err;
        }
    }

    private async sendS3Request<D, E extends AWSError>(
        request: AWS.Request<D, E>,
        apiName: string,
        bucketName?: string,
        key?: string,
        isCreateStream?: false,
        options?: RequestOptions,
    ): Promise<D>
    private async sendS3Request<D, E extends AWSError>(
        request: AWS.Request<D, E>,
        apiName: string,
        bucketName?: string,
        key?: string,
        isCreateStream?: true,
        options?: RequestOptions,
    ): Promise<Readable>
    private async sendS3Request<D, E extends AWSError>(
        request: AWS.Request<D, E>,
        apiName: string,
        bucketName?: string,
        key?: string,
        isCreateStream = false,
        options: RequestOptions = {},
    ): Promise<D | Readable> {
        let requestInfo: RequestInfo | undefined;
        const beginTime = new Date().getTime();
        const uplogMaker = new GenRequestUplogEntry(
            apiName,
            {
                apiType: 's3',
                httpVersion: '1.1',
                method: request.httpRequest.method,
                sdkName: this.adapterOption.appName,
                sdkVersion: this.adapterOption.appVersion,
                targetBucket: bucketName,
                targetKey: key,
                url: new URL(request.httpRequest.endpoint.href),
            },
        );

        let uplog: RequestUplogEntry | ErrorRequestUplogEntry;

        const reqId = generateReqId({
            url: request.httpRequest.endpoint.href,
            method: request.httpRequest.method,
            headers: request.httpRequest.headers,
        });
        request.httpRequest.headers['X-Reqid'] = reqId;

        options = {
            ...options,
            // for uplog with {@link S3Scope}
            ...this.getRequestsOption(),
        };

        // for uplog remoteAddress
        const remoteAddress = await new Promise<string>(resolve => {
            dns.lookup(request.httpRequest.endpoint.hostname, (err, address) => {
                if (err) {
                    resolve('');
                    return;
                }
                resolve(address);
            });
        });

        // abort option
        if (options.abortSignal) {
            if (options.abortSignal.aborted) {
                return Promise.reject(HttpClient.userCanceledError);
            }
            options.abortSignal.addEventListener('abort', () => {
                request.abort();
            }, {
                once: true,
            });
        }

        // request events
        request.on('sign', (request) => {
            if (options.stats) {
                options.stats.requestsCount += 1;
            }
            let url = request.httpRequest.endpoint.href;
            if (url.endsWith('/') && request.httpRequest.path.startsWith('/')) {
                url += request.httpRequest.path.substring(1);
            } else {
                url += request.httpRequest.path;
            }
            requestInfo = {
                url,
                method: request.httpRequest.method,
                headers: request.httpRequest.headers,
                data: request.httpRequest.body,
            };
            if (this.adapterOption.requestCallback) {
                this.adapterOption.requestCallback(requestInfo);
            }
        });
        request.on('complete', (response) => {
            const responseInfo: ResponseInfo = {
                request: requestInfo!,
                statusCode: response.httpResponse.statusCode,
                headers: response.httpResponse.headers,
                data: response.data,
                error: response.error,
                interval: new Date().getTime() - beginTime,
            };
            if (this.adapterOption.responseCallback) {
                this.adapterOption.responseCallback(responseInfo);
            }

            const reqBodyLength = getContentLength(request.httpRequest);
            const resBodyLength = getContentLength(response.httpResponse);
            if (options.stats) {
                options.stats.bytesTotalSent +=
                    reqBodyLength;
                options.stats.bytesTotalReceived +=
                    resBodyLength;
            }
            uplog = uplogMaker.getRequestUplogEntry({
                costDuration: responseInfo.interval,
                remoteIp: remoteAddress,
                bytesSent: reqBodyLength,
                bytesReceived: resBodyLength,
                reqId: response.requestId,
                statusCode: response.httpResponse.statusCode,
            });
            if (response.error) {
                if (response.httpResponse.statusCode) {
                    uplog.error_type = getErrorTypeFromStatusCode(response.httpResponse.statusCode);
                } else {
                    uplog.error_type = getErrorTypeFromS3Error(response.error);
                }
                uplog.error_description = (response.error as any).message || (response.error as any).code;
                if (options.stats) {
                    options.stats.errorType = uplog.error_type;
                    options.stats.errorDescription = uplog.error_description;
                }
            }
            this.log(uplog);
        });

        if (isCreateStream) {
            return request.createReadStream();
        }

        return await new Promise<D>((resolve, reject) => {
            request.send((err, data) => {
                if (err) {
                    if (
                        options.abortSignal?.aborted &&
                        err.statusCode &&
                        err.statusCode >= 200 &&
                        err.statusCode < 300
                    ) {
                        resolve(data);
                    } else if (options.abortSignal?.aborted) {
                        reject(HttpClient.userCanceledError);
                    } else {
                        reject(err);
                    }
                } else {
                    resolve(data);
                }
            });
        });
    }

    async createBucket(s3RegionId: string, bucket: string): Promise<void> {
        const s3 = await this.getClient(s3RegionId);
        const request = s3.createBucket({
            Bucket: bucket,
            CreateBucketConfiguration: {
                LocationConstraint: s3RegionId,
            },
        });
        await this.sendS3Request(request, 'createBucket', bucket);
    }

    async deleteBucket(s3RegionId: string, bucket: string): Promise<void> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(bucket),
        ]);
        const request = s3.deleteBucket({ Bucket: bucketId });
        await this.sendS3Request(request, 'deleteBucket', bucket);
    }

    async getBucketLocation(bucket: string): Promise<string> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(),
            this.fromKodoBucketNameToS3BucketId(bucket),
        ]);
        return await this._getBucketLocation(s3, bucketId);
    }

    private async _getBucketLocation(s3: AWS.S3, bucketId: string): Promise<string> {
        const request = s3.getBucketLocation({ Bucket: bucketId });
        const data = await this.sendS3Request(request, 'getBucketLocation', this.bucketIdToNameCache[bucketId] ?? bucketId);
        return data.LocationConstraint!;
    }

    private async listKodoBuckets(): Promise<Bucket[]> {
        if (!this.listKodoBucketsPromise) {
            this.listKodoBucketsPromise = super.listBuckets();
        }
        const buckets = await this.listKodoBucketsPromise;
        buckets.forEach((bucket) => {
            this.bucketNameToIdCache[bucket.name] = bucket.id;
            this.bucketIdToNameCache[bucket.id] = bucket.name;
        });
        return buckets;
    }

    async listBuckets(): Promise<Bucket[]> {
        const s3 = await this.getClient();
        const data = await this.sendS3Request(s3.listBuckets(), 'listBuckets');

        if (!Array.isArray(data?.Buckets)) {
            return [];
        }

        let kodoBucketsMap: Record<string, Bucket> = {};
        kodoBucketsMap = (await this.listKodoBuckets()).reduce((res, bucket) => {
            res[bucket.id] = bucket;
            return res;
        }, kodoBucketsMap);
        const concurrencyLimit = 200; // to avoid Error "too many pending task"

        let result: Bucket[] = [];
        for (let i = 0; i < data.Buckets.length; i += concurrencyLimit){
            const segmentBuckets = data.Buckets.slice(i, i + concurrencyLimit);
            const segmentResult: Bucket[] = await Promise.all(segmentBuckets.map(async (bucket) => {
                const bucketS3Name = bucket.Name ?? '';
                const bucketLocation = await this._getBucketLocation(s3, bucketS3Name);
                return {
                    id: bucketS3Name,
                    name: kodoBucketsMap[bucketS3Name].name,
                    createDate: bucket.CreationDate ?? new Date(0),
                    regionId: bucketLocation,
                    remark: kodoBucketsMap[bucketS3Name].remark,
                };
            }));
            result = result.concat(segmentResult);
        }

        return result;
    }

    async listDomains(s3RegionId: string, bucket: string): Promise<Domain[]> {
        const fallbackScheme = this.adapterOption.ucUrl?.startsWith('https')
            ? 'https'
            : 'http';
        const domains = await this.getOriginDomains(
            s3RegionId,
            bucket,
            fallbackScheme,
        );
        return domains
            .filter(d => d.apiScope === 's3')
            .map(d => ({
                ...d,
                private: true,
                protected: false
            }));
    }

    async isExists(s3RegionId: string, object: StorageObject): Promise<boolean> {
        try {
            await this.getObjectInfo(s3RegionId, object);
        } catch (error: any) {
            if (error.message === 'no such file or directory') {
                return false;
            } else {
                throw error;
            }
        }
        return true;
    }

    async deleteObject(s3RegionId: string, object: StorageObject): Promise<void> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);
        const request = s3.deleteObject({ Bucket: bucketId, Key: object.key });
        await this.sendS3Request(request, 'deleteObject', object.bucket, object.key);
    }

    async putObject(
        s3RegionId: string,
        object: StorageObject,
        data: Buffer | Readable | (() => Buffer | Readable),
        _originalFileName: string,
        header?: SetObjectHeader,
        option?: PutObjectOption,
    ): Promise<void> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);

        if (typeof data === 'function') {
          data = data();
        }

        // get data source for http body
        const dataSource = S3.getDataSource(
            data,
            s3.endpoint.protocol,
            option,
        );

        // get content length and content md5
        let contentLength: number;
        let contentMd5: string;
        if (data instanceof Readable) {
            if (!option?.fileStreamSetting) {
                throw new Error('s3 need fileStreamSetting when use stream');
            }
            contentLength = (await fs.promises.stat(option.fileStreamSetting.path)).size;
            contentMd5 = await this.getContentMd5(
                option.fileStreamSetting.path,
                option.fileStreamSetting.start,
                option.fileStreamSetting.end,
            );
        } else {
            contentLength = data.length;
            contentMd5 = await this.getContentMd5(data);
        }

        // get put object params
        const params: AWS.S3.Types.PutObjectRequest = {
            Bucket: bucketId,
            Key: object.key,
            Body: dataSource,
            ContentLength: contentLength,
            ContentType: header?.contentType,
            ContentMD5: contentMd5,
            Metadata: header?.metadata,
            StorageClass: this.convertStorageClass(
                object.storageClassName,
                'kodoName',
                's3Name',
            ),
        };
        const uploader = s3.putObject(params);

        // progress callback
        if (option?.progressCallback) {
            uploader.on('httpUploadProgress', (progress) => {
                option.progressCallback?.(progress.loaded, progress.total);
            });
        }

        await this.sendS3Request(
            uploader,
            'putObject',
            object.bucket,
            object.key,
            false,
            {
                abortSignal: option?.abortSignal,
            },
        );
    }

    async getObject(
        s3RegionId: string,
        object: StorageObject,
        domain?: Domain,
        style?: UrlStyle,
    ): Promise<ObjectGetResult> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId, style, domain),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);
        const request = s3.getObject({ Bucket: bucketId, Key: object.key });
        const data: any = await this.sendS3Request(request, 'getObject', object.bucket, object.key);
        return {
            data: Buffer.from(data.Body!),
            header: {
                size: data.ContentLength!,
                contentType: data.ContentType!,
                lastModified: data.LastModified!,
                metadata: data.Metadata!,
            },
        };
    }

    async getObjectStream(
        s3RegionId: string,
        object: StorageObject,
        domain?: Domain,
        option?: GetObjectStreamOption,
    ): Promise<Readable> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId, option?.urlStyle, domain),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);
        let range: string | undefined;
        if (option?.rangeStart || option?.rangeEnd) {
            range = `bytes=${option?.rangeStart ?? ''}-${option?.rangeEnd ?? ''}`;
        }
        const request = s3.getObject({
            Bucket: bucketId,
            Key: object.key,
            Range: range,
        });
        return this.sendS3Request(
            request,
            'getObjectStream',
            object.bucket,
            object.key,
            true,
            {
                abortSignal: option?.abortSignal,
            },
        );
    }

    async getObjectURL(
        s3RegionId: string,
        object: StorageObject,
        domain?: Domain,
        deadline?: Date,
        style: UrlStyle = UrlStyle.Path,
    ): Promise<URL> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId, style, domain),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);
        const expires = deadline
            ? Math.floor((deadline.getTime() - Date.now()) / 1000)
            : 7 * 24 * 60 * 60;
        const url = s3.getSignedUrl('getObject', {
            Bucket: bucketId,
            Key: object.key,
            Expires: expires,
        });
        return new URL(url);
    }

    async getObjectInfo(s3RegionId: string, object: StorageObject): Promise<ObjectInfo> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);
        const data: any = await this.sendS3Request(
            s3.listObjects({ Bucket: bucketId, MaxKeys: 1, Prefix: object.key }),
            'getObjectInfo',
            object.bucket,
            object.key,
        );

        if (!data?.Contents?.[0]?.Key || data.Contents[0].Key !== object.key) {
            throw new Error('no such file or directory');
        }
        return {
            bucket: object.bucket,
            key: data.Contents[0].Key!,
            size: data.Contents[0].Size!,
            lastModified: data.Contents[0].LastModified!,
            storageClass: this.convertStorageClass(
                data.Contents[0].StorageClass,
                's3Name',
                'kodoName',
                'unknown',
            ),
        };
    }

    async getObjectHeader(
        s3RegionId: string,
        object: StorageObject,
        _domain?: Domain,
    ): Promise<ObjectHeader> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);
        const request = s3.headObject({ Bucket: bucketId, Key: object.key });
        const data = await this.sendS3Request(request, 'getObjectHeader', object.bucket, object.key);
        return {
            size: data.ContentLength!,
            contentType: data.ContentType!,
            lastModified: data.LastModified!,
            metadata: data.Metadata!,
            objectLockMode: data?.ObjectLockMode,
            objectLockRetainUntilDate: data?.ObjectLockRetainUntilDate,
        };
    }

    async moveObject(s3RegionId: string, transferObject: TransferObject): Promise<void> {
        const res = await this.getObjectHeader(s3RegionId, transferObject.from);
        if (res?.objectLockMode === 'COMPLIANCE') {
            if (!res.objectLockRetainUntilDate) {
                throw new Error('object locked and lost retain date');
            }
            if (res.objectLockRetainUntilDate >= new Date()) {
                throw new Error('object locked');
            }
        }

        try {
            await this.copyObject(s3RegionId, transferObject);
        } catch (err) {
            err.stage = 'copy';
            throw err;
        }

        try {
            await this.deleteObject(s3RegionId, transferObject.from);
        } catch (err) {
            err.stage = 'delete';
            throw err;
        }
    }

    async copyObject(s3RegionId: string, transferObject: TransferObject): Promise<void> {
        const { size, metadata } = await this.getObjectHeader(s3RegionId, transferObject.from);
        const [
            s3,
            storageClass,
            fromBucketId,
            toBucketId,
        ] = await Promise.all([
            this.getClient(s3RegionId),
            this.getObjectStorageClass(s3RegionId, transferObject.from),
            this.fromKodoBucketNameToS3BucketId(transferObject.from.bucket),
            this.fromKodoBucketNameToS3BucketId(transferObject.to.bucket),
        ]);
        try {
            return await this.copyObjectWhole({
                s3,
                storageClass,
                fromBucketId,
                fromKey: transferObject.from.key,
                toBucketId,
                toKey: transferObject.to.key,
                fromBucketName: transferObject.from.bucket,
            });
        } catch (err) {
            if (err.code !== 'CopyObjectTooLarge') {
                throw err;
            }
            return await this.copyObjectMultipart({
                s3,
                storageClass,
                fromBucketId,
                fromKey: transferObject.from.key,
                toBucketId,
                toKey: transferObject.to.key,
                size,
                metadata,
                fromBucketName: transferObject.from.bucket,
            });
        }
    }

    private async copyObjectWhole({
        s3,
        storageClass,
        fromBucketId,
        fromKey,
        toBucketId,
        toKey,
        fromBucketName,
    }: {
        s3: AWS.S3,
        storageClass?: string,
        fromBucketId: string,
        fromKey: string,
        toBucketId: string,
        toKey: string,
        // uplog
        fromBucketName: string,
    }): Promise<void> {
        const params: AWS.S3.Types.CopyObjectRequest = {
            Bucket: toBucketId,
            Key: toKey,
            CopySource: `/${fromBucketId}/${encodeURIComponent(fromKey)}`,
            MetadataDirective: 'COPY',
            StorageClass: storageClass,
        };
        await this.sendS3Request(s3.copyObject(params), 'copyObject', fromBucketName, fromKey);
    }

    private async copyObjectMultipart({
        s3,
        storageClass,
        fromBucketId,
        fromKey,
        toBucketId,
        toKey,
        size,
        partSize = 8 * (1 << 20),
        metadata,
        fromBucketName,
    }: {
        s3: AWS.S3,
        storageClass?: string,
        fromBucketId: string,
        fromKey: string,
        toBucketId: string,
        toKey: string,
        size: number,
        partSize?: number,
        metadata: Record<string, string>,
        // uplog
        fromBucketName: string,
    }): Promise<void> {
        const createParams: AWS.S3.CreateMultipartUploadRequest = {
            Bucket: toBucketId,
            Key: toKey,
            StorageClass: storageClass,
            Metadata: metadata,
        };
        const createRes = await this.sendS3Request(
            s3.createMultipartUpload(createParams),
            'createMultipartUpload',
            fromBucketName,
            fromKey,
        );
        if (!createRes.UploadId) {
            throw new Error('Create upload id failed');
        }

        const parts: Part[] = [];
        const _partSize = Math.max(
            partSize,
            Math.ceil(size / 10000),
        );

        const limiter = new Semaphore(10);
        const restPromise: Set<Promise<AWS.S3.UploadPartCopyOutput>> = new Set();
        let error: Error | null = null;
        for (let offset = 0; offset < size; offset += _partSize) {
            const releaseLimiter = await limiter.acquire();
            if (error) {
                throw error;
            }
            const end: number = Math.min(
                offset + _partSize - 1,
                size - 1,
            );
            const partNum = Math.floor(end / _partSize) + 1;
            // left-close, right-close
            const sourceRange = `bytes=${offset}-${end}`;
            const partParams: AWS.S3.UploadPartCopyRequest = {
                UploadId: createRes.UploadId,
                Bucket: toBucketId,
                Key: toKey,
                CopySource: `/${fromBucketId}/${encodeURIComponent(fromKey)}`,
                CopySourceRange: sourceRange,
                PartNumber: partNum,
            };
            const p = this.sendS3Request(
                s3.uploadPartCopy(partParams),
                'uploadPartCopy',
                fromBucketName,
                fromKey,
            );
            restPromise.add(p);
            p.then(partRes => {
                restPromise.delete(p);
                if (!partRes.CopyPartResult?.ETag) {
                    error = new Error('Can not copy by lost copy part etag.');
                    return;
                }
                parts.push({
                    partNumber: partNum,
                    etag: partRes.CopyPartResult.ETag,
                });
            })
                .catch(err => {
                    if (!error) {
                        error = err;
                    }
                })
                .finally(releaseLimiter);
        }
        await Promise.all(restPromise);
        if (error) {
            throw error;
        }

        parts.sort((part1, part2) => part1.partNumber - part2.partNumber);
        const completeParams: AWS.S3.CompleteMultipartUploadRequest = {
            Bucket: toBucketId,
            Key: toKey,
            UploadId: createRes.UploadId,
            MultipartUpload: {
                Parts: parts.map(p => ({
                    PartNumber: p.partNumber,
                    ETag: p.etag,
                })),
            },
        };
        await this.sendS3Request(
            s3.completeMultipartUpload(completeParams),
            'completeMultipartUpload',
            fromBucketName,
            fromKey,
        );
    }

    private async getObjectStorageClass(s3RegionId: string, object: StorageObject): Promise<string | undefined> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);
        const request = s3.headObject({ Bucket: bucketId, Key: object.key });
        const data: any = await this.sendS3Request(request, 'getObjectStorageClass', object.bucket, object.key);
        return data.StorageClass;
    }

    moveObjects(s3RegionId: string, transferObjects: TransferObject[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.s3BatchOps(transferObjects.map((to) => new MoveObjectOp(this, s3RegionId, to)), callback);
    }

    copyObjects(s3RegionId: string, transferObjects: TransferObject[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.s3BatchOps(transferObjects.map((to) => new CopyObjectOp(this, s3RegionId, to)), callback);
    }

    setObjectsStorageClass(
        s3RegionId: string,
        bucket: string,
        keys: string[],
        storageClass: StorageClass['kodoName'],
        callback?: BatchCallback
    ): Promise<PartialObjectError[]> {
        return this.s3BatchOps(
            keys.map((key) =>
                new SetObjectStorageClassOp(
                    this,
                    s3RegionId,
                    { bucket, key },
                    storageClass,
                )
            ),
            callback,
        );
    }

    restoreObjects(s3RegionId: string, bucket: string, keys: string[], days: number, callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.s3BatchOps(keys.map((key) => new RestoreObjectOp(this, s3RegionId, { bucket, key }, days)), callback);
    }

    private async s3BatchOps(objectOps: ObjectOp[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        const semaphore = new Semaphore(100);
        const ops = async (op: ObjectOp, index: number) => {
            let error: Error | undefined;
            try {
                await op.getOpPromise();
            } catch (err) {
                error = err;
            }
            if (callback?.(index, error) === false) {
                throw new Error('aborted');
            }
            return {
                error,
                ...op.getObject(),
            };
        };

        const promises: Promise<PartialObjectError>[] = objectOps.map(async (objectOp, index) => {
            const release = await semaphore.acquire();
            try {
                return await ops(objectOp, index);
            } finally {
                release();
            }
        });

        return await Promise.all(promises);
    }

    async deleteObjects(
        s3RegionId: string,
        bucket: string,
        keys: string[],
        callback?: BatchCallback,
    ): Promise<PartialObjectError[]> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(bucket),
        ]);
        const semaphore = new Semaphore(100);
        const batchCount = 100;
        const batches: string[][] = [];
        while (keys.length >= batchCount) {
            batches.push(keys.splice(0, batchCount));
        }
        if (keys.length > 0) {
            batches.push(keys);
        }
        let counter = 0;

        const promises: Promise<PartialObjectError[]>[] = batches.map(async (batch) => {
            const firstIndexInCurrentBatch = counter;
            const partialObjectErrors: PartialObjectError[] = new Array(batch.length);
            counter += batch.length;

            const release = await semaphore.acquire();
            try {
                const request = s3.deleteObjects({
                    Bucket: bucketId,
                    Delete: {
                        Objects: batch.map((key) => { return { Key: key }; }),
                    },
                });
                const results = await this.sendS3Request(
                    request,
                    'deleteObjects',
                    bucket,
                );

                let aborted = false;
                if (results.Deleted) {
                    for (const deletedObject of results.Deleted) {
                        const index = batch.findIndex((key) => key === deletedObject.Key);
                        if (index < 0) {
                            throw new Error('s3.deleteObjects deleted key which is not given');
                        }
                        if (callback && callback(index + firstIndexInCurrentBatch) === false) {
                            aborted = true;
                        }
                        partialObjectErrors[index] = { bucket, key: deletedObject.Key! };
                    }
                }
                if (results.Errors) {
                    for (const deletedObject of results.Errors) {
                        const error = new Error(deletedObject.Message);
                        const index = batch.findIndex((key) => key === deletedObject.Key);
                        if (index < 0) {
                            throw new Error('s3.deleteObjects deleted key which is not given');
                        }
                        if (callback && callback(index + firstIndexInCurrentBatch, error) === false) {
                            aborted = true;
                        }
                        partialObjectErrors[index] = { bucket, key: deletedObject.Key!, error };
                    }
                }
                if (aborted) {
                    throw new Error('aborted');
                } else {
                    return partialObjectErrors;
                }
            } catch(err: any) {
                let aborted = false;
                if (err) {
                    batch.forEach((key, index) => {
                        if (callback && callback(index + firstIndexInCurrentBatch, err) === false) {
                            aborted = true;
                        }
                        partialObjectErrors[index] = { bucket, key, error: err };
                    });
                }
                if (aborted) {
                    throw new Error('aborted');
                } else {
                    return partialObjectErrors;
                }
            } finally {
                release();
            }
        });

        const batchesResult: PartialObjectError[][] = await Promise.all(promises);
        let results: PartialObjectError[] = [];
        for (const batchResult of batchesResult) {
            results = results.concat(batchResult);
        }
        return results;
    }

    async getFrozenInfo(s3RegionId: string, object: StorageObject): Promise<FrozenInfo> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);
        const data: any = await this.sendS3Request(
            s3.headObject({ Bucket: bucketId, Key: object.key }),
            'getFrozenInfo',
            object.bucket,
            object.key,
        );

        if (!['glacier', 'deep_archive'].includes(data.StorageClass?.toLowerCase())) {
            return { status: 'Normal' };
        }

        if (!data.Restore) {
            return { status: 'Frozen' };
        }

        const restoreInfo = parseRestoreInfo(data.Restore);
        if (restoreInfo.get('ongoing-request') === 'true') {
            return { status: 'Unfreezing' };
        }

        const frozenInfo: FrozenInfo = { status: 'Unfrozen' };
        const expiryDate: string | undefined = restoreInfo.get('expiry-date');
        if (expiryDate) {
            frozenInfo.expiryDate = new Date(expiryDate);
        }
        return frozenInfo;
    }

    async restoreObject(s3RegionId: string, object: StorageObject, days: number): Promise<void> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);
        const params: AWS.S3.Types.RestoreObjectRequest = {
            Bucket: bucketId, Key: object.key,
            RestoreRequest: {
                Days: days,
                GlacierJobParameters: { Tier: 'Standard' },
            },
        };
        await this.sendS3Request(
            s3.restoreObject(params),
            'restoreObject',
            object.bucket,
            object.key,
        );
    }

    async setObjectStorageClass(
        s3RegionId: string,
        object: StorageObject,
        storageClass: StorageClass['kodoName'],
    ): Promise<void> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);

        const request = s3.copyObject({
            Bucket: bucketId, Key: object.key,
            CopySource: `/${bucketId}/${encodeURIComponent(object.key)}`,
            MetadataDirective: 'COPY',
            StorageClass: this.convertStorageClass(
                storageClass,
                'kodoName',
                's3Name',
            ),
        });
        await this.sendS3Request(request, 'setObjectStorageClass', object.bucket, object.key);
    }

    async listObjects(s3RegionId: string, bucket: string, prefix: string, option?: ListObjectsOption): Promise<ListedObjects> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(bucket),
        ]);
        const results: ListedObjects = { objects: [] };
        return await this._listS3Objects(s3RegionId, s3, bucket, bucketId, prefix, results, option);
    }

    private async _listS3Objects(
        s3RegionId: string,
        s3: AWS.S3,
        bucket: string,
        bucketId: string,
        prefix: string,
        results: ListedObjects,
        option?: ListObjectsOption,
    ): Promise<ListedObjects> {
        const newOption: ListObjectsOption = {
            delimiter: option?.delimiter,
        };
        const request = s3.listObjects({
            Bucket: bucketId,
            Delimiter: option?.delimiter,
            Marker: option?.nextContinuationToken,
            MaxKeys: option?.maxKeys,
            Prefix: prefix,
        });
        const data: any = await this.sendS3Request(request, 'listS3Objects', bucket);
        delete results.nextContinuationToken;
        if (data?.Contents.length > 0) {
            results.objects = [
                ...results.objects,
                ...data.Contents.map((object: AWS.S3.Types.Object) => ({
                    bucket,
                    key: object.Key!,
                    size: object.Size!,
                    lastModified: object.LastModified!,
                    storageClass: this.convertStorageClass(
                        object.StorageClass,
                        's3Name',
                        'kodoName',
                        'unknown',
                    ),
                })),
            ];
        }
        if (data?.CommonPrefixes.length > 0) {
            results.commonPrefixes ??= [];
            const newCommonPrefixes = data.CommonPrefixes
                .filter((newCommonPrefix: AWS.S3.Types.CommonPrefix) => {
                    const hasFoundDup = results.commonPrefixes?.some(commonPrefix => commonPrefix.key === newCommonPrefix.Prefix);
                    return !hasFoundDup;
                })
                .map((commonPrefix: AWS.S3.Types.CommonPrefix) => ({
                    bucket,
                    key: commonPrefix.Prefix!,
                }));
            results.commonPrefixes = [
                ...results.commonPrefixes,
                ...newCommonPrefixes,
            ];
        }

        results.nextContinuationToken = data.NextMarker;
        if (!data.NextMarker) {
            return results;
        }
        newOption.nextContinuationToken = data.NextMarker;
        if (!option?.minKeys) {
            return results;
        }
        let resultsSize = results.objects.length;
        if (results.commonPrefixes) {
            resultsSize += results.commonPrefixes.length;
        }
        if (resultsSize < option.minKeys) {
            newOption.minKeys = option.minKeys;
            newOption.maxKeys = option.minKeys - resultsSize;
            return await this._listS3Objects(s3RegionId, s3, bucket, bucketId, prefix, results, newOption);
        }
        return results;
    }

    async createMultipartUpload(
        s3RegionId: string,
        object: StorageObject,
        _originalFileName: string,
        header?: SetObjectHeader,
        abortSignal?: AbortSignal,
        _accelerateUploading?: boolean,
    ): Promise<InitPartsOutput> {
        const [s3, bucketId] = await Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]);
        const request = s3.createMultipartUpload({
            Bucket: bucketId,
            Key: object.key,
            Metadata: header?.metadata,
            ContentType: header?.contentType,
        });
        const data: any = await this.sendS3Request(
            request,
            'createMultipartUpload',
            object.bucket,
            object.key,
            false,
            {
                abortSignal,
            },
        );
        return { uploadId: data.UploadId! };
    }

    async uploadPart(
        s3RegionId: string,
        object: StorageObject,
        uploadId: string,
        partNumber: number,
        data: Buffer | Readable | (() => Buffer | Readable),
        option?: PutObjectOption,
    ): Promise<UploadPartOutput> {
        const [s3, bucketId] = await Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]);

        if (typeof data === 'function') {
          data = data();
        }

        // get data source for http body
        const dataSource = S3.getDataSource(
            data,
            s3.endpoint.protocol,
            option,
        );

        // get content length and content md5
        let contentLength: number;
        let contentMd5: string;
        if (data instanceof Readable) {
            if (!option?.fileStreamSetting) {
                throw new Error('s3 need fileStreamSetting when use stream');
            }
            contentLength =
                option.fileStreamSetting.end - option.fileStreamSetting.start + 1;
            contentMd5 = await this.getContentMd5(
                option.fileStreamSetting.path,
                option.fileStreamSetting.start,
                option.fileStreamSetting.end,
            );
        } else {
            contentLength = data.length;
            contentMd5 = await this.getContentMd5(data);
        }

        // get upload part params
        const params: AWS.S3.Types.UploadPartRequest = {
            Bucket: bucketId,
            Key: object.key,
            Body: dataSource,
            ContentLength: contentLength,
            ContentMD5: contentMd5,
            PartNumber: partNumber,
            UploadId: uploadId,
        };
        const uploader = s3.uploadPart(params);

        // progress callback
        if (option?.progressCallback) {
            // s3 will read all data at once,
            // so use httpUploadProgress event on request instead of data event on stream.
            uploader.on('httpUploadProgress', (progress) => {
                option.progressCallback?.(progress.loaded, progress.total);
            });
        }

        // send request
        const respond = await this.sendS3Request(
            uploader,
            'uploadPart',
            object.bucket,
            object.key,
            false,
            {
                abortSignal: option?.abortSignal,
            },
        );
        return { etag: respond.ETag! };
    }

    async completeMultipartUpload(
        s3RegionId: string,
        object: StorageObject,
        uploadId: string,
        parts: Part[],
        _originalFileName: string,
        _header?: SetObjectHeader,
        abortSignal?: AbortSignal,
        _accelerateUploading?: boolean,
    ): Promise<void> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);
        const request = s3.completeMultipartUpload({
            Bucket: bucketId,
            Key: object.key,
            UploadId: uploadId,
            MultipartUpload: {
                Parts: parts.map(part => ({
                    PartNumber: part.partNumber,
                    ETag: part.etag,
                })),
            },
        });
        await this.sendS3Request(
            request,
            'completeMultipartUpload',
            object.bucket,
            object.key,
            false,
            {
                abortSignal,
            },
        );
    }

    clearCache() {
        Object.keys(this.bucketNameToIdCache).forEach((key) => { delete this.bucketNameToIdCache[key]; });
        Object.keys(this.bucketIdToNameCache).forEach((key) => { delete this.bucketIdToNameCache[key]; });
        Object.keys(this.clients).forEach((key) => { delete this.clients[key]; });
        super.clearCache();
        this.regionService.clearCache();
    }

    protected getRequestsOption(): RequestOptions {
        return {};
    }

    /**
     * hack data source like a file stream if provide file info
     */
    private static getDataSource(
        data: Buffer | Readable,
        protocol: string,
        option?: PutObjectOption,
    ): Buffer | Readable {
        let result: Readable | Buffer;
        let reader: Readable;
        if (data instanceof Buffer) {
            const _reader = new ReadableStreamBuffer({ initialSize: data.length, chunkSize: 16 * (1 << 10) });
            _reader.put(data);
            _reader.stop();
            reader = _reader;
        } else {
            reader = data;
        }
        if (
            protocol === 'https:' ||
            (protocol === 'http:' && option?.fileStreamSetting)
        ) {
            // s3 not support non-file stream when use http protocol.
            // that make [httpUploadProgress not work as expect](https://github.com/aws/aws-sdk-js/issues/1323)
            // so hack it like a file stream.
            // ref: https://github.com/aws/aws-sdk-js/blob/v2.1015.0/lib/util.js#L730-L755
            result = reader.pipe(new PassThrough(), { end: false });
            if (protocol === 'http:' && option?.fileStreamSetting) {
                // prevent conflict with `end` property of file stream.
                if (result instanceof Writable) {
                    // @ts-ignore
                    result._end = result.end;
                    reader.on('end', () => {
                        // @ts-ignore
                        option.throttle?._end();
                    });
                }
                Object.assign(result, option.fileStreamSetting);
            }
        } else {
            result = data;
        }
        return result;
    }

    /**
     * @return result is **base64** format
     */
    protected async getContentMd5(data: Buffer): Promise<string>
    protected async getContentMd5(filePath: string, start: number, end: number): Promise<string>
    protected async getContentMd5(data: string | Buffer, start?: number, end?: number): Promise<string> {
        if (data instanceof Buffer) {
            return md5.base64(data);
        }

        const chunkStream = fs.createReadStream(data, { start, end });
        const chunkMd5 = md5.create();
        chunkStream.on('data', chunk => {
            chunkMd5.update(chunk);
        });
        return new Promise<string>((resolve, reject) => {
            chunkStream.on('error', reject);
            chunkStream.on('end', () => {
                resolve(chunkMd5.base64());
            });
        });
    }
}

interface S3ScopeCachesOptions {
    bucketNameToIdCache: Record<string, string>;
    bucketIdToNameCache: Record<string, string>;
    clients: Record<string, AWS.S3>;
    clientsLock: AsyncLock;
    listKodoBucketsPromise?: Promise<Bucket[]>;
}

class S3Scope extends S3 {
    private readonly requestStats: RequestStats;
    private readonly sdkUplogOption: SdkUplogOption;
    private readonly beginTime = new Date();

    constructor(
        sdkApiName: string,
        adapterOption: AdapterOption,
        sdkUplogOption: SdkUplogOption,
        caches: S3ScopeCachesOptions,
    ) {
        super(adapterOption);
        this.sdkUplogOption = sdkUplogOption;
        this.requestStats = {
            bytesTotalSent: 0,
            bytesTotalReceived: 0,
            sdkApiName,
            requestsCount: 0,
        };
        this.bucketNameToIdCache = caches.bucketNameToIdCache;
        this.bucketIdToNameCache = caches.bucketIdToNameCache;
        this.clients = caches.clients;
        this.clientsLock = caches.clientsLock;
        this.listKodoBucketsPromise = caches.listKodoBucketsPromise;
    }

    done(successful: boolean): Promise<void> {
        const uplogMaker = new GenSdkApiUplogEntry(this.requestStats.sdkApiName, {
            language: this.sdkUplogOption.language,
            sdkName: this.adapterOption.appName,
            sdkVersion: this.adapterOption.appVersion,
            targetBucket: this.sdkUplogOption.targetBucket,
            targetKey: this.sdkUplogOption.targetKey,
        });
        let uplog: SdkApiUplogEntry = uplogMaker.getSdkApiUplogEntry({
            costDuration: new Date().getTime() - this.beginTime.getTime(),
            bytesSent: this.requestStats.bytesTotalSent,
            bytesReceived: this.requestStats.bytesTotalReceived,
            requestsCount: this.requestStats.requestsCount,
        });
        if (!successful) {
            uplog = uplogMaker.getErrorSdkApiUplogEntry({
                errorDescription: this.requestStats.errorDescription ?? '',
                errorType: this.requestStats.errorType ?? ErrorType.UnknownError,
                requestsCount: this.requestStats.requestsCount,
            });
        }
        this.requestStats.requestsCount = 0;
        this.requestStats.errorType = undefined;
        this.requestStats.errorDescription = undefined;
        this.requestStats.bytesTotalSent = 0;
        this.requestStats.bytesTotalReceived = 0;
        return this.log(uplog);
    }

    protected getRequestsOption(): RequestOptions {
        const options = super.getRequestsOption();
        options.stats = this.requestStats;
        return options;
    }

    getRegionRequestOptions(): RegionRequestOptions {
        const options = super.getRegionRequestOptions();
        options.stats = this.requestStats;
        return options;
    }
}

function parseRestoreInfo(s: string): Map<string, string> {
    const matches = s.match(/([\w-]+)="([^"]+)"/g);
    const result = new Map<string, string>();
    if (matches) {
        matches.forEach((s) => {
            const pair = s.match(/([\w-]+)="([^"]+)"/);
            if (pair && pair.length >= 3) {
                result.set(pair[1], pair[2]);
            }
        });
    }
    return result;
}

abstract class ObjectOp {
    abstract getOpPromise(): Promise<void>;
    abstract getObject(): StorageObject;
}

class MoveObjectOp extends ObjectOp {
    constructor(private readonly s3: S3, private readonly s3RegionId: string, private readonly transferObject: TransferObject) {
        super();
    }

    getOpPromise(): Promise<void> {
        return this.s3.moveObject(this.s3RegionId, this.transferObject);
    }

    getObject(): StorageObject {
        return this.transferObject.from;
    }
}

class CopyObjectOp extends ObjectOp {
    constructor(private readonly s3: S3, private readonly s3RegionId: string, private readonly transferObject: TransferObject) {
        super();
    }

    getOpPromise(): Promise<void> {
        return this.s3.copyObject(this.s3RegionId, this.transferObject);
    }

    getObject(): StorageObject {
        return this.transferObject.from;
    }
}

class SetObjectStorageClassOp extends ObjectOp {
    constructor(
        private readonly s3: S3,
        private readonly s3RegionId: string,
        private readonly object: StorageObject,
        private readonly storageClass: StorageClass['kodoName'],
    ) {
        super();
    }

    getOpPromise(): Promise<void> {
        return this.s3.setObjectStorageClass(this.s3RegionId, this.object, this.storageClass);
    }

    getObject(): StorageObject {
        return this.object;
    }
}

class RestoreObjectOp extends ObjectOp {
    constructor(
        private readonly s3: S3,
        private readonly s3RegionId: string,
        private readonly object: StorageObject,
        private readonly days: number,
    ) {
        super();
    }

    getOpPromise(): Promise<void> {
        return this.s3.restoreObject(this.s3RegionId, this.object, this.days);
    }

    getObject(): StorageObject {
        return this.object;
    }
}

function getContentLength(r: AWS.HttpRequest | AWS.HttpResponse): number {
    let result =
        parseInt(
            r.headers['Content-Length'] ?? r.headers['content-length'],
            10
        );
    if (!Number.isNaN(result)) {
        return result;
    }
    if (typeof r.body === 'string') {
        result = Buffer.from(r.body).length;
        return result;
    }
    if (r.body instanceof Buffer) {
        result = r.body.length;
        return result;
    }
    return 0;
}
