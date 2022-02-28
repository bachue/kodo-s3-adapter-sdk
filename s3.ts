import dns from 'dns';
import AsyncLock from 'async-lock';
import AWS from 'aws-sdk';
import os from 'os';
import pkg from './package.json';
import md5 from 'js-md5';
import { URL } from 'url';
import { Readable } from 'stream';
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
import { RequestStats } from './http-client';
import { RegionRequestOptions } from './region';
import { generateReqId } from './req_id';

export const USER_AGENT = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/s3`;

interface RequestOptions {
    stats?: RequestStats,
}

export class S3 extends Kodo {
    private readonly bucketNameToIdCache: { [name: string]: string; } = {};
    private readonly bucketIdToNameCache: { [id: string]: string; } = {};
    private readonly clients: { [key: string]: AWS.S3; } = {};
    private readonly bucketNameToIdCacheLock = new AsyncLock();
    private readonly clientsLock = new AsyncLock();

    private async getClient(s3RegionId?: string): Promise<AWS.S3> {
        const cacheKey = s3RegionId ?? '';
        if (this.clients[cacheKey]) {
            return this.clients[cacheKey];
        }

        const client = await this.clientsLock.acquire(cacheKey, async (): Promise<AWS.S3> => {
            let userAgent = USER_AGENT;
            if (this.adapterOption.appendedUserAgent) {
                userAgent += `/${this.adapterOption.appendedUserAgent}`;
            }
            const s3IdEndpoint = await this.regionService.getS3Endpoint(s3RegionId, this.getRegionRequestOptions());
            return new AWS.S3({
                apiVersion: '2006-03-01',
                customUserAgent: userAgent,
                computeChecksums: true,
                region: s3IdEndpoint.s3Id,
                endpoint: s3IdEndpoint.s3Endpoint,
                accessKeyId: this.adapterOption.accessKey,
                secretAccessKey: this.adapterOption.secretKey,
                maxRetries: 10,
                s3ForcePathStyle: true,
                signatureVersion: 'v4',
                useDualstack: true,
                httpOptions: {
                    connectTimeout: 30000,
                    timeout: 300000,
                }
            });
        });
        this.clients[cacheKey] = client;
        return client;
    }

    async fromKodoBucketNameToS3BucketId(bucketName: string): Promise<string> {
        if (this.bucketNameToIdCache[bucketName]) {
            return this.bucketNameToIdCache[bucketName];
        }
        await this.bucketNameToIdCacheLock.acquire('all', async (): Promise<void> => {
            if (this.bucketNameToIdCache[bucketName]) {
                return;
            }
            const buckets = await super.listBucketIdNames();
            buckets.forEach((bucket) => {
                this.bucketNameToIdCache[bucket.name] = bucket.id;
                this.bucketIdToNameCache[bucket.id] = bucket.name;
            });
            return;
        });
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
        await this.bucketNameToIdCacheLock.acquire('all', async (): Promise<void> => {
            if (this.bucketIdToNameCache[bucketId]) {
                return;
            }
            const buckets = await super.listBucketIdNames();
            buckets.forEach((bucket) => {
                this.bucketNameToIdCache[bucket.name] = bucket.id;
                this.bucketIdToNameCache[bucket.id] = bucket.name;
            });
            return;
        });

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
        const scope = new S3Scope(sdkApiName, this.adapterOption, {
            ...enterUplogOption,
            language: this.adapterOption.appNatureLanguage,
        });

        try {
            const data = await f(scope, scope.getRegionRequestOptions());
            await scope.done(true).catch(() => null); // ignore error
            return data;
        } catch (err) {
            await scope.done(false).catch(() => null); // ignore error
            throw err;
        }
    }

    private async sendS3Request<D, E>(
        request: AWS.Request<D, E>,
        apiName: string,
        bucketName?: string,
        key?: string,
    ): Promise<D> {
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

        const options = this.getRequestsOption();
        const remoteAddress = await new Promise<string>(resolve => {
            dns.lookup(request.httpRequest.endpoint.hostname, (err, address) => {
                if (err) {
                    resolve('');
                    return;
                }
                resolve(address);
            });
        });

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
        });

        return await new Promise<D>((resolve, reject) => {
            request.send((err, data) => {
                this.log(uplog).finally(() => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(data);
                    }
                });
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

    async listBuckets(): Promise<Bucket[]> {
        const s3 = await this.getClient();
        const data = await this.sendS3Request(s3.listBuckets(), 'listBuckets');

        const bucketNamePromises: Promise<string>[] = data.Buckets!.map((info: any) => {
            return this.fromS3BucketIdToKodoBucketName(info.Name);
        });
        const bucketLocationPromises: Promise<string | undefined>[] = data.Buckets!.map(async (info: any) => {
            return await this._getBucketLocation(s3, info.Name).catch(() => undefined);
        });
        const [bucketNames, bucketLocations] = await Promise.all([
            Promise.all(bucketNamePromises),
            Promise.all(bucketLocationPromises),
        ]);

        return data.Buckets!.map((info: any, index: number) => ({
            id: info.Name,
            name: bucketNames[index],
            createDate: info.CreationDate,
            regionId: bucketLocations[index],
        }));
    }

    async listDomains(_s3RegionId: string, _bucket: string): Promise<Domain[]> {
        return [];
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
        data: Buffer,
        originalFileName: string,
        header?: SetObjectHeader,
        option?: PutObjectOption,
    ): Promise<void> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.fromKodoBucketNameToS3BucketId(object.bucket),
        ]);

        let dataSource: Readable | Buffer;
        if (this.adapterOption.ucUrl?.startsWith('https://') ?? true) {
            const reader = new ReadableStreamBuffer({ initialSize: data.length, chunkSize: 1 << 20 });
            reader.put(data);
            reader.stop();
            if (option?.throttle) {
                dataSource = reader.pipe(option.throttle);
            } else {
                dataSource = reader;
            }
        } else {
            dataSource = data;
        }
        const params: AWS.S3.Types.PutObjectRequest = {
            Bucket: bucketId,
            Key: object.key,
            Body: dataSource,
            ContentLength: data.length,
            ContentType: header?.contentType,
            ContentMD5: md5.base64(data),
            Metadata: header?.metadata,
            ContentDisposition: makeContentDisposition(originalFileName),
            StorageClass: this.convertStorageClass(
                object.storageClassName,
                'kodoName',
                's3Name',
            ),
        };
        const uploader = s3.putObject(params);
        if (option?.progressCallback) {
            uploader.on('httpUploadProgress', (progress) => {
                option.progressCallback!(progress.loaded, progress.total);
            });
        }

        await this.sendS3Request(uploader, 'putObject', object.bucket, object.key);
    }

    async getObject(
        s3RegionId: string,
        object: StorageObject,
        _domain?: Domain
    ): Promise<ObjectGetResult> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
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
        _domain?: Domain,
        option?: GetObjectStreamOption,
    ): Promise<Readable> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
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
        return request.createReadStream();
    }

    async getObjectURL(s3RegionId: string, object: StorageObject, _domain?: Domain, deadline?: Date): Promise<URL> {
        const [s3, bucketId] = await Promise.all([
            this.getClient(s3RegionId),
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
        };
    }

    async moveObject(s3RegionId: string, transferObject: TransferObject): Promise<void> {
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
        const [s3, storageClass, fromBucketId, toBucketId] = await Promise.all([
            this.getClient(s3RegionId),
            this.getObjectStorageClass(s3RegionId, transferObject.from),
            this.fromKodoBucketNameToS3BucketId(transferObject.from.bucket),
            this.fromKodoBucketNameToS3BucketId(transferObject.to.bucket),
        ]);
        const params: AWS.S3.Types.CopyObjectRequest = {
            Bucket: toBucketId, Key: transferObject.to.key,
            CopySource: `/${fromBucketId}/${encodeURIComponent(transferObject.from.key)}`,
            MetadataDirective: 'COPY',
            StorageClass: storageClass,
        };
        await this.sendS3Request(s3.copyObject(params), 'copyObject', transferObject.from.bucket, transferObject.from.key);
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
        originalFileName: string,
        header?: SetObjectHeader,
    ): Promise<InitPartsOutput> {
        const [s3, bucketId] = await Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]);
        const request = s3.createMultipartUpload({
            Bucket: bucketId,
            Key: object.key,
            Metadata: header?.metadata,
            ContentDisposition: makeContentDisposition(originalFileName),
            ContentType: header?.contentType,
        });
        const data: any = await this.sendS3Request(request, 'createMultipartUpload', object.bucket, object.key);
        return { uploadId: data.UploadId! };
    }

    async uploadPart(
        s3RegionId: string,
        object: StorageObject,
        uploadId: string,
        partNumber: number,
        data: Buffer,
        option?: PutObjectOption,
    ): Promise<UploadPartOutput> {
        const [s3, bucketId] = await Promise.all([this.getClient(s3RegionId), this.fromKodoBucketNameToS3BucketId(object.bucket)]);
        let dataSource: Readable | Buffer;
        if (this.adapterOption.ucUrl?.startsWith('https://') ?? true) {
            const reader = new ReadableStreamBuffer({
                initialSize: data.length, chunkSize: 1 << 20,
            });
            reader.put(data);
            reader.stop();
            if (option?.throttle) {
                dataSource = reader.pipe(option.throttle);
            } else {
                dataSource = reader;
            }
        } else {
            dataSource = data;
        }
        const params: AWS.S3.Types.UploadPartRequest = {
            Bucket: bucketId,
            Key: object.key,
            Body: dataSource,
            ContentLength: data.length,
            ContentMD5: md5.base64(data),
            PartNumber: partNumber, UploadId: uploadId,
        };
        const uploader = s3.uploadPart(params);
        if (option?.progressCallback) {
            uploader.on('httpUploadProgress', (progress) => {
                option.progressCallback!(progress.loaded, progress.total);
            });
        }
        const respond: any = await this.sendS3Request(uploader, 'uploadPart', object.bucket, object.key);
        return { etag: respond.ETag! };
    }

    async completeMultipartUpload(
        s3RegionId: string,
        object: StorageObject,
        uploadId: string,
        parts: Part[],
        _originalFileName: string,
        _header?: SetObjectHeader,
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
        await this.sendS3Request(request, 'completeMultipartUpload', object.bucket, object.key);
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
}

class S3Scope extends S3 {
    private readonly requestStats: RequestStats;
    private readonly sdkUplogOption: SdkUplogOption;
    private readonly beginTime = new Date();

    constructor(
        sdkApiName: string,
        adapterOption: AdapterOption,
        sdkUplogOption: SdkUplogOption,
    ) {
        super(adapterOption);
        this.sdkUplogOption = sdkUplogOption;
        this.requestStats = {
            bytesTotalSent: 0,
            bytesTotalReceived: 0,
            sdkApiName,
            requestsCount: 0,
        };
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

function makeContentDisposition(originalFileName: string): string {
    return `attachment; filename*=utf-8''${encodeURIComponent(originalFileName)}`;
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
