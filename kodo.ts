import AsyncLock from 'async-lock';
import os from 'os';
import pkg from './package.json';
import FormData from 'form-data';
import CRC32 from 'buffer-crc32';
import md5 from 'js-md5';
import { Semaphore } from 'semaphore-promise';
import { RegionRequestOptions } from './region';
import { RegionService } from './region_service';
import { URL, URLSearchParams } from 'url';
import { Readable } from 'stream';
import { HttpClientResponse } from 'urllib';
import { encode as base64Encode } from 'js-base64';
import { base64ToUrlSafe, makeUploadToken, newUploadPolicy, signPrivateURL } from './kodo-auth';
import {
    Adapter,
    AdapterOption,
    BatchCallback,
    Bucket,
    DEFAULT_STORAGE_CLASS,
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
    SdkUplogOption,
    SetObjectHeader,
    StorageClass,
    StorageObject,
    TransferObject,
    UploadPartOutput,
} from './adapter';
import { KodoHttpClient, RequestOptions, ServiceName } from './kodo-http-client';
import { RequestStats, URLRequestOptions } from './http-client';
import { GenSdkApiUplogEntry, UplogEntry, ErrorType, SdkApiUplogEntry } from './uplog';

export const USER_AGENT = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/kodo`;

export class Kodo implements Adapter {
    storageClasses: StorageClass[] = [];
    protected readonly client: KodoHttpClient;
    protected readonly regionService: RegionService;
    private readonly bucketDomainsCache: { [bucketName: string]: Domain[]; } = {};
    private readonly bucketDomainsCacheLock = new AsyncLock();

    constructor(protected adapterOption: AdapterOption) {
        let userAgent: string = USER_AGENT;
        if (adapterOption.appendedUserAgent) {
            userAgent += `/${adapterOption.appendedUserAgent}`;
        }
        this.client = new KodoHttpClient({
            accessKey: adapterOption.accessKey,
            secretKey: adapterOption.secretKey,
            ucUrl: adapterOption.ucUrl,
            regions: adapterOption.regions,
            appendedUserAgent: adapterOption.appendedUserAgent,
            appName: adapterOption.appName,
            appVersion: adapterOption.appVersion,
            appNatureLanguage: adapterOption.appNatureLanguage,
            uplogBufferSize: adapterOption.uplogBufferSize,
            userAgent,
            timeout: [30000, 300000],
            retry: 10,
            retryDelay: 500,
            requestCallback: adapterOption.requestCallback,
            responseCallback: adapterOption.responseCallback,

            // for uplog
            apiType: 'kodo',
        });
        this.regionService = new RegionService(adapterOption);
    }

    convertStorageClass<
        T extends keyof StorageClass,
        K extends keyof StorageClass,
    >(
        from: StorageClass[K] | undefined,
        fromFormat: K,
        targetFormat: T,
        fallbackValue: StorageClass[T] = DEFAULT_STORAGE_CLASS[targetFormat],
    ): StorageClass[T] {
        if (from === undefined) {
            return fallbackValue;
        }
        const storageClass = this.storageClasses.find(
            storageClass => storageClass[fromFormat] === from
        );
        return storageClass?.[targetFormat] ?? fallbackValue;
    }

    async enter<T>(
        sdkApiName: string,
        f: (scope: Adapter, options: RegionRequestOptions) => Promise<T>,
        enterUplogOption?: EnterUplogOption,
    ): Promise<T> {
        const scope = new KodoScope(sdkApiName, this.adapterOption, {
            ...enterUplogOption,
            language: this.adapterOption.appNatureLanguage,
        });
        try {
            const data = await f(scope, scope.getRegionRequestOptions());
            await scope.done(true);
            return data;
        } catch (err) {
            await scope.done(false);
            throw err;
        }
    }

    async createBucket(s3RegionId: string, bucket: string): Promise<void> {
        const kodoRegionId = await this.regionService.fromS3IdToKodoRegionId(s3RegionId, this.getRegionRequestOptions());
        await this.call({
            method: 'POST',
            serviceName: ServiceName.Uc,
            path: `mkbucketv3/${bucket}/region/${kodoRegionId}`,

            // for uplog
            apiName: 'createBucket',
            targetBucket: bucket,
        });
    }

    async deleteBucket(_region: string, bucket: string): Promise<void> {
        await this.call({
            method: 'POST',
            serviceName: ServiceName.Uc,
            bucketName: bucket,
            path: `drop/${bucket}`,

            // for uplog
            apiName: 'deleteBucket',
            targetBucket: bucket,
        });
    }

    async getBucketLocation(bucket: string): Promise<string> {
        const response = await this.call({
            method: 'GET',
            serviceName: ServiceName.Uc,
            bucketName: bucket,
            path: `bucket/${bucket}`,
            dataType: 'json',

            // for uplog
            apiName: 'getBucketLocation',
            targetBucket: bucket,
        });
        return await this.regionService.fromKodoRegionIdToS3Id(
            response.data.region,
            this.getRegionRequestOptions(),
        );
    }

    async listBuckets(): Promise<Bucket[]> {
        const bucketsQuery = new URLSearchParams();
        bucketsQuery.set('shared', 'rd');

        const response = await this.call({
            method: 'GET',
            serviceName: ServiceName.Uc,
            path: 'v2/buckets',
            dataType: 'json',
            query: bucketsQuery,

            // for uplog
            apiName: 'listBuckets',
        });
        if (!response.data) {
            return [];
        }
        const regionsPromises: Promise<string | undefined>[] = response.data.map((info: any) => (
            this.regionService
                .fromKodoRegionIdToS3Id(
                    info.region,
                    this.getRegionRequestOptions(),
                )
                .catch(() => Promise.resolve())
        ));
        const regionsInfo = await Promise.all(regionsPromises);
        return response.data.map((info: any, index: number) => {
            let grantedPermission: string | undefined;
            switch (info.perm) {
                case 1:
                    grantedPermission = 'readonly';
                    break;
                case 2:
                    grantedPermission = 'readwrite';
                    break;
            }
            return {
                id: info.id, name: info.tbl,
                createDate: new Date(info.ctime * 1000),
                regionId: regionsInfo[index],
                grantedPermission,
            };
        });
    }

    async listDomains(s3RegionId: string, bucket: string): Promise<Domain[]> {
        const domainsQuery = new URLSearchParams();
        domainsQuery.set('sourceTypes', 'qiniuBucket');
        domainsQuery.set('sourceQiniuBucket', bucket);
        domainsQuery.set('operatingState', 'success');
        domainsQuery.set('limit', '50');

        const getBucketInfoQuery = new URLSearchParams();
        getBucketInfoQuery.set('bucket', bucket);

        const bucketDefaultDomainQuery = new URLSearchParams();
        bucketDefaultDomainQuery.set('bucket', bucket);

        const promises = [
            this.call({
                method: 'GET',
                serviceName: ServiceName.Qcdn,
                path: 'domain',
                query: domainsQuery,
                dataType: 'json',
                s3RegionId,

                // for uplog
                apiName: 'queryDomain',
                targetBucket: bucket,
            }),
            this.call({
                method: 'POST',
                serviceName: ServiceName.Uc,
                path: 'v2/bucketInfo',
                query: getBucketInfoQuery,
                dataType: 'json',
                s3RegionId,

                // for uplog
                apiName: 'queryBucket',
                targetBucket: bucket,
            }),
            this.call({
                method: 'GET',
                serviceName: ServiceName.Portal,
                path: 'api/kodov2/domain/default/get',
                query: bucketDefaultDomainQuery,
                dataType: 'json',
                s3RegionId,

                // for uplog
                apiName: 'queryDefaultDomain',
                targetBucket: bucket,
            }),
        ];

        const [
            domainResponse,
            bucketResponse,
            defaultDomainQuery,
        ] = await Promise.all(promises);

        if (bucketResponse.data.perm && bucketResponse.data.perm > 0) {
            const result = defaultDomainQuery.data;
            const domains: Domain[] = [];
            if (result.domain && result.protocol) {
                domains.push({
                    name: result.domain, protocol: result.protocol,
                    type: 'normal', private: bucketResponse.data.private != 0,
                });
            }
            return domains;
        }

        return domainResponse.data.domains
            .filter((domain: any) => {
                switch (domain.type) {
                    case 'normal':
                    case 'pan':
                    case 'test':
                        return true;
                    default:
                        return false;
                }
            })
            .map((domain: any) => ({
                name: domain.name,
                protocol: domain.protocol,
                type: domain.type,
                private: bucketResponse.data.private != 0,
            }));
    }

    private async _listDomains(s3RegionId: string, bucket: string): Promise<Domain[]> {
        if (this.bucketDomainsCache[bucket]) {
            return this.bucketDomainsCache[bucket];
        }

        const domains = await this.bucketDomainsCacheLock.acquire(bucket, async (): Promise<Domain[]> => {
            if (this.bucketDomainsCache[bucket]) {
                return this.bucketDomainsCache[bucket];
            }
            return await this.listDomains(s3RegionId, bucket);
        });
        this.bucketDomainsCache[bucket] = domains;
        return domains;
    }

    async listBucketIdNames(): Promise<BucketIdName[]> {
        const response = await this.call({
            method: 'GET',
            serviceName: ServiceName.Uc,
            path: 'v2/buckets',
            dataType: 'json',

            // for uplog
            apiName: 'listBucketIdNames',
        });
        return response.data.map((info: any) => ({
            id: info.id,
            name: info.tbl,
        }));
    }

    async isExists(s3RegionId: string, object: StorageObject): Promise<boolean> {
        try {
            await this.getObjectInfo(s3RegionId, object);
            return true;
        } catch (err) {
            if (err.message === 'no such file or directory') {
                return false;
            }
            throw err;
        }
    }

    async deleteObject(s3RegionId: string, object: StorageObject): Promise<void> {
        await this.call({
            method: 'POST',
            serviceName: ServiceName.Rs,
            path: `delete/${encodeObject(object)}`,
            dataType: 'json',
            s3RegionId,
            contentType: 'application/x-www-form-urlencoded',

            // for uplog
            apiName: 'deleteObject',
            targetBucket: object.bucket,
            targetKey: object.key,
        });
    }

    async putObject(
        s3RegionId: string,
        object: StorageObject,
        data: Buffer,
        originalFileName: string,
        header?: SetObjectHeader,
        option?: PutObjectOption,
    ): Promise<void> {
        const token = makeUploadToken(
            this.adapterOption.accessKey,
            this.adapterOption.secretKey,
            newUploadPolicy({
                bucket: object.bucket,
                key: object.key,
                fileType: object?.storageClassName
                    ? this.convertStorageClass(
                        object.storageClassName,
                        'kodoName',
                        'fileType',
                    )
                    : undefined,
            }),
        );

        const form = new FormData();
        form.append('key', object.key);
        form.append('token', token);
        if (header?.metadata) {
            for (const [metaKey, metaValue] of Object.entries(header.metadata)) {
                form.append(`x-qn-meta-${metaKey}`, metaValue);
            }
        }
        if (option?.crc32) {
            form.append('crc32', option.crc32);
        } else {
            form.append('crc32', CRC32.unsigned(data));
        }

        const fileOption: FormData.AppendOptions = {
            filename: originalFileName,
        };
        fileOption.contentType = header?.contentType;
        form.append('file', data, fileOption);

        await this.call({
            method: 'POST',
            serviceName: ServiceName.Up,
            dataType: 'json',
            s3RegionId,
            contentType: form.getHeaders()['content-type'],
            form,
            uploadProgress: option?.progressCallback,
            uploadThrottle: option?.throttle,
            appendAuthorization: false,

            // for uplog
            apiName: 'putObject',
            targetBucket: object.bucket,
            targetKey: object.key,
        });
    }

    async getObject(
        s3RegionId: string,
        object: StorageObject,
        domain?: Domain,
    ): Promise<ObjectGetResult> {
        const url = await this.getObjectURL(s3RegionId, object, domain);
        const response: HttpClientResponse<Buffer> = await this.callUrl(
            [
                url.toString(),
            ],
            {
                fullUrl: true,
                appendAuthorization: false,
                method: 'GET',

                // for uplog
                apiName: 'getObject',
                targetBucket: object.bucket,
                targetKey: object.key,
            },
        );
        return {
            data: response.data,
            header: getObjectHeader(response),
        };
    }

    async getObjectStream(
        s3RegionId: string,
        object: StorageObject,
        domain?: Domain,
        option?: GetObjectStreamOption,
    ): Promise<Readable> {
        const headers: { [headerName: string]: string; } = {};
        if (option?.rangeStart || option?.rangeEnd) {
            headers.Range = `bytes=${option?.rangeStart ?? ''}-${option?.rangeEnd ?? ''}`;
        }

        const url = await this.getObjectURL(s3RegionId, object, domain);
        const response = await this.callUrl(
            [
                url.toString(),
            ],
            {
                fullUrl: true,
                appendAuthorization: false,
                method: 'GET',
                headers,
                streaming: true,

                // for uplog
                apiName: 'getObjectStream',
                targetBucket: object.bucket,
                targetKey: object.key,
            },
        );

        return response.res;
    }

    async getObjectURL(
        s3RegionId: string,
        object: StorageObject,
        domain?: Domain,
        deadline?: Date,
    ): Promise<URL> {
        if (!domain) {
            let domains = await this._listDomains(s3RegionId, object.bucket);
            if (domains.length === 0) {
                throw new Error('no domain found');
            }
            const domainTypeScope = (domain: Domain): number => {
                switch (domain.type) {
                    case 'normal':
                        return 1;
                    case 'pan':
                        return 2;
                    case 'test':
                        return 3;
                }
            };
            domains = domains.sort((domain1, domain2) => domainTypeScope(domain1) - domainTypeScope(domain2));

            domain = domains[0];
        }

        let url = new URL(`${domain.protocol}://${domain.name}`);
        url.pathname = object.key;
        if (domain.private) {
            url = signPrivateURL(this.adapterOption.accessKey, this.adapterOption.secretKey, url, deadline);
        }
        return url;
    }

    async getObjectInfo(s3RegionId: string, object: StorageObject): Promise<ObjectInfo> {
        const response = await this.call({
            method: 'GET',
            serviceName: ServiceName.Rs,
            path: `stat/${encodeObject(object)}`,
            dataType: 'json',
            s3RegionId,
            contentType: 'application/x-www-form-urlencoded',

            // for uplog
            apiName: 'getObjectInfo',
            targetBucket: object.bucket,
            targetKey: object.key,
        });
        return {
            bucket: object.bucket,
            key: response.data.key,
            size: response.data.fsize,
            lastModified: new Date(response.data.putTime / 10000),
            storageClass: this.convertStorageClass(
                response.data.type,
                'fileType',
                'kodoName',
                'unknown',
            ),
        };
    }

    async getObjectHeader(s3RegionId: string, object: StorageObject, domain?: Domain): Promise<ObjectHeader> {
        const url = await this.getObjectURL(s3RegionId, object, domain);
        const response = await this.callUrl<Buffer>(
            [
                url.toString(),
            ],
            {
                fullUrl: true,
                appendAuthorization: false,
                method: 'HEAD',

                // for uplog
                apiName: 'getObjectHeader',
                targetBucket: object.bucket,
                targetKey: object.key,
            },
        );
        return getObjectHeader(response);
    }

    async moveObject(s3RegionId: string, transferObject: TransferObject): Promise<void> {
        await this.call({
            method: 'POST',
            serviceName: ServiceName.Rs,
            path: `move/${encodeObject(transferObject.from)}/${encodeObject(transferObject.to)}/force/true`,
            dataType: 'json',
            s3RegionId,
            contentType: 'application/x-www-form-urlencoded',

            // for uplog
            apiName: 'moveObject',
            targetBucket: transferObject.from.bucket,
            targetKey: transferObject.from.key,
        });
    }

    async copyObject(s3RegionId: string, transferObject: TransferObject): Promise<void> {
        await this.call({
            method: 'POST',
            serviceName: ServiceName.Rs,
            path: `copy/${encodeObject(transferObject.from)}/${encodeObject(transferObject.to)}/force/true`,
            dataType: 'json',
            s3RegionId,
            contentType: 'application/x-www-form-urlencoded',

            // for uplog
            apiName: 'copyObject',
            targetBucket: transferObject.from.bucket,
            targetKey: transferObject.from.key,
        });
    }

    moveObjects(s3RegionId: string, transferObjects: TransferObject[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.batchOps(
            'moveObjects',
            transferObjects.map((to) => new MoveObjectOp(to)),
            100,
            s3RegionId,
            callback,
        );
    }

    copyObjects(s3RegionId: string, transferObjects: TransferObject[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.batchOps(
            'copyObjects',
            transferObjects.map((to) => new CopyObjectOp(to)),
            100,
            s3RegionId,
            callback
        );
    }

    deleteObjects(s3RegionId: string, bucket: string, keys: string[], callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.batchOps(
            'deleteObjects',
            keys.map((key) => new DeleteObjectOp({ bucket, key })),
            100,
            s3RegionId,
            callback,
        );
    }

    setObjectsStorageClass(
        s3RegionId: string,
        bucket: string,
        keys: string[],
        storageClass: StorageClass['kodoName'],
        callback?: BatchCallback
    ): Promise<PartialObjectError[]> {
        return this.batchOps(
            'setObjectsStorageClass',
            keys.map((key) => new SetObjectStorageClassOp(
                {
                    bucket,
                    key,
                },
                this.convertStorageClass(storageClass, 'kodoName', 'fileType'),
            )),
            100,
            s3RegionId,
            callback,
        );
    }

    restoreObjects(s3RegionId: string, bucket: string, keys: string[], days: number, callback?: BatchCallback): Promise<PartialObjectError[]> {
        return this.batchOps('restoreObjects', keys.map((key) => new RestoreObjectsOp({ bucket, key }, days)), 100, s3RegionId, callback);
    }

    private async batchOps(
        requestApiName: string,
        ops: ObjectOp[],
        batchCount: number,
        s3RegionId: string,
        callback?: BatchCallback
    ): Promise<PartialObjectError[]> {
        const semaphore = new Semaphore(20);
        const opsBatches: ObjectOp[][] = [];

        while (ops.length >= batchCount) {
            const batch: ObjectOp[] = ops.splice(0, batchCount);
            opsBatches.push(batch);
        }
        if (ops.length > 0) {
            opsBatches.push(ops);
        }

        let counter = 0;
        const promises: Promise<PartialObjectError[]>[] = opsBatches.map(async (batch) => {
            const firstIndexInCurrentBatch = counter;
            counter += batch.length;

            const params = new URLSearchParams();
            for (const op of batch) {
                params.append('op', op.getOp());
            }

            const release = await semaphore.acquire();
            try {
                const response = await this.call({
                    method: 'POST',
                    serviceName: ServiceName.Rs,
                    path: 'batch',
                    dataType: 'json',
                    s3RegionId,
                    contentType: 'application/x-www-form-urlencoded',
                    data: params.toString(),

                    // for uplog
                    apiName: requestApiName,
                    targetBucket: batch[0]?.getObject().bucket
                });
                let aborted = false;
                const results: PartialObjectError[] = response.data.map((item: any, index: number) => {
                    const currentIndex = firstIndexInCurrentBatch + index;
                    const result: PartialObjectError = batch[index].getObject();
                    let error: Error | undefined;
                    if (item?.data?.error) {
                        error = new Error(item?.data?.error);
                        result.error = error;
                    }
                    if (callback && callback(currentIndex, error) === false) {
                        aborted = true;
                    }
                    return result;
                });
                if (aborted) {
                    throw new Error('aborted');
                }
                return results;
            } catch (error) {
                let aborted = false;
                const results: PartialObjectError[] = batch.map((op, index) => {
                    const currentIndex = firstIndexInCurrentBatch + index;
                    if (callback && callback(currentIndex, error) === false) {
                        aborted = true;
                    }
                    return Object.assign({}, op.getObject());
                });
                if (aborted) {
                    throw new Error('aborted');
                }
                return results;
            } finally {
                release();
            }
        });

        const batches = await Promise.all<PartialObjectError[]>(promises);
        let results: PartialObjectError[] = [];
        for (const batch of batches) {
            results = results.concat(batch);
        }
        return results;
    }

    async getFrozenInfo(s3RegionId: string, object: StorageObject): Promise<FrozenInfo> {
        const response = await this.call({
            method: 'POST',
            serviceName: ServiceName.Rs,
            path: `stat/${encodeObject(object)}`,
            dataType: 'json',
            s3RegionId,
            contentType: 'application/x-www-form-urlencoded',

            // for uplog
            apiName: 'getFrozenInfo',
            targetBucket: object.bucket,
            targetKey: object.key,
        });

        // 2 archive, 3 deep archive
        if (![2, 3].includes(response.data.type)) {
            return {
                status: 'Normal',
            };
        }
        if (!response.data.restoreStatus) {
            return {
                status: 'Frozen',
            };
        }
        if (response.data.restoreStatus === 1) {
            return {
                status: 'Unfreezing',
            };
        }
        return {
            status: 'Unfrozen',
            expiryDate: !isNaN(response.data.restoreExpiration)
                ? new Date(response.data.restoreExpiration * 1000)
                : undefined,
        };
    }

    async restoreObject(s3RegionId: string, object: StorageObject, days: number): Promise<void> {
        await this.call({
            method: 'POST',
            serviceName: ServiceName.Rs,
            path: `restoreAr/${encodeObject(object)}/freezeAfterDays/${days}`,
            dataType: 'json',
            s3RegionId,
            contentType: 'application/x-www-form-urlencoded',

            // for uplog
            apiName: 'restoreObject',
            targetBucket: object.bucket,
            targetKey: object.key,
        });
    }

    async setObjectStorageClass(
        s3RegionId: string,
        object: StorageObject,
        storageClass: StorageClass['kodoName'],
    ): Promise<void> {
        const fileType = this.convertStorageClass(
            storageClass,
            'kodoName',
            'fileType',
        );
        await this.call({
            method: 'POST',
            serviceName: ServiceName.Rs,
            path: `chtype/${encodeObject(object)}/type/${fileType}`,
            dataType: 'json',
            s3RegionId,
            contentType: 'application/x-www-form-urlencoded',

            // for uplog
            apiName: 'setObjectStorageClass',
            targetBucket: object.bucket,
            targetKey: object.key,
        });
    }

    listObjects(s3RegionId: string, bucket: string, prefix: string, option?: ListObjectsOption): Promise<ListedObjects> {
        const results: ListedObjects = { objects: [] };
        return this._listObjects(s3RegionId, bucket, prefix, results, option);
    }

    private async _listObjects(
        s3RegionId: string,
        bucket: string,
        prefix: string,
        results: ListedObjects,
        option?: ListObjectsOption,
    ): Promise<ListedObjects> {
        const query = new URLSearchParams();
        query.set('bucket', bucket);
        query.set('prefix', prefix);
        if (option?.nextContinuationToken) {
            query.set('marker', option.nextContinuationToken);
        }
        if (option?.maxKeys) {
            query.set('limit', option.maxKeys.toString());
        }
        if (option?.delimiter) {
            query.set('delimiter', option.delimiter);
        }
        const newOption: ListObjectsOption = {
            delimiter: option?.delimiter,
        };

        const response = await this.call({
            method: 'POST',
            serviceName: ServiceName.Rsf,
            path: 'v2/list',
            s3RegionId,
            query,
            dataType: 'multijson',
            contentType: 'application/x-www-form-urlencoded',

            // for uplog
            apiName: 'listObjects',
        });

        let marker: string | undefined;
        delete results.nextContinuationToken;

        response.data.forEach((data: { [key: string]: any; }) => {
            if (data.item) {
                results.objects.push({
                    bucket,
                    key: data.item.key,
                    size: data.item.fsize,
                    lastModified: new Date(data.item.putTime / 10000),
                    storageClass: this.convertStorageClass(
                        data.item.type,
                        'fileType',
                        'kodoName',
                        'unknown',
                    ),
                });
            } else if (data.dir) {
                results.commonPrefixes ??= [];
                const foundDup = results.commonPrefixes.some(
                    commonPrefix => commonPrefix.key === data.dir
                );
                if (!foundDup) {
                    results.commonPrefixes.push({
                        bucket,
                        key: data.dir,
                    });
                }
            }
            marker = data.marker;
        });

        if (marker) {
            newOption.nextContinuationToken = marker;
            results.nextContinuationToken = marker;
            if (option?.minKeys) {
                let resultsSize = results.objects.length;
                if (results.commonPrefixes) {
                    resultsSize += results.commonPrefixes.length;
                }
                if (resultsSize < option.minKeys) {
                    newOption.minKeys = option.minKeys;
                    newOption.maxKeys = option.minKeys - resultsSize;
                    return await this._listObjects(s3RegionId, bucket, prefix, results, newOption);
                }
            }
        }
        return results;
    }

    async createMultipartUpload(s3RegionId: string, object: StorageObject, _originalFileName: string, _header?: SetObjectHeader): Promise<InitPartsOutput> {
        const token = makeUploadToken(
            this.adapterOption.accessKey,
            this.adapterOption.secretKey,
            newUploadPolicy({
                bucket: object.bucket,
                key: object.key,
                fileType: object?.storageClassName
                    ? this.convertStorageClass(
                        object.storageClassName,
                        'kodoName',
                        'fileType',
                    )
                    : undefined,
            }),
        );
        const path = `/buckets/${object.bucket}/objects/${urlSafeBase64(object.key)}/uploads`;

        const response = await this.call({
            method: 'POST',
            serviceName: ServiceName.Up,
            path,
            dataType: 'json',
            s3RegionId,
            contentType: 'application/x-www-form-urlencoded',
            headers: { 'authorization': `UpToken ${token}` },

            // for uplog
            apiName: 'createMultipartUpload',
            targetBucket: object.bucket,
            targetKey: object.key,
        });
        return {
            uploadId: response.data.uploadId,
        };
    }

    async uploadPart(
        s3RegionId: string,
        object: StorageObject,
        uploadId: string,
        partNumber: number,
        data: Buffer,
        option?: PutObjectOption,
    ): Promise<UploadPartOutput> {
        const token = makeUploadToken(
            this.adapterOption.accessKey,
            this.adapterOption.secretKey,
            newUploadPolicy({
                bucket: object.bucket,
                key: object.key,
                fileType: object?.storageClassName
                    ? this.convertStorageClass(
                        object.storageClassName,
                        'kodoName',
                        'fileType',
                    )
                    : undefined,
            }),
        );
        const path = `/buckets/${object.bucket}/objects/${urlSafeBase64(object.key)}/uploads/${uploadId}/${partNumber}`;

        const response = await this.call({
            method: 'PUT',
            serviceName: ServiceName.Up,
            path,
            data,
            dataType: 'json',
            s3RegionId,
            contentType: 'application/octet-stream',
            headers: {
                'authorization': `UpToken ${token}`,
                'content-md5': md5.hex(data),
            },
            uploadProgress: option?.progressCallback,
            uploadThrottle: option?.throttle,
            appendAuthorization: false,

            // for uplog
            apiName: 'uploadPart',
            targetBucket: object.bucket,
            targetKey: object.key,
        });

        return { etag: response.data.etag };
    }

    async completeMultipartUpload(
        s3RegionId: string,
        object: StorageObject,
        uploadId: string,
        parts: Part[],
        originalFileName: string,
        header?: SetObjectHeader,
    ): Promise<void> {
        const token = makeUploadToken(
            this.adapterOption.accessKey,
            this.adapterOption.secretKey,
            newUploadPolicy({
                bucket: object.bucket,
                key: object.key,
                fileType: this.convertStorageClass(
                    object.storageClassName,
                    'kodoName',
                    'fileType',
                ),
            }),
        );
        const path = `/buckets/${object.bucket}/objects/${urlSafeBase64(object.key)}/uploads/${uploadId}`;
        const metadata: { [metaKey: string]: string; } = {};
        if (header?.metadata) {
            for (const [metaKey, metaValue] of Object.entries(header!.metadata)) {
                metadata[`x-qn-meta-${metaKey}`] = metaValue;
            }
        }
        const data: any = { fname: originalFileName, parts, metadata };
        if (header?.contentType) {
            data.mimeType = header!.contentType;
        }

        await this.call({
            method: 'POST',
            serviceName: ServiceName.Up,
            path,
            data: JSON.stringify(data),
            dataType: 'json',
            s3RegionId,
            headers: { 'authorization': `UpToken ${token}` },

            // for uplog
            apiName: 'completeMultipartUpload',
            targetBucket: object.bucket,
            targetKey: object.key,
        });
    }

    clearCache() {
        Object.keys(this.bucketDomainsCache).forEach((key) => {
            delete this.bucketDomainsCache[key];
        });
        this.client.clearCache();
        this.regionService.clearCache();
    }

    protected call<T = any>(options: RequestOptions): Promise<HttpClientResponse<T>> {
        return this.client.call(options);
    }

    protected callUrl<T = any>(urls: string[], options: URLRequestOptions): Promise<HttpClientResponse<T>> {
        return this.client.callUrls(urls, options);
    }

    protected getRegionRequestOptions(): RegionRequestOptions {
        return {
            timeout: [30000, 300000],
            retry: 10,
            retryDelay: 500,
        };
    }

    protected log(entry: UplogEntry): Promise<void> {
        return this.client.log(entry);
    }
}

class KodoScope extends Kodo {
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
            sdkApiName,
            requestsCount: 0,
            bytesTotalSent: 0,
            bytesTotalReceived: 0,
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

    protected call<T = any>(options: RequestOptions): Promise<HttpClientResponse<T>> {
        options.stats = this.requestStats;
        return super.call(options);
    }

    protected callUrl<T = any>(urls: string[], options: URLRequestOptions): Promise<HttpClientResponse<T>> {
        options.stats = this.requestStats;
        return super.callUrl(urls, options);
    }

    getRegionRequestOptions(): RegionRequestOptions {
        const options = super.getRegionRequestOptions();
        options.stats = this.requestStats;
        return options;
    }
}

function encodeObject(object: StorageObject): string {
    return encodeBucketKey(object.bucket, object.key);
}

function encodeBucketKey(bucket: string, key?: string): string {
    let data: string = bucket;
    if (key) {
        data += `:${key}`;
    }
    return urlSafeBase64(data);
}

function urlSafeBase64(data: string): string {
    return base64ToUrlSafe(base64Encode(data));
}

function getObjectHeader(response: HttpClientResponse<Buffer>): ObjectHeader {
    const size: number = parseInt(response.headers['content-length']! as string);
    const contentType: string = response.headers['content-type']! as string;
    const lastModified: Date = new Date(response.headers['last-modified']! as string);
    const metadata: { [key: string]: string; } = {};
    for (const [metaKey, metaValue] of Object.entries(response.headers)) {
        if (metaKey?.startsWith('x-qn-meta-')) {
            metadata[metaKey.substring('x-qn-meta-'.length) as string] = (metaValue as string);
        }
    }
    return { size, contentType, lastModified, metadata };
}


export interface BucketIdName {
    id: string;
    name: string;
}

abstract class ObjectOp {
    abstract getObject(): StorageObject;

    abstract getOp(): string;
}

class MoveObjectOp extends ObjectOp {
    constructor(private readonly object: TransferObject) {
        super();
    }

    getObject(): StorageObject {
        return this.object.from;
    }

    getOp(): string {
        return `/move/${encodeObject(this.object.from)}/${encodeObject(this.object.to)}/force/true`;
    }
}

class CopyObjectOp extends ObjectOp {
    constructor(private readonly object: TransferObject) {
        super();
    }

    getObject(): StorageObject {
        return this.object.from;
    }

    getOp(): string {
        return `/copy/${encodeObject(this.object.from)}/${encodeObject(this.object.to)}/force/true`;
    }
}

class DeleteObjectOp extends ObjectOp {
    constructor(private readonly object: StorageObject) {
        super();
    }

    getObject(): StorageObject {
        return this.object;
    }

    getOp(): string {
        return `/delete/${encodeObject(this.object)}`;
    }
}

class SetObjectStorageClassOp extends ObjectOp {
    constructor(
        private readonly object: StorageObject,
        private readonly fileType: StorageClass['fileType'],
    ) {
        super();
    }

    getObject(): StorageObject {
        return this.object;
    }

    getOp(): string {
        return `chtype/${encodeObject(this.object)}/type/${this.fileType}`;
    }
}

class RestoreObjectsOp extends ObjectOp {
    constructor(private readonly object: StorageObject, private readonly days: number) {
        super();
    }

    getObject(): StorageObject {
        return this.object;
    }

    getOp(): string {
        return `restoreAr/${encodeObject(this.object)}/freezeAfterDays/${this.days}`;
    }
}
