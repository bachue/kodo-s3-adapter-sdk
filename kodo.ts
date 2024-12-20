import AsyncLock from 'async-lock';
import os from 'os';
import fs from 'fs';
import pkg from './package.json';
import FormData from 'form-data';
import CRC32 from 'buffer-crc32';
import md5 from 'js-md5';
import { Semaphore } from 'semaphore-promise';
import { RegionRequestOptions } from './region';
import { RegionService } from './region_service';
import { URL, URLSearchParams } from 'url';
import { PassThrough, Readable, Transform } from 'stream';
import { ReadableStreamBuffer } from 'stream-buffers';
import { HttpClientResponse } from 'urllib';
import { encode as base64Encode } from 'js-base64';
import { base64ToUrlSafe, makeUploadToken, newUploadPolicy, signPrivateURL } from './kodo-auth';
import {
    Adapter,
    AdapterOption,
    BatchCallback,
    Bucket,
    BucketDetails,
    DEFAULT_STORAGE_CLASS,
    Domain,
    DomainWithoutShouldSign,
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
    UrlStyle,
} from './adapter';
import { KodoHttpClient, RequestOptions, ServiceName } from './kodo-http-client';
import { RequestStats, URLRequestOptions } from './http-client';
import { ErrorType, GenSdkApiUplogEntry, SdkApiUplogEntry, UplogEntry } from './uplog';

export const USER_AGENT = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/kodo`;

interface KodoAdapterOption {
    client: KodoHttpClient,
    regionService: RegionService,
}

export class Kodo implements Adapter {
    storageClasses: StorageClass[] = [];
    readonly client: KodoHttpClient;
    protected readonly regionService: RegionService;
    protected bucketDomainsCache: Record<string, Domain[]> = {};
    protected bucketDomainsCacheLock = new AsyncLock();

    constructor(protected adapterOption: AdapterOption, kodoAdapterOption?: KodoAdapterOption) {
        if (kodoAdapterOption) {
            this.client = kodoAdapterOption.client;
            this.regionService = kodoAdapterOption.regionService;
            return;
        }
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
        const scope = new KodoScope(
            sdkApiName,
            this.adapterOption,
            {
                client: this.client,
                regionService: this.regionService,
            },
            {
                ...enterUplogOption,
                language: this.adapterOption.appNatureLanguage,
            },
            {
                bucketDomainsCache: this.bucketDomainsCache,
                bucketDomainsCacheLock: this.bucketDomainsCacheLock,
            },
        );
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
        const bucketsQuery = {
            // get all shared buckets. can't get read-only shared buckets if miss this parameter.
            shared: 'rd',
        };

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
        const regions = await this.regionService.getAllRegions();
        const kodoRegionIdToS3RegionId = regions.reduce((m, region) => {
            m[region.id] = region.s3Id;
            return m;
        }, {} as Record<string, string>);

        return response.data.map((info: any) => {
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
                id: info.id,
                name: info.tbl,
                createDate: new Date(info.ctime * 1000),
                regionId: kodoRegionIdToS3RegionId[info.region],
                grantedPermission,
                remark: info.remark,
            };
        });
    }

    async getBucketInfo(s3RegionId: string, bucket: string): Promise<BucketDetails> {
        const getBucketInfoQuery: Record<string, string | number> = {
            bucket,
        };

        const response = await this.call({
            method: 'POST',
            serviceName: ServiceName.Uc,
            path: 'v2/bucketInfo',
            query: getBucketInfoQuery,
            dataType: 'json',
            s3RegionId,

            // for uplog
            apiName: 'queryBucket',
            targetBucket: bucket,
        });

        return response.data;
    }

    async getCdnDomains(s3RegionId: string, bucket: string): Promise<DomainWithoutShouldSign[]> {
        // DO NOT use uc service instead this to get CDN domains
        // the uc service will not filter the domains, such as wildcard domain
        const domainsQuery: Record<string, string | number> = {
            sourceTypes: 'qiniuBucket',
            sourceQiniuBucket: bucket,
            operatingState: 'success',
            limit: 50,
        };
        const response = await this.call({
            method: 'GET',
            serviceName: ServiceName.CentralApi,
            path: 'domain',
            query: domainsQuery,
            dataType: 'json',
            s3RegionId,

            // for uplog
            apiName: 'queryCdnDomains',
        });

        if (!Array.isArray(response.data.domains)) {
            return [];
        }

        return response.data.domains
            .filter((d: any) => ['normal', 'pan', 'test'].includes(d.type))
            .map((d: any) => ({
                name: d.name,
                protocol: d.protocol,
                type: 'cdn',
                apiScope: 'kodo',
            } as DomainWithoutShouldSign));
    }

    async getOriginDomains(
      s3RegionId: string,
      bucket: string,
      fallbackScheme: 'http' | 'https' = 'http',
    ): Promise<DomainWithoutShouldSign[]> {
        const domainsQuery: Record<string, string | number> = {
            bucket,
            type: 'source',
        };

        const [
            domainResult,
            certResult,
        ] = await Promise.allSettled([
            this.call({
                method: 'GET',
                serviceName: ServiceName.Uc,
                path: 'domain',
                query: domainsQuery,
                dataType: 'json',
                s3RegionId,

                // for uplog
                apiName: 'queryOriginDomain',
                targetBucket: bucket,
            }),
            this.call({
                method: 'GET',
                serviceName: ServiceName.Uc,
                path: 'cert/bindings',
                dataType: 'json',
                s3RegionId,

                // for uplog
                apiName: 'queryCert',
            }),
        ]);

        if (
            domainResult.status === 'rejected' ||
            !Array.isArray(domainResult.value.data)
        ) {
            return [];
        }

        const domainShouldHttps: Map<string, boolean> = new Map();
        if (
            certResult.status === 'fulfilled' &&
            Array.isArray(certResult.value.data)
        ) {
            certResult.value.data.forEach((cert: any) => {
                domainShouldHttps.set(cert.domain, true);
            });
        }

        return domainResult.value.data
            .map((domain: any) => ({
                name: domain.domain,
                protocol: domainShouldHttps.size
                    ? domainShouldHttps.has(domain.domain) ? 'https' : 'http'
                    : fallbackScheme,
                type: 'origin',
                apiScope: domain.api_scope === 0 ? 'kodo' : 's3',
            }));
    }

    async getDefaultDomains(s3RegionId: string, bucket: string): Promise<DomainWithoutShouldSign[]> {
        const bucketDefaultDomainQuery: Record<string, string | number> = {
            bucket,
        };
        const response = await this.call({
            method: 'GET',
            serviceName: ServiceName.Portal,
            path: 'api/kodov2/domain/default/get',
            query: bucketDefaultDomainQuery,
            dataType: 'json',
            s3RegionId,

            // for uplog
            apiName: 'queryDefaultDomain',
            targetBucket: bucket,
        });

        const typeMap: Record<string | number, DomainWithoutShouldSign['type']> = {
            0: 'cdn',
            1: 'origin',
        };
        const defaultDomainData = response.data;
        const domains: DomainWithoutShouldSign[] = [];
        if (
            defaultDomainData.domain &&
            defaultDomainData.protocol &&
            defaultDomainData.isAvailable &&
            defaultDomainData.apiScope === 0
        ) {
            domains.push({
                name: defaultDomainData.domain,
                protocol: defaultDomainData.protocol,
                type: typeMap[defaultDomainData.domainType]
                    ? typeMap[defaultDomainData.domainType]
                    : 'others',
                apiScope: defaultDomainData.apiScope === 0 ? 'kodo' : 's3',
            });
        }
        return domains;
    }

    async listDomains(s3RegionId: string, bucket: string): Promise<Domain[]> {
        const [
            bucketInfoResult,
            defaultDomainsResult,
        ] = await Promise.allSettled([
            this.getBucketInfo(s3RegionId, bucket),
            this.getDefaultDomains(s3RegionId, bucket),
        ]);

        // handle kodo share bucket
        if (
            bucketInfoResult.status === 'fulfilled' &&
            bucketInfoResult.value.perm &&
            bucketInfoResult.value.perm > 0
        ) {
            if (defaultDomainsResult.status === 'rejected') {
                return [];
            }
            return defaultDomainsResult.value
                // s3 domain is not available with kodo share bucket
                .filter(d => d.apiScope === 'kodo')
                .map(d => ({
                    ...d,
                    private: bucketInfoResult.value.private !== 0 || d.apiScope === 's3',
                    protected: bucketInfoResult.value.protected !== 0,
                }));
        }

        // handle normal bucket
        const domainsResults = await Promise.allSettled([
            this.getCdnDomains(s3RegionId, bucket),
            this.getOriginDomains(s3RegionId, bucket),
        ]);

        let result: Domain[] = [];

        domainsResults.forEach(domainsResult => {
            if (domainsResult.status === 'fulfilled') {
                result = result.concat(domainsResult.value.map(d => ({
                    ...d,
                    private: bucketInfoResult.status === 'rejected'
                        ? true
                        : bucketInfoResult.value?.private !== 0 || d.apiScope === 's3',
                    protected: bucketInfoResult.status === 'rejected'
                        ? true
                        : bucketInfoResult.value?.protected !== 0,
                })));
            }
        });

        return result;
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
        if (!response.data) {
            return [];
        }
        return response.data.map((info: any) => ({
            id: info.id,
            name: info.tbl,
        }));
    }

    async updateBucketRemark(bucket: string, remark: string): Promise<void> {
        const queryParams = {
            remark: undefined,
        };
        const bodyParams = {
            remark,
        };
        await this.call({
            method: 'PUT',
            serviceName: ServiceName.Uc,
            path: `buckets/${bucket}`,
            query: queryParams,
            contentType: 'application/json',
            dataType: 'json',
            data: JSON.stringify(bodyParams),

            // for uplog
            apiName: 'updateBucketRemark'
        });
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
        _s3RegionId: string,
        object: StorageObject,
        data: Buffer | Readable | (() => Buffer | Readable),
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

        // get content length and content crc32
        let contentLength: number;
        let crc32: string;
        if (data instanceof Readable || typeof data === 'function') {
            if (!option?.fileStreamSetting) {
                throw new Error('kodo need fileStreamSetting when use stream or getter');
            }
            contentLength = (await fs.promises.stat(option.fileStreamSetting.path)).size;
            crc32 = (await this.getContentCrc32(
                option.fileStreamSetting.path,
                option.fileStreamSetting.start,
                option.fileStreamSetting.end,
            )).toString();
        } else {
            contentLength = data.length;
            crc32 = (await this.getContentCrc32(data)).toString();
        }

        // get the form stream
        // settle the boundary for retryable
        const formForBoundary = new FormData();
        const formBoundary = formForBoundary.getBoundary();
        const contentType = formForBoundary.getHeaders()['content-type'];

        // make this to a function for http client retryable
        const putData = () => {
            const form = new FormData({
                // @ts-ignore settle the boundary for retryable
                _boundary: formBoundary,
            });
            form.append('key', object.key);
            form.append('token', token);
            if (header?.metadata) {
                for (const [metaKey, metaValue] of Object.entries(header.metadata)) {
                    form.append(`x-qn-meta-${metaKey}`, metaValue);
                }
            }
            form.append('crc32', crc32);

            // progress callback
            const fileData = typeof data === 'function'
                ? Kodo.wrapDataWithProgress(data(), contentLength, option)
                : Kodo.wrapDataWithProgress(data, contentLength, option);
            // Fix the bug of form-data lib
            // https://html.spec.whatwg.org/#multipart-form-data
            const escapeFileName = originalFileName.replace(/"/g, '%22')
                .replace(/\r/g, '%0D')
                .replace(/\n/g, '%0A');
            const fileOption: FormData.AppendOptions = {
                filename: escapeFileName,
                contentType: header?.contentType,
            };
            form.append('file', fileData, fileOption);

            // fix form not instanceof readable, causing http client not upload as stream.
            return form.pipe(new PassThrough());
        };

        // set preferred up service
        let serviceName = ServiceName.Up;
        if (option?.accelerateUploading) {
            serviceName = ServiceName.UpAcc;
        }

        await this.call({
            method: 'POST',
            serviceName,
            // spicial `bucketName` instead of `s3RegionId`,
            // to make sure query upload services by `bucketName`.
            bucketName: object.bucket,
            dataType: 'json',
            contentType: contentType,
            data: putData,
            appendAuthorization: false,
            abortSignal: option?.abortSignal,

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

        const url = await this.getObjectURL(
            s3RegionId,
            object,
            domain,
            undefined,
            option?.urlStyle,
        );
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
                abortSignal: option?.abortSignal,

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
        style: UrlStyle = UrlStyle.BucketEndpoint,
    ): Promise<URL> {
        if (!domain) {
            const domains = await this._listDomains(s3RegionId, object.bucket);
            if (domains.length === 0) {
                throw new Error('no domain found');
            }
            domain = domains[0];
        }

        if (style !== UrlStyle.BucketEndpoint) {
            throw new Error('Only support "bucketEndpoint" style for now');
        }

        let url = new URL(`${domain.protocol}://${domain.name}`);
        url.pathname = encodeURI(object.key);
        if (domain.private || domain.protected) {
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
                const results: PartialObjectError[] = response.data?.map((item: any, index: number) => {
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
                }) ?? [];
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
        const query: Record<string, string> = {
            bucket,
            prefix,
        };
        if (option?.nextContinuationToken) {
            query['marker'] = option.nextContinuationToken;
        }
        if (option?.maxKeys) {
            query['limit'] = option.maxKeys.toString();
        }
        if (option?.delimiter) {
            query['delimiter'] = option.delimiter;
        }
        const newOption: ListObjectsOption = {
            delimiter: option?.delimiter,
        };

        const response = await this.call({
            method: 'POST',
            serviceName: ServiceName.Rsf,
            path: 'list',
            s3RegionId,
            query,
            dataType: 'multijson',
            contentType: 'application/x-www-form-urlencoded',

            // for uplog
            apiName: 'listObjects',
        });

        let marker: string | undefined;
        delete results.nextContinuationToken;

        response.data?.forEach((data: { [key: string]: any; }) => {
            // add objects;
            if (data.items && data.items.length) {
                results.objects = results.objects.concat(
                    data.items.map((obj: any) => ({
                        bucket,
                        key: obj.key,
                        size: obj.fsize,
                        lastModified: new Date(obj.putTime / 10000),
                        storageClass: this.convertStorageClass(
                            obj.type,
                            'fileType',
                            'kodoName',
                            'unknown',
                        ),
                    }))
                );
            }

            // add commonPrefixes
            if (data.commonPrefixes && data.commonPrefixes.length) {
                results.commonPrefixes ??= [];
                results.commonPrefixes = results.commonPrefixes.concat(
                    data.commonPrefixes.map((dir: string) => ({
                        bucket,
                        key: dir,
                    }))
                );
            }

            // change marker
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

    async createMultipartUpload(
        s3RegionId: string,
        object: StorageObject,
        _originalFileName: string,
        _header?: SetObjectHeader,
        abortSignal?: AbortSignal,
        accelerateUploading?: boolean,
    ): Promise<InitPartsOutput> {
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

        // set preferred up service
        let serviceName = ServiceName.Up;
        if (accelerateUploading) {
            serviceName = ServiceName.UpAcc;
        }

        const response = await this.call({
            method: 'POST',
            serviceName: serviceName,
            // spicial `bucketName` instead of `s3RegionId`,
            // to make sure query upload services by `bucketName`.
            bucketName: object.bucket,
            path,
            dataType: 'json',
            s3RegionId,
            contentType: 'application/x-www-form-urlencoded',
            headers: { 'authorization': `UpToken ${token}` },
            abortSignal,

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
        _s3RegionId: string,
        object: StorageObject,
        uploadId: string,
        partNumber: number,
        data: Buffer | Readable | (() => Buffer | Readable),
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

        // get content length and content md5
        let contentLength: number;
        let contentMd5: string;
        if (data instanceof Readable || typeof data === 'function') {
            if (!option?.fileStreamSetting) {
                throw new Error('kodo need fileStreamSetting when use stream');
            }
            contentLength = option.fileStreamSetting.end - option.fileStreamSetting.start + 1;
            contentMd5 = await this.getContentMd5(
                option.fileStreamSetting.path,
                option.fileStreamSetting.start,
                option.fileStreamSetting.end,
            );
        } else {
            contentLength = data.length;
            contentMd5 = await this.getContentMd5(data);
        }

        // get the form stream
        // make this to a function for http client retryable
        const putData = () => {
            // progress callback
            if (typeof data === 'function') {
                return Kodo.wrapDataWithProgress(data(), contentLength, option);
            } else {
                return Kodo.wrapDataWithProgress(data, contentLength, option);
            }
        };
        const path = `/buckets/${object.bucket}/objects/${urlSafeBase64(object.key)}/uploads/${uploadId}/${partNumber}`;

        // set preferred up service
        let serviceName = ServiceName.Up;
        if (option?.accelerateUploading) {
            serviceName = ServiceName.UpAcc;
        }

        // send request
        const response = await this.call({
            method: 'PUT',
            serviceName: serviceName,
            // spicial `bucketName` instead of `s3RegionId`,
            // to make sure query upload services by `bucketName`.
            bucketName: object.bucket,
            path,
            data: putData,
            dataType: 'json',
            contentType: 'application/octet-stream',
            headers: {
                'authorization': `UpToken ${token}`,
                'content-md5': contentMd5,
            },
            appendAuthorization: false,
            abortSignal: option?.abortSignal,

            // for uplog
            apiName: 'uploadPart',
            targetBucket: object.bucket,
            targetKey: object.key,
        });

        return { etag: response.data.etag };
    }

    async completeMultipartUpload(
        _s3RegionId: string,
        object: StorageObject,
        uploadId: string,
        parts: Part[],
        originalFileName: string,
        header?: SetObjectHeader,
        abortSignal?: AbortSignal,
        accelerateUploading?: boolean,
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
            for (const [metaKey, metaValue] of Object.entries(header.metadata)) {
                metadata[`x-qn-meta-${metaKey}`] = metaValue;
            }
        }
        const data: any = { fname: originalFileName, parts, metadata };
        if (header?.contentType) {
            data.mimeType = header.contentType;
        }

        // set preferred up service
        let serviceName = ServiceName.Up;
        if (accelerateUploading) {
            serviceName = ServiceName.UpAcc;
        }

        await this.call({
            method: 'POST',
            serviceName: serviceName,
            // spicial `bucketName` instead of `s3RegionId`,
            // to make sure query upload services by `bucketName`.
            bucketName: object.bucket,
            path,
            data: JSON.stringify(data),
            dataType: 'json',
            headers: { 'authorization': `UpToken ${token}` },
            abortSignal,

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

    /**
     * trans data source to stream
     */
    private static wrapDataWithProgress(
        data: Buffer | Readable,
        dataLength: number,
        option?: PutObjectOption,
    ): Readable {
        let result: Readable;
        let reader: Readable;
        if (data instanceof Buffer) {
            const _reader = new ReadableStreamBuffer({ initialSize: data.length, chunkSize: 16 * (1 << 10) });
            _reader.put(data);
            _reader.stop();
            reader = _reader;
        } else {
            reader = data;
        }
        result = reader;
        if (option?.progressCallback) {
            let uploaded = 0;
            result = reader.pipe(new Transform({
                transform(chunk, _encoding, callback) {
                    uploaded += chunk.length;
                    option.progressCallback?.(uploaded, dataLength);
                    callback(null, chunk);
                },
            }));
        }
        return result;
    }

    /**
     * @return result is **hex** format
     */
    protected async getContentMd5(data: Buffer): Promise<string>
    protected async getContentMd5(filePath: string, start: number, end: number): Promise<string>
    protected async getContentMd5(data: string | Buffer, start?: number, end?: number): Promise<string> {
        if (data instanceof Buffer) {
            return md5.hex(data);
        }

        const chunkStream = fs.createReadStream(data, { start, end });
        const chunkMd5 = md5.create();
        chunkStream.on('data', chunk => {
            chunkMd5.update(chunk);
        });
        return new Promise<string>((resolve, reject) => {
            chunkStream.on('error', reject);
            chunkStream.on('end', () => {
                resolve(chunkMd5.hex());
            });
        });
    }

    protected async getContentCrc32(data: Buffer): Promise<number>
    protected async getContentCrc32(filePath: string, start: number, end: number): Promise<number>
    protected async getContentCrc32(data: string | Buffer, start?: number, end?: number): Promise<number> {
        if (data instanceof Buffer) {
            return CRC32.unsigned(data);
        }

        let result: Buffer = CRC32(Buffer.alloc(0));
        const chunkStream = fs.createReadStream(data, { start, end });
        chunkStream.on('data', chunk => {
            result = CRC32(chunk, result);
        });
        return new Promise<number>((resolve, reject) => {
            chunkStream.on('error', reject);
            chunkStream.on('end', () => {
                resolve(parseInt(result.toString('hex'), 16));
            });
        });
    }
}

interface KodoScopeCachesOptions {
    bucketDomainsCache: Record<string, Domain[]>,
    bucketDomainsCacheLock: AsyncLock,
}

class KodoScope extends Kodo {
    private readonly requestStats: RequestStats;
    private readonly sdkUplogOption: SdkUplogOption;
    private readonly beginTime = new Date();

    constructor(
        sdkApiName: string,
        adapterOption: AdapterOption,
        kodoAdapterOption: KodoAdapterOption,
        sdkUplogOption: SdkUplogOption,
        caches: KodoScopeCachesOptions,
    ) {
        super(adapterOption, kodoAdapterOption);
        this.sdkUplogOption = sdkUplogOption;
        this.requestStats = {
            sdkApiName,
            requestsCount: 0,
            bytesTotalSent: 0,
            bytesTotalReceived: 0,
        };
        this.bucketDomainsCache = caches.bucketDomainsCache;
        this.bucketDomainsCacheLock = caches.bucketDomainsCacheLock;
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
    if (key !== undefined) {
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
