import AsyncLock from 'async-lock';
import { Region, RegionRequestOptions, RegionWithStorageClasses } from './region';
import { AdapterOption } from './adapter';

export interface S3IdEndpoint {
    s3Id: string,
    s3Endpoint: string,
}

export type GetAllRegionsOptions = RegionRequestOptions;

export class RegionService {
    private allRegions: Region[] | undefined = undefined;
    private allRegionsStorageClass: RegionWithStorageClasses[] | undefined = undefined;
    private readonly allRegionsLock = new AsyncLock();

    constructor(private readonly adapterOption: AdapterOption) {
    }

    async getAllRegions(options?: GetAllRegionsOptions): Promise<Region[]> {
        if (this.adapterOption.regions.length > 0) {
            return this.adapterOption.regions;
        }
        if (this.allRegions && this.allRegions.length > 0) {
            return this.allRegions;
        }

        const regions = await this.allRegionsLock.acquire('all', (): Promise<Region[]> => {
            if (this.allRegions && this.allRegions.length > 0) {
                return Promise.resolve(this.allRegions);
            }
            return Region.getAll({
                accessKey: this.adapterOption.accessKey,
                secretKey: this.adapterOption.secretKey,
                ucUrl: this.adapterOption.ucUrl,
                timeout: options?.timeout,
                retry: options?.retry,
                retryDelay: options?.retryDelay,
                appName: this.adapterOption.appName,
                appVersion: this.adapterOption.appVersion,
                uplogBufferSize: this.adapterOption.uplogBufferSize,
                requestCallback: this.adapterOption.requestCallback,
                responseCallback: this.adapterOption.responseCallback,
                stats: options?.stats,
            });
        });

        this.allRegions = regions;
        return regions;
    }

    async getAllRegionsStorageClasses(options?: GetAllRegionsOptions): Promise<RegionWithStorageClasses[]> {
        if (this.allRegionsStorageClass && this.allRegionsStorageClass.length > 0) {
            return this.allRegionsStorageClass;
        }

        this.allRegionsStorageClass = await this.allRegionsLock.acquire('allStorageClasses', (): Promise<RegionWithStorageClasses[]> => {
            if (this.allRegionsStorageClass && this.allRegionsStorageClass.length > 0) {
                return Promise.resolve(this.allRegionsStorageClass);
            }

            return Region.getAllRegionsStorageClasses({
                accessKey: this.adapterOption.accessKey,
                secretKey: this.adapterOption.secretKey,
                ucUrl: this.adapterOption.ucUrl,
                timeout: options?.timeout,
                retry: options?.retry,
                retryDelay: options?.retryDelay,
                appName: this.adapterOption.appName,
                appVersion: this.adapterOption.appVersion,
                uplogBufferSize: this.adapterOption.uplogBufferSize,
                requestCallback: this.adapterOption.requestCallback,
                responseCallback: this.adapterOption.responseCallback,
                stats: options?.stats,
            });
        });

        return this.allRegionsStorageClass;
    }

    clearCache() {
        this.allRegions = undefined;
        this.allRegionsStorageClass = undefined;
    }

    async getS3Endpoint(s3RegionId?: string, options?: GetAllRegionsOptions): Promise<S3IdEndpoint> {
        let queryCondition: (region: Region) => boolean;

        if (s3RegionId) {
            queryCondition = (region) => region.s3Id === s3RegionId && region.s3Urls.length > 0;
        } else {
            queryCondition = (region) => !!region.s3Id && region.s3Urls.length > 0;
        }

        const regions = await this.getAllRegions(options);

        const region: Region | undefined = regions.find(queryCondition);

        if (!region) {
            if (s3RegionId) {
                throw new Error(`Cannot find region endpoint url of ${s3RegionId}`);
            } else {
                throw new Error('Cannot find valid region endpoint url');
            }
        }

        return { s3Id: region.s3Id, s3Endpoint: region.s3Urls[0] };
    }

    async fromKodoRegionIdToS3Id(regionId: string, options?: GetAllRegionsOptions): Promise<string> {
        const regions = await this.getAllRegions(options);

        const queryCondition: (region: Region) => boolean = (region) => region.id === regionId;

        const region: Region | undefined = regions.find(queryCondition);
        if (!region?.s3Id) {
            throw new Error(`Cannot find region s3 id of ${regionId}`);
        }

        return region.s3Id;
    }

    async fromS3IdToKodoRegionId(s3Id: string, options?: GetAllRegionsOptions): Promise<string> {
        const regions = await this.getAllRegions(options);

        const queryCondition: (region: Region) => boolean = (region) => region.s3Id === s3Id;

        const region: Region | undefined = regions.find(queryCondition);

        if (!region?.id) {
            throw new Error(`Cannot find region id of ${s3Id}`);
        }

        return region.id;
    }
}
