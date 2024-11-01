import AsyncLock from 'async-lock';
import { Region, RegionRequestOptions } from './region';
import { AdapterOption } from './adapter';

export interface S3IdEndpoint {
    s3Id: string,
    s3Endpoint: string,
}

export type GetAllRegionsOptions = RegionRequestOptions;

const defaultCacheKey = 'DEFAULT';
const regionCache: Map<string, Region[]> = new Map<string, Region[]>();
const queryRegionLock = new AsyncLock();

export class RegionService {
    static clearCache() {
        regionCache.clear();
    }

    constructor(private readonly adapterOption: AdapterOption) {
    }

    async getAllRegions(options?: GetAllRegionsOptions): Promise<Region[]> {
        if (this.adapterOption.regions.length > 0) {
            return this.adapterOption.regions;
        }

        const cacheKey = this.adapterOption.ucUrl ?? defaultCacheKey;

        let regions = regionCache.get(cacheKey);
        const isCacheValid = regions?.every(r => r.validated);
        if (regions && isCacheValid) {
            return regions;
        }

        regions = await queryRegionLock.acquire(cacheKey, async (): Promise<Region[]> => {
            const regions = regionCache.get(cacheKey);
            const isCacheValid = regions?.every(r => r.validated);
            if (regions && isCacheValid) {
                return regions;
            }

            return await Region.getAll({
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

        regionCache.set(cacheKey, regions);
        return regions;
    }

    clearCache() {
        RegionService.clearCache();
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
