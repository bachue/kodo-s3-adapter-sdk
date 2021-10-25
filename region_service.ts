import AsyncLock from 'async-lock';
import { RegionRequestOptions, Region } from './region';
import { AdapterOption } from './adapter';

export interface S3IdEndpoint {
    s3Id: string,
    s3Endpoint: string,
}

export type GetAllRegionsOptions = RegionRequestOptions;

export class RegionService {
    private allRegions: Region[] | undefined = undefined;
    private readonly allRegionsLock = new AsyncLock();

    constructor(private readonly adapterOption: AdapterOption) {
    }

    getAllRegions(options?: GetAllRegionsOptions): Promise<Region[]> {
        return new Promise((resolve, reject) => {
            if (this.adapterOption.regions.length > 0) {
                resolve(this.adapterOption.regions);
            } else if (this.allRegions && this.allRegions.length > 0) {
                resolve(this.allRegions);
            } else {
                this.allRegionsLock.acquire('all', (): Promise<Region[]> => {
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
                }).then((regions: Region[]) => {
                    this.allRegions = regions;
                    resolve(regions);
                }).catch(reject);
            }
        });
    }

    clearCache() {
        this.allRegions = undefined;
    }

    getS3Endpoint(s3RegionId?: string, options?: GetAllRegionsOptions): Promise<S3IdEndpoint> {
        return new Promise((resolve, reject) => {
            let queryCondition: (region: Region) => boolean;

            if (s3RegionId) {
                queryCondition = (region) => region.s3Id === s3RegionId && region.s3Urls.length > 0;
            } else {
                queryCondition = (region) => !!region.s3Id && region.s3Urls.length > 0;
            }
            const queryInRegions: (regions: Region[]) => void = (regions) => {
                const region: Region | undefined = regions.find(queryCondition);
                if (region) {
                    resolve({ s3Id: region.s3Id, s3Endpoint: region.s3Urls[0] });
                } else if (s3RegionId) {
                    reject(new Error(`Cannot find region endpoint url of ${s3RegionId}`));
                } else {
                    reject(new Error(`Cannot find valid region endpoint url`));
                }
            };

            this.getAllRegions(options).then(queryInRegions).catch(reject);
        });
    }

    fromKodoRegionIdToS3Id(regionId: string, options?: GetAllRegionsOptions): Promise<string> {
        return new Promise((resolve, reject) => {
            const queryCondition: (region: Region) => boolean = (region) => region.id === regionId;
            const queryInRegions: (regions: Region[]) => void = (regions) => {
                const region: Region | undefined = regions.find(queryCondition);
                if (region && region.s3Id) {
                    resolve(region.s3Id);
                } else {
                    reject(new Error(`Cannot find region s3 id of ${regionId}`));
                }
            };

            this.getAllRegions(options).then(queryInRegions).catch(reject);
        });
    }

    fromS3IdToKodoRegionId(s3Id: string, options?: GetAllRegionsOptions): Promise<string> {
        return new Promise((resolve, reject) => {
            const queryCondition: (region: Region) => boolean = (region) => region.s3Id === s3Id;
            const queryInRegions: (regions: Region[]) => void = (regions) => {
                const region: Region | undefined = regions.find(queryCondition);
                if (region && region.id) {
                    resolve(region.id);
                } else {
                    reject(new Error(`Cannot find region id of ${s3Id}`));
                }
            };

            this.getAllRegions(options).then(queryInRegions).catch(reject);
        });
    }
}
