import AsyncLock from 'async-lock';
import { Region } from './region';
import { AdapterOption } from './adapter';

export interface S3IdEndpoint {
    s3Id: string,
    s3Endpoint: string,
}

export class RegionService {
    private allRegions: Array<Region> | undefined = undefined;
    private readonly allRegionsLock = new AsyncLock();

    constructor(private readonly adapterOption: AdapterOption) {
    }

    getAllRegions(): Promise<Array<Region>> {
        return new Promise((resolve, reject) => {
            if (this.adapterOption.regions.length > 0) {
                resolve(this.adapterOption.regions);
            } else if (this.allRegions && this.allRegions.length > 0) {
                resolve(this.allRegions);
            } else {
                this.allRegionsLock.acquire('all', (): Promise<Array<Region>> => {
                    if (this.allRegions && this.allRegions.length > 0) {
                        return Promise.resolve(this.allRegions);
                    }
                    return Region.getAll({
                        accessKey: this.adapterOption.accessKey,
                        secretKey: this.adapterOption.secretKey,
                        ucUrl: this.adapterOption.ucUrl,
                    });
                }).then((regions: Array<Region>) => {
                    this.allRegions = regions;
                    resolve(regions);
                }, reject);
            }
        });
    }

    getS3Endpoint(s3RegionId?: string): Promise<S3IdEndpoint> {
        return new Promise((resolve, reject) => {
            let queryCondition: (region: Region) => boolean;

            if (s3RegionId) {
                queryCondition = (region) => region.s3Id === s3RegionId && region.s3Urls.length > 0;
            } else {
                queryCondition = (region) => !!region.s3Id && region.s3Urls.length > 0;
            }
            const queryInRegions: (regions: Array<Region>) => void = (regions) => {
                const region: Region | undefined = regions.find(queryCondition);
                if (region) {
                    resolve({ s3Id: region.s3Id, s3Endpoint: region.s3Urls[0] });
                } else if (s3RegionId) {
                    reject(new Error(`Cannot find region endpoint url of ${s3RegionId}`));
                } else {
                    reject(new Error(`Cannot find valid region endpoint url`));
                }
            };

            this.getAllRegions().then(queryInRegions, reject);
        });
    }

    fromKodoRegionIdToS3Id(regionId: string): Promise<string> {
        return new Promise((resolve, reject) => {
            const queryCondition: (region: Region) => boolean = (region) => region.id === regionId;
            const queryInRegions: (regions: Array<Region>) => void = (regions) => {
                const region: Region | undefined = regions.find(queryCondition);
                if (region && region.s3Id) {
                    resolve(region.s3Id);
                } else {
                    reject(new Error(`Cannot find region s3 id of ${regionId}`));
                }
            };

            this.getAllRegions().then(queryInRegions, reject);
        });
    }

    fromS3IdToKodoRegionId(s3Id: string): Promise<string> {
        return new Promise((resolve, reject) => {
            const queryCondition: (region: Region) => boolean = (region) => region.s3Id === s3Id;
            const queryInRegions: (regions: Array<Region>) => void = (regions) => {
                const region: Region | undefined = regions.find(queryCondition);
                if (region && region.id) {
                    resolve(region.id);
                } else {
                    reject(new Error(`Cannot find region id of ${s3Id}`));
                }
            };

            this.getAllRegions().then(queryInRegions, reject);
        });
    }
}
