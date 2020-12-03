import os from 'os';
import pkg from './package.json';
import AWS from 'aws-sdk';
import { Region } from './region';
import { Kodo } from './kodo';
import { Adapter, AdapterOption, Bucket } from './adapter';

export const USER_AGENT: string = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch()}; )/s3`;

interface S3IdEndpoint {
    s3Id: string,
    s3Endpoint: string,
}

export class S3 implements Adapter {
    private allRegions: Array<Region> | undefined = undefined;
    private readonly bucketNameToIdCache: { [name: string]: string; } = {};
    private readonly bucketIdToNameCache: { [id: string]: string; } = {};
    private readonly clients: { [key: string]: AWS.S3; } = {};
    private readonly kodo: Kodo;

    constructor(private readonly adapterOption: AdapterOption) {
        this.kodo = new Kodo(adapterOption);
    }

    private getClient(regionId?: string): Promise<AWS.S3> {
        return new Promise((resolve, reject) => {
            const cacheKey = regionId ?? '';
            if (this.clients[cacheKey]) {
                resolve(this.clients[cacheKey]);
            } else {
                let userAgent = USER_AGENT;
                if (this.adapterOption.appendedUserAgent) {
                    userAgent += `/${this.adapterOption.appendedUserAgent}`;
                }
                this.getS3Endpoint(regionId).then((s3IdEndpoint) => {
                    const s3 = new AWS.S3({
                        apiVersion: "2006-03-01",
                        customUserAgent: userAgent,
                        computeChecksums: true,
                        region: s3IdEndpoint.s3Id,
                        endpoint: s3IdEndpoint.s3Endpoint,
                        accessKeyId: this.adapterOption.accessKey,
                        secretAccessKey: this.adapterOption.secretKey,
                        // logger: console, TODO: Use Adapter Option here
                        maxRetries: 10,
                        s3ForcePathStyle: true,
                        signatureVersion: "v4",
                        useDualstack: true,
                        httpOptions: {
                            connectTimeout: 30000,
                            timeout: 300000,
                        }
                    });
                    this.clients[cacheKey] = s3;
                    resolve(s3);
                }, reject);
            }
        });
    }

    private getS3Endpoint(regionId?: string): Promise<S3IdEndpoint> {
        return new Promise((resolve, reject) => {
            let queryCondition: (region: Region) => boolean;

            if (regionId) {
                queryCondition = (region) => region.id === regionId && region.s3Urls.length > 0;
            } else {
                queryCondition = (region) => !!region.s3Id && region.s3Urls.length > 0;
            }
            const queryInRegions: (regions: Array<Region>) => void = (regions) => {
                const region: Region | undefined = regions.find(queryCondition);
                if (region) {
                    resolve({ s3Id: region.s3Id, s3Endpoint: region.s3Urls[0] });
                } else if (regionId) {
                    reject(new Error(`Cannot find region endpoint url of ${regionId}`));
                } else {
                    reject(new Error(`Cannot find valid region endpoint url`));
                }
            };

            if (this.adapterOption.regions.length > 0) {
                queryInRegions(this.adapterOption.regions);
            } else if (this.allRegions && this.allRegions.length > 0) {
                queryInRegions(this.allRegions);
            } else {
                Region.getAll({
                    accessKey: this.adapterOption.accessKey,
                    secretKey: this.adapterOption.secretKey,
                    ucUrl: this.adapterOption.ucUrl,
                }).then((regions: Array<Region>) => {
                    this.allRegions = regions;
                    queryInRegions(regions);
                }, reject);
            }
        });
    }

    private fromKodoRegionIdToS3Id(regionId: string): Promise<string> {
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
            if (this.adapterOption.regions.length > 0) {
                queryInRegions(this.adapterOption.regions);
            } else if (this.allRegions && this.allRegions.length > 0) {
                queryInRegions(this.allRegions);
            } else {
                Region.getAll({
                    accessKey: this.adapterOption.accessKey,
                    secretKey: this.adapterOption.secretKey,
                    ucUrl: this.adapterOption.ucUrl,
                }).then((regions: Array<Region>) => {
                    this.allRegions = regions;
                    queryInRegions(regions);
                }, reject);
            }
        });
    }

    private fromS3IdToKodoRegionId(s3Id: string): Promise<string> {
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
            if (this.adapterOption.regions.length > 0) {
                queryInRegions(this.adapterOption.regions);
            } else if (this.allRegions && this.allRegions.length > 0) {
                queryInRegions(this.allRegions);
            } else {
                Region.getAll({
                    accessKey: this.adapterOption.accessKey,
                    secretKey: this.adapterOption.secretKey,
                    ucUrl: this.adapterOption.ucUrl,
                }).then((regions: Array<Region>) => {
                    this.allRegions = regions;
                    queryInRegions(regions);
                }, reject);
            }
        });
    }

    private fromKodoBucketNameToS3BucketId(bucketName: string): Promise<string> {
        return new Promise((resolve, reject) => {
            if (this.bucketNameToIdCache[bucketName]) {
                resolve(this.bucketNameToIdCache[bucketName]);
            } else {
                this.kodo.listBucketIdNames().then((buckets) => {
                    buckets.forEach((bucket) => {
                        this.bucketNameToIdCache[bucket.name] = bucket.id;
                        this.bucketIdToNameCache[bucket.id] = bucket.name;
                    });
                    if (this.bucketNameToIdCache[bucketName]) {
                        resolve(this.bucketNameToIdCache[bucketName]);
                    } else {
                        reject(new Error(`Cannot find bucket id of bucket ${bucketName}`));
                    }
                }, reject);
            }
        });
    }

    private fromS3BucketIdToKodoBucketName(bucketId: string): Promise<string> {
        return new Promise((resolve, reject) => {
            if (this.bucketIdToNameCache[bucketId]) {
                resolve(this.bucketIdToNameCache[bucketId]);
            } else {
                this.kodo.listBucketIdNames().then((buckets) => {
                    buckets.forEach((bucket) => {
                        this.bucketNameToIdCache[bucket.name] = bucket.id;
                        this.bucketIdToNameCache[bucket.id] = bucket.name;
                    });
                    if (this.bucketIdToNameCache[bucketId]) {
                        resolve(this.bucketIdToNameCache[bucketId]);
                    } else {
                        reject(new Error(`Cannot find bucket name of bucket ${bucketId}`));
                    }
                }, reject);
            }
        });
    }

    createBucket(region: string, bucket: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.getClient(region).then((s3) => {
                this.fromKodoRegionIdToS3Id(region).then((s3Id) => {
                    s3.createBucket({
                        Bucket: bucket,
                        CreateBucketConfiguration: {
                            LocationConstraint: s3Id,
                        },
                    }, function(err) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve();
                        }
                    });
                }, reject);
            }, reject);
        });
    }

    deleteBucket(region: string, bucket: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.getClient(region).then((s3) => {
                this.fromKodoBucketNameToS3BucketId(bucket).then((bucketId) => {
                    s3.deleteBucket({ Bucket: bucketId }, function(err) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve();
                        }
                    });
                }, reject);
            }, reject);
        });
    }

    getBucketLocation(bucket: string): Promise<string> {
        return new Promise((resolve, reject) => {
            this.getClient().then((s3) => {
                this.fromKodoBucketNameToS3BucketId(bucket).then((bucketId) => {
                    this._getBucketLocation(s3, bucketId, resolve, reject);
                }, reject);
            }, reject);
        });
    }

    private _getBucketLocation(s3: AWS.S3, bucketId: string, resolve: any, reject: any): void {
        s3.getBucketLocation({ Bucket: bucketId }, (err, data) => {
            if (err) {
                reject(err);
            } else {
                const s3Id: string = data.LocationConstraint!;
                this.fromS3IdToKodoRegionId(s3Id).then((regionId) => {
                    resolve(regionId);
                }, reject);
            }
        });
    }

    listBuckets(): Promise<Array<Bucket>> {
        return new Promise((resolve, reject) => {
            this.getClient().then((s3) => {
                s3.listBuckets((err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        const bucketNamePromises: Array<Promise<string>> = data.Buckets!.map((info: any) => {
                            return this.fromS3BucketIdToKodoBucketName(info.Name);
                        });
                        const bucketLocationPromises: Array<Promise<string>> = data.Buckets!.map((info: any) => {
                            return new Promise((resolve, reject) => {
                                this._getBucketLocation(s3, info.Name, resolve, reject);
                            });
                        });
                        Promise.all([Promise.all(bucketNamePromises), Promise.all(bucketLocationPromises)]).then((results) => {
                            const bucketNames: Array<string> = results[0];
                            const bucketLocations: Array<string> = results[1];
                            const bucketInfos: Array<Bucket> = data.Buckets!.map((info: any, index: number) => {
                                return {
                                    id: info.Name, name: bucketNames[index],
                                    createDate: info.CreationDate,
                                    regionId: bucketLocations[index],
                                };
                            });
                            resolve(bucketInfos);
                        }, reject);
                    }
                });
            }, reject);
        });
    }
}
