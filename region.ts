import urllib from 'urllib';

export class Region {
    upUrls: Array<string> = [];
    rsUrls: Array<string> = [];
    rsfUrls: Array<string> = [];
    apiUrls: Array<string> = [];
    s3Urls: Array<string> = [];
    constructor(readonly id: string, readonly s3Id: string) {
    }

    // accessKey: string, bucketName: string, ucUrl: string = 'https://uc.qbox.me'
    static query(options: { accessKey: string, bucketName: string, ucUrl?: string }): Promise<Region> {
        if (!options.ucUrl) {
            options.ucUrl = 'https://uc.qbox.me';
        }
        return new Promise((resolve, reject) => {
            urllib.request(`${options.ucUrl!}/v4/query`,
                { data: { ak: options.accessKey, bucket: options.bucketName }, dataAsQueryString: true, dataType: 'json' },
                (err, body) => {
                    if (err) {
                        reject(err);
                        return;
                    }

                    let r: any = null;
                    try {
                        r = body.hosts[0];
                    } catch {
                        reject(new Error('Invalid uc query v4 body'));
                        return;
                    };
                    const region: Region = new Region(r.region, r.s3.region_alias);
                    const domain2Url = (domain: any) => {
                        const url = new URL(options.ucUrl!);
                        url.host = domain;
                        return url.toString();
                    };
                    region.upUrls = r.up.domains.map(domain2Url);
                    region.rsUrls = r.rs.domains.map(domain2Url);
                    region.rsfUrls = r.rsf.domains.map(domain2Url);
                    region.apiUrls = r.api.domains.map(domain2Url);
                    region.s3Urls = r.s3.domains.map(domain2Url);
                    resolve(region);
                });
        });
    }
}

