const urllib = require('urllib');

class Region {
    constructor(id, s3Id) {
        return {
            id, s3Id,
            urls: {},

            get upUrls() {
                return this.urls.up || [];
            },
            get rsUrls() {
                return this.urls.rs || [];
            },
            get rsfUrls() {
                return this.urls.rsf || [];
            },
            get apiUrls() {
                return this.urls.api || [];
            },
            get s3Urls() {
                return this.urls.s3 || [];
            },
            set upUrls(urls) {
                if (!(urls instanceof Array)) {
                    urls = [urls];
                }
                return this.urls.up = urls;
            },
            set rsUrls(urls) {
                if (!(urls instanceof Array)) {
                    urls = [urls];
                }
                return this.urls.rs = urls;
            },
            set rsfUrls(urls) {
                if (!(urls instanceof Array)) {
                    urls = [urls];
                }
                return this.urls.rsf = urls;
            },
            set apiUrls(urls) {
                if (!(urls instanceof Array)) {
                    urls = [urls];
                }
                return this.urls.api = urls;
            },
            set s3Urls(urls) {
                if (!(urls instanceof Array)) {
                    urls = [urls];
                }
                return this.urls.s3 = urls;
            }
        };
    }

    static query({ accessKey, bucketName, ucUrl = 'https://uc.qbox.me' }) {
        return new Promise((resolve, reject) => {
            urllib.request(`${ucUrl}/v4/query`, { data: { ak: accessKey, bucket: bucketName }, dataAsQueryString: true, dataType: 'json' }, (err, body) => {
                if (err) {
                    reject(err);
                    return;
                }

                let r = null;
                try {
                    r = body.hosts[0];
                } catch {
                    reject(new Error('Invalid uc query v4 body'));
                    return;
                };
                const region = new Region(r.region, r.s3.region_alias);
                const domain2Url = (domain) => {
                    const url = new URL(ucUrl);
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

exports.Region = Region;
