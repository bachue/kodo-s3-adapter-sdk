import { expect } from 'chai';
import { Region } from '../region';

describe('Region query', () => {
    it('queries for https urls', async () => {
        const region = await Region.query({ accessKey: 'vHg2e7nOh7Jsucv2Azr5FH6omPgX22zoJRWa0FN5', bucketName: 'kodo-s3-adapter-sdk' });
        expect(region.id).to.equal('na0');
        expect(region.s3Id).to.equal('us-north-1');
        expect(region.s3Urls).to.eql(['https://s3-us-north-1.qiniucs.com/']);
    });

    it('queries for http urls', async () => {
        const region = await Region.query({ accessKey: 'vHg2e7nOh7Jsucv2Azr5FH6omPgX22zoJRWa0FN5', bucketName: 'kodo-s3-adapter-sdk', ucUrl: 'http://uc.qbox.me' });
        expect(region.id).to.equal('na0');
        expect(region.s3Id).to.equal('us-north-1');
        expect(region.s3Urls).to.eql(['http://s3-us-north-1.qiniucs.com/']);
    });
});
