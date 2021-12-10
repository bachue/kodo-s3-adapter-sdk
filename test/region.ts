import process from 'process';
import fs from 'fs';
import { expect, assert } from 'chai';
import { Region } from '../region';

process.on('uncaughtException', (err: any, origin: any) => {
    fs.writeSync(
        process.stderr.fd,
        `Caught exception: ${err}\n` +
        `Exception origin: ${origin}`
    );
    assert.fail();
});

describe('Region', () => {
    context('Region query', () => {
        it('queries for https urls', async () => {
            const region = await Region.query({
                accessKey: process.env.QINIU_ACCESS_KEY!,
                bucketName: process.env.QINIU_TEST_BUCKET!,
                appName: 'fakeAppName',
                appVersion: 'fakeAppVersion',
            });
            expect(region.id).to.equal('na0');
            expect(region.s3Id).to.equal('us-north-1');
            expect(region.s3Urls).to.eql(['https://s3-us-north-1.qiniucs.com/']);
        });

        it('queries for http urls', async () => {
            const region = await Region.query({
                accessKey: process.env.QINIU_ACCESS_KEY!,
                bucketName: process.env.QINIU_TEST_BUCKET!,
                ucUrl: 'http://uc.qbox.me',
                appName: 'fakeAppName',
                appVersion: 'fakeAppVersion',
            });
            expect(region.id).to.equal('na0');
            expect(region.s3Id).to.equal('us-north-1');
            expect(region.s3Urls).to.eql(['http://s3-us-north-1.qiniucs.com/']);
        });
    });

    context('Region getAll', () => {
        it('get for https urls', async () => {
            const regions = await Region.getAll({
                accessKey: process.env.QINIU_ACCESS_KEY!,
                secretKey: process.env.QINIU_SECRET_KEY!,
                appName: 'fakeAppName',
                appVersion: 'fakeAppVersion',
            });
            expect(regions.map((r) => r.id)).to.have.members(['z0', 'z1', 'z2', 'as0', 'na0', 'fog-cn-east-1', 'cn-east-2']);
            regions.forEach((r) => {
                expect(r.label).not.to.be.empty;
                expect(r.translatedLabels!['zh_CN']).not.to.be.empty;
                expect(r.translatedLabels!['ja_JP']).not.to.be.empty;
            });
        });
    });
});
