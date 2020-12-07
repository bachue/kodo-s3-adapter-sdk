import process from 'process';
import urllib from 'urllib';
import { randomBytes } from 'crypto';
import { expect, assert } from 'chai';
import { Qiniu, KODO_MODE, S3_MODE } from '../qiniu';

[KODO_MODE, S3_MODE].forEach((mode: string) => {
    describe(`${mode} Adapter`, () => {
        context('bucket', () => {
            it('creates a bucket and drops it', async () => {
                const qiniu = new Qiniu(process.env.QINIU_ACCESS_KEY!, process.env.QINIU_SECRET_KEY!, 'http://uc.qbox.me');
                const qiniuAdapter = qiniu.mode(mode);
                const bucketName = `test-bucket-${Math.floor(Math.random() * (2**64 -1))}`;
                await qiniuAdapter.createBucket('z1', bucketName);
                const regionId = await qiniuAdapter.getBucketLocation(bucketName);
                expect(regionId).to.equal('z1');

                await qiniuAdapter.deleteBucket('z1', bucketName);
                try {
                    await qiniuAdapter.getBucketLocation(bucketName)
                    assert.fail();
                } catch {
                }
            });

            it('lists all buckets', async () => {
                const qiniu = new Qiniu(process.env.QINIU_ACCESS_KEY!, process.env.QINIU_SECRET_KEY!, 'http://uc.qbox.me');
                const qiniuAdapter = qiniu.mode(mode);

                const buckets = await qiniuAdapter.listBuckets();
                let bucket = buckets.find((bucket) => bucket.name === 'phpsdk');
                expect(bucket?.id).not.to.equal('phpsdk');

                bucket = buckets.find((bucket) => bucket.name === 'kodo-s3-adapter-sdk');
                expect(bucket?.regionId).to.equal('na0');
            });

            it('uploads file', async () => {
                const qiniu = new Qiniu(process.env.QINIU_ACCESS_KEY!, process.env.QINIU_SECRET_KEY!, 'http://uc.qbox.me');
                const qiniuAdapter = qiniu.mode(mode);

                const buffer = randomBytes(1 << 22);
                const key = `4m-${Math.floor(Math.random() * (2**64 -1))}`;
                await qiniuAdapter.putObject('na0', { bucket: 'kodo-s3-adapter-sdk', key: key }, buffer);

                let isExisted: boolean = await qiniuAdapter.isExists('na0', { bucket: 'kodo-s3-adapter-sdk', key: key });
                expect(isExisted).to.equal(true);

                const url = await qiniuAdapter.getObjectURL('na0', { bucket: 'kodo-s3-adapter-sdk', key: key }, undefined, new Date(Date.now() + 86400000));
                expect(url.toString().includes(key)).to.equal(true);

                const response = await urllib.request(url.toString(), { method: 'GET', streaming: true });
                expect(response.status).to.equal(200);

                const result = await qiniuAdapter.getObject('na0', { bucket: 'kodo-s3-adapter-sdk', key: key });
                expect(result.data).to.eql(buffer);
                expect(result.header.size).to.equal(1 << 22);

                await qiniuAdapter.deleteObject('na0', { bucket: 'kodo-s3-adapter-sdk', key: key });

                isExisted = await qiniuAdapter.isExists('na0', { bucket: 'kodo-s3-adapter-sdk', key: key });
                expect(isExisted).to.equal(false);
            });

            it('lists domain', async () => {
                const qiniu = new Qiniu(process.env.QINIU_ACCESS_KEY!, process.env.QINIU_SECRET_KEY!, 'http://uc.qbox.me');
                const qiniuAdapter = qiniu.mode(mode);

                const domains = await qiniuAdapter.listDomains('na0', 'kodo-s3-adapter-sdk');
                if (mode === KODO_MODE) {
                    expect(domains).to.have.lengthOf(1);
                    expect(domains[0].protocol).to.equal('http');
                    expect(domains[0].private).to.equal(true);
                } else {
                    expect(domains).to.have.lengthOf(0);
                }
            });
        });
    });
});
