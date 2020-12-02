import process from 'process';
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
        });
    });
});
