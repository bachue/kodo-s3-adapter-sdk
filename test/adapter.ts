import process from 'process';
import urllib from 'urllib';
import { Semaphore } from 'semaphore-promise';
import { randomBytes } from 'crypto';
import { expect, assert } from 'chai';
import { Qiniu, KODO_MODE, S3_MODE } from '../qiniu';
import { TransferObject } from '../adapter';

[KODO_MODE, S3_MODE].forEach((mode: string) => {
    describe(`${mode} Adapter`, () => {
        context('bucket', () => {
            const bucketName = process.env.QINIU_TEST_BUCKET!;
            const bucketRegionId = process.env.QINIU_TEST_BUCKET_REGION_ID!;

            it('creates a bucket and drops it', async () => {
                const qiniu = new Qiniu(process.env.QINIU_ACCESS_KEY!, process.env.QINIU_SECRET_KEY!, 'http://uc.qbox.me');
                const qiniuAdapter = qiniu.mode(mode);
                const bucketName = `test-bucket-${Math.floor(Math.random() * (2**64 -1))}`;
                await qiniuAdapter.createBucket(bucketRegionId, bucketName);
                const regionId = await qiniuAdapter.getBucketLocation(bucketName);
                expect(regionId).to.equal(bucketRegionId);

                await qiniuAdapter.deleteBucket(bucketRegionId, bucketName);
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

                const bucket = buckets.find((bucket) => bucket.name === bucketName);
                expect(bucket?.regionId).to.equal(bucketRegionId);
            });

            it('uploads file', async () => {
                const qiniu = new Qiniu(process.env.QINIU_ACCESS_KEY!, process.env.QINIU_SECRET_KEY!, 'http://uc.qbox.me');
                const qiniuAdapter = qiniu.mode(mode);

                const buffer = randomBytes(1 << 12);
                const key = `4k-${Math.floor(Math.random() * (2**64 -1))}`;
                await qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key }, buffer, { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' } });

                let isExisted: boolean = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: key });
                expect(isExisted).to.equal(true);

                {
                    const url = await qiniuAdapter.getObjectURL(bucketRegionId, { bucket: bucketName, key: key }, undefined, new Date(Date.now() + 86400000));
                    expect(url.toString().includes(key)).to.equal(true);
                    const response = await urllib.request(url.toString(), { method: 'GET', streaming: true });
                    expect(response.status).to.equal(200);
                    if (mode == KODO_MODE) {
                        expect(response.headers['x-qn-meta-key-a']).to.equal('Value-A');
                        expect(response.headers['x-qn-meta-key-b']).to.equal('Value-B');
                    } else if (mode == S3_MODE) {
                        expect(response.headers['x-amz-meta-key-a']).to.equal('Value-A');
                        expect(response.headers['x-amz-meta-key-b']).to.equal('Value-B');
                    }
                    response.res.destroy();
                }

                {
                    const result = await qiniuAdapter.getObject(bucketRegionId, { bucket: bucketName, key: key });
                    expect(result.data).to.eql(buffer);
                    expect(result.header.size).to.equal(1 << 12);
                    expect(result.header.metadata['key-a']).to.equal('Value-A');
                    expect(result.header.metadata['key-b']).to.equal('Value-B');
                    expect(result.header.metadata).to.have.all.keys('key-a', 'key-b');
                }

                {
                    const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: key });
                    expect(header.size).to.equal(1 << 12);
                    expect(header.metadata['key-a']).to.equal('Value-A');
                    expect(header.metadata['key-b']).to.equal('Value-B');
                    expect(header.metadata).to.have.all.keys('key-a', 'key-b');
                }

                await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });

                isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: key });
                expect(isExisted).to.equal(false);
            });

            it('lists domain', async () => {
                const qiniu = new Qiniu(process.env.QINIU_ACCESS_KEY!, process.env.QINIU_SECRET_KEY!, 'http://uc.qbox.me');
                const qiniuAdapter = qiniu.mode(mode);

                const domains = await qiniuAdapter.listDomains(bucketRegionId, bucketName);
                if (mode === KODO_MODE) {
                    expect(domains).to.have.lengthOf(1);
                    expect(domains[0].protocol).to.equal('http');
                    expect(domains[0].private).to.equal(true);
                } else {
                    expect(domains).to.have.lengthOf(0);
                }
            });

            it('moves and copies file', async () => {
                const qiniu = new Qiniu(process.env.QINIU_ACCESS_KEY!, process.env.QINIU_SECRET_KEY!, 'http://uc.qbox.me');
                const qiniuAdapter = qiniu.mode(mode);

                const buffer = randomBytes(1 << 12);
                const key = `4k-${Math.floor(Math.random() * (2**64 -1))}`;
                await qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key }, buffer, { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' } });

                const keyCopied = `${key}-copy`;
                await qiniuAdapter.copyObject(bucketRegionId, { from: { bucket: bucketName, key: key }, to: { bucket: bucketName, key: keyCopied } });

                {
                    const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: keyCopied });
                    expect(header.size).to.equal(1 << 12);
                    expect(header.metadata['key-a']).to.equal('Value-A');
                    expect(header.metadata['key-b']).to.equal('Value-B');
                    expect(header.metadata).to.have.all.keys('key-a', 'key-b');
                }

                await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: keyCopied });

                const keyMoved = `${key}-move`;
                await qiniuAdapter.moveObject(bucketRegionId, { from: { bucket: bucketName, key: key }, to: { bucket: bucketName, key: keyMoved } });

                {
                    const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: keyMoved });
                    expect(header.size).to.equal(1 << 12);
                    expect(header.metadata['key-a']).to.equal('Value-A');
                    expect(header.metadata['key-b']).to.equal('Value-B');
                    expect(header.metadata).to.have.all.keys('key-a', 'key-b');
                }

                await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: keyMoved });

                let isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: key });
                expect(isExisted).to.equal(false);

                isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: keyCopied });
                expect(isExisted).to.equal(false);

                isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: keyMoved });
                expect(isExisted).to.equal(false);
            });

            it('moves, copies and deletes files', async () => {
                const qiniu = new Qiniu(process.env.QINIU_ACCESS_KEY!, process.env.QINIU_SECRET_KEY!, 'http://uc.qbox.me');
                const qiniuAdapter = qiniu.mode(mode);
                const semaphore = new Semaphore(5);

                const seed = Math.floor(Math.random() * (2**64 -1));
                const keys: Array<string> = new Array(250).fill('').map((_, idx: number) => `4k-${seed}-${idx}`);
                const uploadPromises = keys.map((key) => {
                    return new Promise((resolve, reject) => {
                        semaphore.acquire().then((release) => {
                            qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key }, randomBytes(10))
                                        .then(resolve, reject)
                                        .finally(() => { release(); });
                        });
                    });
                });
                await Promise.all(uploadPromises);

                {
                    const transferObjects: Array<TransferObject> = keys.map((key) => {
                        return { from: { bucket: bucketName, key: key }, to: { bucket: bucketName, key: `${key}-copy` } };
                    });

                    {
                        const indexes = new Set<number>();
                        await qiniuAdapter.copyObjects(bucketRegionId, transferObjects, (index: number, error?: Error) => {
                            indexes.add(index);
                            expect(error).to.be.undefined;
                        });
                        expect(indexes).to.have.lengthOf(250);
                    }

                    const existsResults = await Promise.all(
                        transferObjects.map((transferObject) => {
                            return new Promise((resolve, reject) => {
                                semaphore.acquire().then((release) => {
                                    qiniuAdapter.isExists(bucketRegionId, transferObject.from)
                                                .then(resolve, reject)
                                                .finally(() => { release(); });
                                });
                            });
                        }).concat(transferObjects.map((transferObject) => {
                            return new Promise((resolve, reject) => {
                                semaphore.acquire().then((release) => {
                                    qiniuAdapter.isExists(bucketRegionId, transferObject.to)
                                                .then(resolve, reject)
                                                .finally(() => { release(); });
                                });
                            });
                        })));
                    for (const existsResult of existsResults) {
                        expect(existsResult).to.equal(true);
                    }

                    await qiniuAdapter.deleteObjects(bucketRegionId, bucketName, transferObjects.map((transferObject) => transferObject.to.key));
                }

                {
                    const transferObjects: Array<TransferObject> = keys.map((key) => {
                        return { from: { bucket: bucketName, key: key }, to: { bucket: bucketName, key: `${key}-move` } };
                    });

                    {
                        const indexes = new Set<number>();
                        await qiniuAdapter.moveObjects(bucketRegionId, transferObjects, (index: number, error?: Error) => {
                            indexes.add(index);
                            expect(error).to.be.undefined;
                        });
                        expect(indexes).to.have.lengthOf(250);
                    }

                    {
                        const existsResults = await Promise.all(
                            transferObjects.map((transferObject) => {
                                return new Promise((resolve, reject) => {
                                    semaphore.acquire().then((release) => {
                                        qiniuAdapter.isExists(bucketRegionId, transferObject.from)
                                                    .then(resolve, reject)
                                                    .finally(() => { release(); });
                                    });
                                });
                            })
                        );
                        for (const existsResult of existsResults) {
                            expect(existsResult).to.equal(false);
                        }
                    }

                    {
                        const existsResults = await Promise.all(
                            transferObjects.map((transferObject) => {
                                return new Promise((resolve, reject) => {
                                    semaphore.acquire().then((release) => {
                                        qiniuAdapter.isExists(bucketRegionId, transferObject.to)
                                                    .then(resolve, reject)
                                                    .finally(() => { release(); });
                                    });
                                });
                            })
                        );
                        for (const existsResult of existsResults) {
                            expect(existsResult).to.equal(true);
                        }
                    }

                    await qiniuAdapter.deleteObjects(bucketRegionId, bucketName, transferObjects.map((transferObject) => transferObject.to.key));
                }
            });
        });
    });
});
