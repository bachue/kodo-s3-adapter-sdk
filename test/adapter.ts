import process from 'process';
import urllib from 'urllib';
import tempfile from 'tempfile';
import fs from 'fs';
import { Semaphore } from 'semaphore-promise';
import { randomBytes } from 'crypto';
import { expect, assert } from 'chai';
import { Qiniu, KODO_MODE, S3_MODE } from '../qiniu';
import { TransferObject, FrozenStatus } from '../adapter';
import { Uploader } from '../uploader';
import { Kodo } from '../kodo';

process.on('uncaughtException', (err: any, origin: any) => {
    fs.writeSync(
        process.stderr.fd,
        `Caught exception: ${err}\n` +
        `Exception origin: ${origin}`
    );
    assert.fail();
});

[KODO_MODE, S3_MODE].forEach((mode: string) => {
    describe(`${mode} Adapter`, () => {
        const bucketName = process.env.QINIU_TEST_BUCKET!;
        const bucketRegionId = process.env.QINIU_TEST_BUCKET_REGION_ID!;
        const accessKey = process.env.QINIU_ACCESS_KEY!;
        const secretKey = process.env.QINIU_SECRET_KEY!;

        context('objects operation', () => {
            it('moves and copies object', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
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

            it('moves, copies and deletes objects', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                const semaphore = new Semaphore(5);

                const seed = Math.floor(Math.random() * (2**64 -1));
                const keys: Array<string> = new Array(250).fill('').map((_, idx: number) => `10b-${seed}-${idx}`);
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

            it('unfreeze objects', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const kodo = new Kodo({ accessKey: accessKey, secretKey: secretKey, regions: [] });
                const qiniuAdapter = qiniu.mode(mode);

                const buffer = randomBytes(1 << 12);
                const key = `4k-${Math.floor(Math.random() * (2**64 -1))}`;
                await qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key }, buffer);

                let frozenInfo = await kodo.getFrozenInfo(bucketRegionId, { bucket: bucketName, key: key });
                expect(frozenInfo.status).to.equal(FrozenStatus.Normal);

                await kodo.freeze(bucketRegionId, { bucket: bucketName, key: key });
                frozenInfo = await kodo.getFrozenInfo(bucketRegionId, { bucket: bucketName, key: key });
                expect(frozenInfo.status).to.equal(FrozenStatus.Frozen);

                await kodo.unfreeze(bucketRegionId, { bucket: bucketName, key: key }, 1);
                frozenInfo = await kodo.getFrozenInfo(bucketRegionId, { bucket: bucketName, key: key });
                expect(frozenInfo.status).to.equal(FrozenStatus.Unfreezing);

                await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
            });

            it('list objects', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                const semaphore = new Semaphore(5);

                const seed = Math.floor(Math.random() * (2**64 -1));
                let keys: Array<string> = [`10b-${seed}/`];
                keys = keys.concat(new Array(250).fill('').map((_, idx: number) => {
                    let path = keys[0];
                    const idxParts = idx.toString().split('');
                    path += idxParts.join('/');
                    if (idxParts.length < 3) {
                        path += '/';
                    }
                    return path;
                }));

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

                let listedObjects = await qiniuAdapter.listObjects(bucketRegionId, bucketName, keys[0], { minKeys: 100, maxKeys: 20 });
                expect(listedObjects.objects).to.have.lengthOf(100);

                listedObjects = await qiniuAdapter.listObjects(bucketRegionId, bucketName, keys[0], { minKeys: 110, maxKeys: 20 });
                expect(listedObjects.objects).to.have.lengthOf(110);

                listedObjects = await qiniuAdapter.listObjects(bucketRegionId, bucketName, keys[0], { minKeys: 250, maxKeys: 20, delimiter: '/' });
                expect(listedObjects.objects).to.have.lengthOf(1);
                expect(listedObjects.commonPrefixes).to.have.lengthOf(10);

                listedObjects = await qiniuAdapter.listObjects(bucketRegionId, bucketName, `${keys[0]}1/`, { minKeys: 250, maxKeys: 20, delimiter: '/' });
                expect(listedObjects.objects).to.have.lengthOf(1);
                expect(listedObjects.commonPrefixes).to.have.lengthOf(10);

                listedObjects = await qiniuAdapter.listObjects(bucketRegionId, bucketName, `${keys[0]}1/1/`, { minKeys: 250, maxKeys: 20, delimiter: '/' });
                expect(listedObjects.objects).to.have.lengthOf(11);
                expect(listedObjects.commonPrefixes).to.be.undefined;
            });
        });

        context('objects upload / download', () => {
            it('uploads and gets object', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);

                const buffer = randomBytes(1 << 12);
                const key = `4k-${Math.floor(Math.random() * (2**64 -1))}`;
                let loaded = 0;
                await qiniuAdapter.putObject(
                    bucketRegionId, { bucket: bucketName, key: key }, buffer,
                    { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' } },
                    (uploaded: number, total: number) => {
                        expect(total).to.at.least(buffer.length);
                        loaded = uploaded;
                    });
                expect(loaded).to.at.least(buffer.length);

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

            it('upload data by chunk', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);

                const key = `2m-${Math.floor(Math.random() * (2**64 -1))}`;

                const createResult = await qiniuAdapter.createMultipartUpload(bucketRegionId, { bucket: bucketName, key: key },
                                                { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' } });

                const buffer_1 = randomBytes(1 << 20);
                let loaded = 0;
                const uploadPartResult_1 = await qiniuAdapter.uploadPart(bucketRegionId, { bucket: bucketName, key: key },
                                                createResult.uploadId, 1, buffer_1, (uploaded: number, total: number) => {
                                                    expect(total).to.equal(buffer_1.length);
                                                    loaded = uploaded;
                                                });
                expect(loaded).to.equal(buffer_1.length);

                const buffer_2 = randomBytes(1 << 20);
                loaded = 0;
                const uploadPartResult_2 = await qiniuAdapter.uploadPart(bucketRegionId, { bucket: bucketName, key: key },
                                                createResult.uploadId, 2, buffer_2, (uploaded: number, total: number) => {
                                                    expect(total).to.equal(buffer_2.length);
                                                    loaded = uploaded;
                                                });
                expect(loaded).to.equal(buffer_2.length);

                await qiniuAdapter.completeMultipartUpload(bucketRegionId, { bucket: bucketName, key: key }, createResult.uploadId,
                    [{ partNumber: 1, etag: uploadPartResult_1.etag }, { partNumber: 2, etag: uploadPartResult_2.etag }],
                    { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' } });

                const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: key });
                expect(header.metadata['key-a']).to.equal('Value-A');
                expect(header.metadata['key-b']).to.equal('Value-B');

                await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
            });

            it('upload object by uploader', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);

                const key = `11m-${Math.floor(Math.random() * (2**64 -1))}`;
                const tmpfilePath = tempfile();

                const tmpfile = await fs.promises.open(tmpfilePath, 'w+');
                try {
                    await tmpfile.write(randomBytes((1 << 20) * 11));
                    const uploader = new Uploader(qiniuAdapter);
                    let fileUploaded = 0;
                    const filePartUploaded = new Set<number>();
                    await uploader.putObjectFromFile(bucketRegionId, { bucket: bucketName, key: key }, tmpfile,
                                                {
                                                    header: { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' } },
                                                    putCallback: {
                                                        progressCallback: (uploaded, total) => {
                                                            expect(total).to.equal((1 << 20) * 11);
                                                            fileUploaded = uploaded;
                                                        },
                                                        partPutCallback: (part) => {
                                                            filePartUploaded.add(part.partNumber);
                                                        },
                                                    },
                                                });
                    expect(fileUploaded).to.equal((1 << 20) * 11);
                    expect(filePartUploaded).to.have.lengthOf(3);

                    const isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: key });
                    expect(isExisted).to.equal(true);

                    const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: key });
                    expect(header.metadata['key-a']).to.equal('Value-A');
                    expect(header.metadata['key-b']).to.equal('Value-B');

                    await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
                } finally {
                    await tmpfile.close();
                }
            });

            it('recover object by uploader', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);

                const key = `11m-${Math.floor(Math.random() * (2**64 -1))}`;
                const tmpfilePath = tempfile();

                const tmpfile = await fs.promises.open(tmpfilePath, 'w+');
                try {
                    await tmpfile.write(randomBytes((1 << 20) * 11));

                    const createResult = await qiniuAdapter.createMultipartUpload(bucketRegionId, { bucket: bucketName, key: key },
                                                    { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' } });

                    const { buffer: buffer_1 } = await tmpfile.read(Buffer.alloc(1 << 22), 0, 1 << 22, 1 << 22);
                    const uploadPartResult_1 = await qiniuAdapter.uploadPart(bucketRegionId, { bucket: bucketName, key: key },
                                                    createResult.uploadId, 2, buffer_1);

                    const { buffer: buffer_2 } = await tmpfile.read(Buffer.alloc((1 << 20) * 3), 0, (1 << 20) * 3, (1 << 22) * 2);
                    const uploadPartResult_2 = await qiniuAdapter.uploadPart(bucketRegionId, { bucket: bucketName, key: key },
                                                    createResult.uploadId, 3, buffer_2);


                    const uploader = new Uploader(qiniuAdapter);
                    let fileUploaded = 0;
                    const filePartUploaded = new Set<number>();
                    await uploader.putObjectFromFile(bucketRegionId, { bucket: bucketName, key: key }, tmpfile,
                                                {
                                                    header: { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' } },
                                                    recovered: {
                                                        uploadId: createResult.uploadId,
                                                        parts: [
                                                            { partNumber: 2, etag: uploadPartResult_1.etag },
                                                            { partNumber: 3, etag: uploadPartResult_2.etag },
                                                        ],
                                                    },
                                                    putCallback: {
                                                        progressCallback: (uploaded, total) => {
                                                            expect(total).to.equal((1 << 20) * 11);
                                                            fileUploaded = uploaded;
                                                        },
                                                        partPutCallback: (part) => {
                                                            filePartUploaded.add(part.partNumber);
                                                        },
                                                    },
                                                });
                    expect(fileUploaded).to.equal((1 << 20) * 11);
                    expect(filePartUploaded).to.have.lengthOf(1);

                    const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: key });
                    expect(header.metadata['key-a']).to.equal('Value-A');
                    expect(header.metadata['key-b']).to.equal('Value-B');

                    await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
                } finally {
                    await tmpfile.close();
                }
            });

            it('upload small object by uploader', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);

                const key = `11k-${Math.floor(Math.random() * (2**64 -1))}`;
                const tmpfilePath = tempfile();

                const tmpfile = await fs.promises.open(tmpfilePath, 'w+');
                try {
                    await tmpfile.write(randomBytes((1 << 10) * 11));
                    const uploader = new Uploader(qiniuAdapter);
                    let fileUploaded = 0;
                    await uploader.putObjectFromFile(bucketRegionId, { bucket: bucketName, key: key }, tmpfile,
                                                {
                                                    header: { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' } },
                                                    putCallback: {
                                                        progressCallback: (uploaded, total) => {
                                                            expect(total).to.at.least((1 << 10) * 11);
                                                            fileUploaded = uploaded;
                                                        },
                                                    },
                                                });
                    expect(fileUploaded).to.at.least((1 << 10) * 11);

                    const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: key });
                    expect(header.metadata['key-a']).to.equal('Value-A');
                    expect(header.metadata['key-b']).to.equal('Value-B');

                    await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
                } finally {
                    await tmpfile.close();
                }
            });
        });

        context('bucket', () => {
            it('creates a bucket and drops it', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
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
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);

                const buckets = await qiniuAdapter.listBuckets();

                const bucket = buckets.find((bucket) => bucket.name === bucketName);
                expect(bucket?.regionId).to.equal(bucketRegionId);
            });

            it('lists domain', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);

                const domains = await qiniuAdapter.listDomains(bucketRegionId, bucketName);
                if (mode === KODO_MODE) {
                    expect(domains).to.have.lengthOf.at.least(1);
                } else {
                    expect(domains).to.be.empty;
                }
            });
        });
    });
});
