import process from 'process';
import urllib from 'urllib';
import tempfile from 'tempfile';
import fs, { ReadStream as FileReadStream } from 'fs';
import md5 from 'md5';
import { Semaphore } from 'semaphore-promise';
import { randomBytes } from 'crypto';
import { expect, assert } from 'chai';
import { Qiniu, KODO_MODE, S3_MODE } from '../qiniu';
import { TransferObject } from '../adapter';
import { Uploader } from '../uploader';
import { Downloader } from '../downloader';
import { ServiceName } from '../kodo-http-client';
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
        const originalFileName = '测试文件名.data';
        const availableStorageClasses = [
            {
                fileType: 0,
                kodoName: 'Standard',
                s3Name: 'STANDARD',
            },
            {
                fileType: 1,
                kodoName: 'InfrequentAccess',
                s3Name: 'LINE',
            },
            {
                fileType: 2,
                kodoName: 'Archive',
                s3Name: 'GLACIER',
            },
        ];

        context('objects operation', () => {
            it('get or getHeaders of unexisted object', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;
                const key = `4k-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;

                try {
                    await qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: key });
                    assert.fail();
                } catch {
                    // pass
                }

                try {
                    await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: key });
                    assert.fail();
                } catch {
                    // pass
                }
            });

            it('moves and copies object', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;

                const buffer = randomBytes(1 << 12);
                const key = `4k-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                await qiniuAdapter.putObject(
                    bucketRegionId, { bucket: bucketName, key: key }, buffer, originalFileName,
                    { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' }, contentType: 'application/json' });
                const keyCopied = `${key}-复制`;
                await qiniuAdapter.copyObject(bucketRegionId, { from: { bucket: bucketName, key: key }, to: { bucket: bucketName, key: keyCopied } });

                {
                    const info = await qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: keyCopied });
                    expect(info.size).to.equal(1 << 12);
                }

                {
                    const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: keyCopied });
                    expect(header.size).to.equal(1 << 12);
                    expect(header.metadata['key-a']).to.equal('Value-A');
                    expect(header.metadata['key-b']).to.equal('Value-B');
                    expect(header.metadata).to.have.all.keys('key-a', 'key-b');
                    expect(header.contentType).to.equal('application/json');
                }

                await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: keyCopied });

                const keyMoved = `${key}-move`;
                await qiniuAdapter.moveObject(bucketRegionId, { from: { bucket: bucketName, key: key }, to: { bucket: bucketName, key: keyMoved } });

                {
                    const header = await qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: keyMoved });
                    expect(header.size).to.equal(1 << 12);
                }

                {
                    const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: keyMoved });
                    expect(header.size).to.equal(1 << 12);
                    expect(header.metadata['key-a']).to.equal('Value-A');
                    expect(header.metadata['key-b']).to.equal('Value-B');
                    expect(header.metadata).to.have.all.keys('key-a', 'key-b');
                    expect(header.contentType).to.equal('application/json');
                }

                await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: keyMoved });

                let isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: key });
                expect(isExisted).to.equal(false);

                isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: keyCopied });
                expect(isExisted).to.equal(false);

                isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: keyMoved });
                expect(isExisted).to.equal(false);
            });

            it('moves and copies object by force', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;

                const buffer = randomBytes(1 << 12);
                const key = `4k-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                await qiniuAdapter.putObject(
                    bucketRegionId, { bucket: bucketName, key: key }, buffer, originalFileName,
                    { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' }, contentType: 'application/json' });

                const newKey = `${key}-新建`;
                await qiniuAdapter.copyObject(bucketRegionId, { from: { bucket: bucketName, key: key }, to: { bucket: bucketName, key: newKey } });

                let isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: key });
                expect(isExisted).to.equal(true);

                isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: newKey });
                expect(isExisted).to.equal(true);

                await qiniuAdapter.moveObject(bucketRegionId, { from: { bucket: bucketName, key: key }, to: { bucket: bucketName, key: newKey } });

                isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: key });
                expect(isExisted).to.equal(false);

                isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: newKey });
                expect(isExisted).to.equal(true);

                await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: newKey });
            });

            it('moves, copies and deletes objects', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;
                const semaphore = new Semaphore(20);

                const seed = Math.floor(Math.random() * (2 ** 64 - 1));
                const keys: Array<string> = new Array(250).fill('').map((_, idx: number) => `10b-${seed}-${idx}`);
                const uploadPromises = keys.map((key) => {
                    return new Promise((resolve, reject) => {
                        semaphore.acquire().then((release) => {
                            qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key }, randomBytes(10), originalFileName)
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

            it('upload object with storage class', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;

                const buffer = randomBytes(1 << 12);

                // Standard
                {
                    const key = `4k-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                    await qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key, storageClassName: 'Standard' }, buffer, originalFileName);

                    const frozenInfo = await qiniuAdapter.getFrozenInfo(bucketRegionId, { bucket: bucketName, key: key });
                    expect(frozenInfo.status).to.equal('Normal');

                    const objectInfo = await qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: key });
                    expect(objectInfo.storageClass).to.equal('Standard');
                    await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
                }

                // InfrequentAccess
                {
                    const key = `4k-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                    await qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key, storageClassName: 'InfrequentAccess' }, buffer, originalFileName);

                    const frozenInfo = await qiniuAdapter.getFrozenInfo(bucketRegionId, { bucket: bucketName, key: key });
                    expect(frozenInfo.status).to.equal('Normal');
                    const objectInfo = await qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: key });
                    expect(objectInfo.storageClass).to.equal('InfrequentAccess');
                    await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
                }

                // Archive
                {
                    const key = `4k-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                    await qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key, storageClassName: 'Archive' }, buffer, originalFileName);

                    const frozenInfo = await qiniuAdapter.getFrozenInfo(bucketRegionId, { bucket: bucketName, key: key });
                    expect(frozenInfo.status).to.equal('Frozen');
                    const objectInfo = await qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: key });
                    expect(objectInfo.storageClass).to.equal('Archive');
                    await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
                }

            });

            it('set object storage class', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;

                const buffer = randomBytes(1 << 12);
                const key = `4k-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                await qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key }, buffer, originalFileName);

                const frozenInfo = await qiniuAdapter.getFrozenInfo(bucketRegionId, { bucket: bucketName, key: key });
                expect(frozenInfo.status).to.equal('Normal');

                let objectInfo = await qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: key });
                expect(objectInfo.storageClass).to.equal('Standard');

                await qiniuAdapter.setObjectStorageClass(bucketRegionId, { bucket: bucketName, key: key }, 'InfrequentAccess');
                objectInfo = await qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: key });
                expect(objectInfo.storageClass).to.equal('InfrequentAccess');

                await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
            });

            it('set objects storage class', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;
                const semaphore = new Semaphore(20);

                const seed = Math.floor(Math.random() * (2 ** 64 - 1));
                const keys: Array<string> = new Array(10).fill('').map((_, idx: number) => `10b-${seed}-${idx}`);
                const uploadPromises = keys.map((key) => {
                    return new Promise((resolve, reject) => {
                        semaphore.acquire().then((release) => {
                            qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key }, randomBytes(10), originalFileName)
                                .then(resolve, reject)
                                .finally(() => { release(); });
                        });
                    });
                });
                await Promise.all(uploadPromises);
                await qiniuAdapter.setObjectsStorageClass(bucketRegionId, bucketName, keys, 'InfrequentAccess');
                const getAllStorageClassesPromises = keys.map((key) => {
                    return new Promise((resolve, reject) => {
                        semaphore.acquire().then((release) => {
                            qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: key })
                                .then((info) => { resolve(info.storageClass); }, reject)
                                .finally(() => { release(); });
                        });
                    });
                });
                const allStorageClasses = await Promise.all(getAllStorageClassesPromises);
                for (const storageClass of allStorageClasses) {
                    expect(storageClass).to.equal('InfrequentAccess');
                }

                await qiniuAdapter.deleteObjects(bucketRegionId, bucketName, keys);
            });

            it('freeze object and restore it', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;
                const kodoAdapter = qiniu.mode(KODO_MODE);

                const buffer = randomBytes(1 << 12);
                const key = `4k-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                await qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key }, buffer, originalFileName);
                await qiniuAdapter.setObjectStorageClass(bucketRegionId, { bucket: bucketName, key: key }, 'Archive');

                let frozenInfo = await kodoAdapter.getFrozenInfo(bucketRegionId, { bucket: bucketName, key: key });
                expect(frozenInfo.status).to.equal('Frozen');
                let objectInfo = await qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: key });
                expect(objectInfo.storageClass).to.equal('Archive');

                await qiniuAdapter.restoreObject(bucketRegionId, { bucket: bucketName, key: key }, 1);
                frozenInfo = await kodoAdapter.getFrozenInfo(bucketRegionId, { bucket: bucketName, key: key });
                expect(frozenInfo.status).to.equal('Unfreezing');
                objectInfo = await qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: key });
                expect(objectInfo.storageClass).to.equal('Archive');

                await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
            });

            it('freeze objects and restore them', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;
                const semaphore = new Semaphore(20);

                const seed = Math.floor(Math.random() * (2 ** 64 - 1));
                const keys: Array<string> = new Array(10).fill('').map((_, idx: number) => `10b-${seed}-${idx}`);

                const uploadPromises = keys.map((key) => {
                    return new Promise((resolve, reject) => {
                        semaphore.acquire().then((release) => {
                            qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key }, randomBytes(10), originalFileName)
                                .then(resolve, reject)
                                .finally(() => { release(); });
                        });
                    });
                });
                await Promise.all(uploadPromises);
                await qiniuAdapter.setObjectsStorageClass(bucketRegionId, bucketName, keys, 'Archive');

                {
                    const getAllStorageClassesPromises = keys.map((key) => {
                        return new Promise((resolve, reject) => {
                            semaphore.acquire().then((release) => {
                                qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: key })
                                    .then((info) => { resolve(info.storageClass); }, reject)
                                    .finally(() => { release(); });
                            });
                        });
                    });
                    const allStorageClasses = await Promise.all(getAllStorageClassesPromises);
                    for (const storageClass of allStorageClasses) {
                        expect(storageClass).to.equal('Archive');
                    }
                }

                await qiniuAdapter.restoreObjects(bucketRegionId, bucketName, keys, 1);

                {
                    const getAllFrozenInfosPromises = keys.map((key) => {
                        return new Promise((resolve, reject) => {
                            semaphore.acquire().then((release) => {
                                qiniuAdapter.getFrozenInfo(bucketRegionId, { bucket: bucketName, key: key })
                                    .then((info) => { resolve(info.status); }, reject)
                                    .finally(() => { release(); });
                            });
                        });
                    });
                    const allFrozenInfos = await Promise.all(getAllFrozenInfosPromises);
                    for (const storageClass of allFrozenInfos) {
                        expect(storageClass).to.equal('Unfreezing');
                    }

                    const getAllStorageClassesPromises = keys.map((key) => {
                        return new Promise((resolve, reject) => {
                            semaphore.acquire().then((release) => {
                                qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: key })
                                    .then((info) => { resolve(info.storageClass); }, reject)
                                    .finally(() => { release(); });
                            });
                        });
                    });
                    const allStorageClasses = await Promise.all(getAllStorageClassesPromises);
                    for (const storageClass of allStorageClasses) {
                        expect(storageClass).to.equal('Archive');
                    }
                }

                await qiniuAdapter.deleteObjects(bucketRegionId, bucketName, keys);
            });

            it('list objects', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;
                const semaphore = new Semaphore(20);

                const seed = Math.floor(Math.random() * (2 ** 64 - 1));
                let keys: Array<string> = [`10b-文件-${seed}/`];
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
                            qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key }, randomBytes(10), originalFileName)
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

            it('check url query', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;

                const seed = Math.floor(Math.random() * (2 ** 64 - 1));
                const baseKey = `10b-文件+${seed}`;
                const key = `${baseKey}/test|adapter`; // the `+`, `|` should be url encode
                await qiniuAdapter.putObject(bucketRegionId, { bucket: bucketName, key: key }, randomBytes(10), originalFileName);
                const listedObjects = await qiniuAdapter.listObjects(bucketRegionId, bucketName, baseKey, { minKeys: 1 });
                expect(listedObjects.objects).to.have.lengthOf(1);
            });
        });

        context('objects upload / download', () => {
            const cases: {
                dataSourceType: 'buffer' | 'stream',
                label: string,
            }[] = [
                { dataSourceType: 'buffer', label: '[buffer]' },
                { dataSourceType: 'stream', label: '[stream]' },
            ];

            async function getDataSource(
                data: Buffer,
                type: 'buffer' | 'stream',
            ): Promise<{
                dataSource: Buffer | FileReadStream,
                filePath?: string,
            }> {
                let dataSource: Buffer | FileReadStream = data;

                let filePath: string | undefined;
                if (type === 'stream') {
                    filePath = tempfile();
                    const tmpFile = await fs.promises.open(filePath, 'w');
                    await tmpFile.write(data);
                    await tmpFile.close();
                    dataSource = fs.createReadStream(filePath);
                }
                return {
                    dataSource,
                    filePath,
                };
            }

            cases.forEach(caseItem => {
                it('uploads and gets object' + caseItem.label, async () => {
                    const qiniu = new Qiniu(accessKey, secretKey);
                    const qiniuAdapter = qiniu.mode(mode);
                    qiniuAdapter.storageClasses = availableStorageClasses;

                    const buffer = randomBytes(1 << 12);
                    const { dataSource, filePath } = await getDataSource(buffer, caseItem.dataSourceType);
                    const key = `4k-文件%-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                    let loaded = 0;
                    await qiniuAdapter.putObject(
                        bucketRegionId, { bucket: bucketName, key: key }, dataSource, originalFileName,
                        { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' }, contentType: 'application/json' },
                        {
                            progressCallback: (uploaded: number, total: number) => {
                                expect(total).to.at.least(buffer.length);
                                loaded = uploaded;
                            },
                            fileStreamSetting: filePath ? {
                                path: filePath,
                                start: 0,
                                end: Infinity,
                            } : undefined,
                        });
                    if (dataSource instanceof FileReadStream) {
                        dataSource.close();
                    }
                    expect(loaded).to.at.least(buffer.length);

                    let isExisted: boolean = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: key });
                    expect(isExisted).to.equal(true);

                    {
                        const url = await qiniuAdapter.getObjectURL(bucketRegionId, { bucket: bucketName, key: key }, undefined, new Date(Date.now() + 86400000));
                        const response = await urllib.request(url.toString(), { method: 'GET', streaming: true });
                        expect(response.status).to.equal(200);
                        if (mode == KODO_MODE) {
                            expect(response.headers['x-qn-meta-key-a']).to.equal('Value-A');
                            expect(response.headers['x-qn-meta-key-b']).to.equal('Value-B');
                        } else if (mode == S3_MODE) {
                            expect(response.headers['x-amz-meta-key-a']).to.equal('Value-A');
                            expect(response.headers['x-amz-meta-key-b']).to.equal('Value-B');
                        }
                        expect(response.headers['content-type']).to.equal('application/json');
                        response.res.destroy();
                    }

                    {
                        const result = await qiniuAdapter.getObject(bucketRegionId, { bucket: bucketName, key: key });
                        expect(result.data).to.eql(buffer);
                        expect(result.header.size).to.equal(1 << 12);
                        expect(result.header.metadata['key-a']).to.equal('Value-A');
                        expect(result.header.metadata['key-b']).to.equal('Value-B');
                        expect(result.header.metadata).to.have.all.keys('key-a', 'key-b');
                        expect(result.header.contentType).to.equal('application/json');
                    }

                    {
                        let dataLength = 0;
                        const readable = await qiniuAdapter.getObjectStream(bucketRegionId, { bucket: bucketName, key: key });
                        await new Promise((resolve, reject) => {
                            readable.on('data', (chunk: any) => {
                                dataLength += chunk.length;
                            });
                            readable.on('end', () => {
                                expect(dataLength).to.equal(1 << 12);
                                resolve();
                            });
                            readable.on('error', reject);
                        });
                    }

                    {
                        const info = await qiniuAdapter.getObjectInfo(bucketRegionId, { bucket: bucketName, key: key });
                        expect(info.size).to.equal(1 << 12);
                    }

                    {
                        const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: key });
                        expect(header.size).to.equal(1 << 12);
                        expect(header.metadata['key-a']).to.equal('Value-A');
                        expect(header.metadata['key-b']).to.equal('Value-B');
                        expect(header.metadata).to.have.all.keys('key-a', 'key-b');
                        expect(header.contentType).to.equal('application/json');
                    }

                    await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });

                    isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: key });
                    expect(isExisted).to.equal(false);
                });

                it('uploads and gets big object' + caseItem.label, async () => {
                    const qiniu = new Qiniu(accessKey, secretKey);
                    const qiniuAdapter = qiniu.mode(mode);
                    qiniuAdapter.storageClasses = availableStorageClasses;

                    const buffer = randomBytes((1 << 20) * 8);
                    const { dataSource, filePath } = await getDataSource(buffer, caseItem.dataSourceType);
                    const key = `8m-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                    await qiniuAdapter.putObject(
                        bucketRegionId,
                        { bucket: bucketName, key: key },
                        dataSource,
                        originalFileName,
                        { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' }, contentType: 'application/json' },
                        {
                            fileStreamSetting: filePath ? {
                                path: filePath,
                                start: 0,
                                end: Infinity,
                            }: undefined,
                        },
                    );
                    if (dataSource instanceof FileReadStream) {
                        dataSource.close();
                    }

                    {
                        let dataLength = 0;
                        const readable = await qiniuAdapter.getObjectStream(bucketRegionId, { bucket: bucketName, key: key });
                        await new Promise((resolve, reject) => {
                            readable.on('data', (chunk: any) => {
                                dataLength += chunk.length;
                            });
                            readable.on('end', () => {
                                expect(dataLength).to.equal((1 << 20) * 8);
                                resolve();
                            });
                            readable.on('error', reject);
                        });
                    }
                    {
                        let dataLength = 0;
                        const readable = await qiniuAdapter.getObjectStream(bucketRegionId, { bucket: bucketName, key: key },
                            undefined,
                            { rangeStart: (1 << 20), rangeEnd: (1 << 20) * 2 });
                        await new Promise((resolve, reject) => {
                            readable.on('data', (chunk: any) => {
                                dataLength += chunk.length;
                            });
                            readable.on('end', () => {
                                expect(dataLength).to.equal((1 << 20) + 1);
                                resolve();
                            });
                            readable.on('error', reject);
                        });
                    }
                });

                it('upload data by chunk' + caseItem.label, async () => {
                    const qiniu = new Qiniu(accessKey, secretKey);
                    const qiniuAdapter = qiniu.mode(mode);
                    qiniuAdapter.storageClasses = availableStorageClasses;

                    const key = `2m-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                    const setHeader = { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' }, contentType: 'application/json' };

                    const createResult = await qiniuAdapter.createMultipartUpload(bucketRegionId, { bucket: bucketName, key: key }, originalFileName, setHeader);

                    const buffer_1 = randomBytes(1 << 20);
                    const { dataSource: dataSource_1, filePath: filePath_1 } = await getDataSource(buffer_1, caseItem.dataSourceType);
                    let loaded = 0;
                    const uploadPartResult_1 = await qiniuAdapter.uploadPart(
                        bucketRegionId,
                        { bucket: bucketName, key: key },
                        createResult.uploadId,
                        1,
                        dataSource_1,
                        {
                            progressCallback: (uploaded: number, total: number) => {
                                expect(total).to.equal(buffer_1.length);
                                loaded = uploaded;
                            },
                            fileStreamSetting: filePath_1 ? {
                                path: filePath_1,
                                start: 0,
                                end: buffer_1.length - 1,
                            } : undefined,
                        },
                    );
                    if (dataSource_1 instanceof FileReadStream) {
                        dataSource_1.close();
                    }
                    expect(loaded).to.equal(buffer_1.length);

                    const buffer_2 = randomBytes(1 << 20);
                    const { dataSource: dataSource_2, filePath: filePath_2 } = await getDataSource(buffer_1, caseItem.dataSourceType);
                    loaded = 0;
                    const uploadPartResult_2 = await qiniuAdapter.uploadPart(
                        bucketRegionId,
                        { bucket: bucketName, key: key },
                        createResult.uploadId,
                        2,
                        dataSource_2,
                        {
                            progressCallback: (uploaded: number, total: number) => {
                                expect(total).to.equal(buffer_2.length);
                                loaded = uploaded;
                            },
                            fileStreamSetting: filePath_2 ? {
                                path: filePath_2,
                                start: 0,
                                end: buffer_2.length - 1,
                            } : undefined,
                        },
                    );
                    if (dataSource_2 instanceof FileReadStream) {
                        dataSource_2.close();
                    }
                    expect(loaded).to.equal(buffer_2.length);

                    await qiniuAdapter.completeMultipartUpload(bucketRegionId, { bucket: bucketName, key: key }, createResult.uploadId,
                        [{ partNumber: 1, etag: uploadPartResult_1.etag }, { partNumber: 2, etag: uploadPartResult_2.etag }],
                        originalFileName, setHeader);

                    const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: key });
                    expect(header.metadata['key-a']).to.equal('Value-A');
                    expect(header.metadata['key-b']).to.equal('Value-B');
                    expect(header.contentType).to.equal('application/json');

                    {
                        const downloader = new Downloader(qiniuAdapter);
                        const targetFilePath = tempfile();
                        let fileDownloaded = 0;

                        await downloader.getObjectToFile(bucketRegionId, { bucket: bucketName, key: key }, targetFilePath, undefined, {
                            getCallback: {
                                progressCallback: (p) => {
                                    expect(p.total).to.equal((1 << 20) * 2);
                                    fileDownloaded = p.transferred;
                                },
                                headerCallback: (header) => {
                                    expect(header.size).to.equal((1 << 20) * 2);
                                },
                            },
                            partSize: 1 << 20,
                            chunkTimeout: 30000,
                            downloadThrottleOption: { rate: 1 << 30 },
                        });
                        expect(fileDownloaded).to.equal((1 << 20) * 2);
                    }

                    await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
                });

                it('upload and download empty object' + caseItem.label, async () => {
                    const qiniu = new Qiniu(accessKey, secretKey);
                    const qiniuAdapter = qiniu.mode(mode);
                    qiniuAdapter.storageClasses = availableStorageClasses;

                    const buffer = Buffer.alloc(0);
                    const { dataSource, filePath } = await getDataSource(buffer, caseItem.dataSourceType);
                    const key = `empty-"file"-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                    let loaded = 0;

                    {
                        await qiniuAdapter.putObject(
                            bucketRegionId,
                            {
                                bucket: bucketName,
                                key: key,
                            },
                            dataSource,
                            originalFileName,
                            {
                                contentType: 'application/octet-stream',
                            },
                            {
                                progressCallback: (uploaded, total) => {
                                    expect(total).to.at.least(buffer.length);
                                    loaded = uploaded;
                                },
                                fileStreamSetting: filePath ? {
                                    path: filePath,
                                    start: 0,
                                    end: Infinity,
                                } : undefined,
                            }
                        );
                        if (dataSource instanceof FileReadStream) {
                            dataSource.close();
                        }
                        expect(loaded).to.at.least(buffer.length);
                    }

                    {
                        const isExisted: boolean = await qiniuAdapter.isExists(
                            bucketRegionId,
                            { bucket: bucketName, key: key },
                        );
                        expect(isExisted).to.equal(true);
                    }

                    {
                        const result = await qiniuAdapter.getObject(
                            bucketRegionId,
                            { bucket: bucketName, key: key },
                        );
                        expect(result.data).to.eql(buffer);
                        expect(result.header.size).to.equal(buffer.length);
                        expect(result.header.contentType).to.equal('application/octet-stream');
                    }

                    {
                        await qiniuAdapter.deleteObject(
                            bucketRegionId,
                            { bucket: bucketName, key: key },
                        );

                        const isExisted = await qiniuAdapter.isExists(
                            bucketRegionId,
                            { bucket: bucketName, key: key },
                        );
                        expect(isExisted).to.equal(false);
                    }
                });
            });

            it('upload object by uploader and download by downloader', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;

                const key = `11m-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                const tmpfilePath = tempfile();

                const tmpfile = await fs.promises.open(tmpfilePath, 'w+');
                try {
                    await tmpfile.write(randomBytes((1 << 20) * 11));
                    const uploader = new Uploader(qiniuAdapter);
                    let fileUploaded = 0;
                    const filePartUploaded = new Set<number>();
                    await uploader.putObjectFromFile(bucketRegionId, { bucket: bucketName, key: key }, tmpfilePath, (1 << 20) * 11, originalFileName,
                        {
                            header: { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' }, contentType: 'application/json' },
                            putCallback: {
                                partsInitCallback: (info) => {
                                    expect(info.uploadId).to.be.ok;
                                    expect(info.parts).to.be.empty;
                                },
                                progressCallback: (p) => {
                                    expect(p.total).to.equal((1 << 20) * 11);
                                    fileUploaded = p.transferred;
                                },
                                partPutCallback: (part) => {
                                    filePartUploaded.add(part.partNumber);
                                },
                            },
                            uploadThrottleOption: { rate: 1 << 30 },
                        });
                    expect(fileUploaded).to.equal((1 << 20) * 11);
                    expect(filePartUploaded).to.have.lengthOf(3);


                    {
                        const isExisted = await qiniuAdapter.isExists(bucketRegionId, { bucket: bucketName, key: key });
                        expect(isExisted).to.equal(true);
                    }

                    {
                        const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: key });
                        expect(header.metadata['key-a']).to.equal('Value-A');
                        expect(header.metadata['key-b']).to.equal('Value-B');
                        expect(header.contentType).to.equal('application/json');
                    }

                    {
                        const downloader = new Downloader(qiniuAdapter);
                        const targetFilePath = tempfile();
                        let fileDownloaded = 0;

                        await downloader.getObjectToFile(bucketRegionId, { bucket: bucketName, key: key }, targetFilePath, undefined, {
                            getCallback: {
                                progressCallback: (p) => {
                                    expect(p.total).to.equal((1 << 20) * 11);
                                    fileDownloaded = p.transferred;
                                },
                                headerCallback: (header) => {
                                    expect(header.size).to.equal((1 << 20) * 11);
                                },
                            },
                            partSize: 1 << 20,
                            chunkTimeout: 30000,
                        });
                        expect(fileDownloaded).to.equal((1 << 20) * 11);

                        const md5FromSource = await new Promise((resolve, reject) => {
                            fs.readFile(tmpfilePath, { encoding: 'binary' }, (err, buf) => {
                                if (err) {
                                    reject(err);
                                    return;
                                }
                                resolve(md5(buf, { encoding: 'binary', asBytes: true }));
                            });
                        });
                        const md5FromObject = await new Promise((resolve, reject) => {
                            fs.readFile(targetFilePath, { encoding: 'binary' }, (err, buf) => {
                                if (err) {
                                    reject(err);
                                    return;
                                }
                                resolve(md5(buf, { encoding: 'binary', asBytes: true }));
                            });
                        });
                        expect(md5FromSource).to.eql(md5FromObject);
                    }

                    await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
                } finally {
                    await tmpfile.close();
                }
            });

            it('upload object and then cancel', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;

                const key = `100m-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                const tmpfilePath = tempfile();

                const tmpfile = await fs.promises.open(tmpfilePath, 'w+');
                try {
                    await tmpfile.write(randomBytes((1 << 20) * 100));
                    const uploader = new Uploader(qiniuAdapter);
                    try {
                        await uploader.putObjectFromFile(bucketRegionId, { bucket: bucketName, key: key }, tmpfilePath, (1 << 20) * 100, originalFileName, {
                            uploadThrottleOption: { rate: 1024 },
                            putCallback: {
                                progressCallback: () => {
                                    uploader.abort();
                                },
                            },
                        });
                        assert.fail();
                    } catch (err) {
                        expect(err.message).to.include('User Canceled');
                    }
                } finally {
                    await tmpfile.close();
                }
            });

            it('download object and then cancel', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);

                const key = `11m-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                const tmpfilePath = tempfile();

                const tmpfile = await fs.promises.open(tmpfilePath, 'w+');
                try {
                    await tmpfile.write(randomBytes((1 << 20) * 11));
                    const uploader = new Uploader(qiniuAdapter);
                    await uploader.putObjectFromFile(bucketRegionId, { bucket: bucketName, key: key }, tmpfilePath, (1 << 20) * 11, originalFileName);

                    const downloader = new Downloader(qiniuAdapter);
                    const targetFilePath = tempfile();

                    try {
                        await downloader.getObjectToFile(bucketRegionId, { bucket: bucketName, key: key }, targetFilePath, undefined, {
                            downloadThrottleOption: { rate: 1024 },
                            getCallback: {
                                progressCallback: () => {
                                    downloader.abort();
                                },
                            },
                            partSize: 1 << 20,
                        });
                        assert.fail();
                    } catch (err) {
                        expect(err.message).to.include('User Canceled');
                    }
                } finally {
                    await tmpfile.close();
                }
            });

            it('recover object by uploader', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;

                const key = `11m-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                const tmpfilePath = tempfile();

                const tmpfile = await fs.promises.open(tmpfilePath, 'w+');
                try {
                    await tmpfile.write(randomBytes((1 << 20) * 11));

                    const createResult = await qiniuAdapter.createMultipartUpload(bucketRegionId, { bucket: bucketName, key: key }, originalFileName,
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
                    await uploader.putObjectFromFile(bucketRegionId, { bucket: bucketName, key: key }, tmpfilePath, (1 << 20) * 11, originalFileName,
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
                                partsInitCallback: (info) => {
                                    expect(info.uploadId).to.equal(createResult.uploadId);
                                    expect(info.parts).to.have.lengthOf(2);
                                },
                                progressCallback: (p) => {
                                    expect(p.total).to.equal((1 << 20) * 11);
                                    fileUploaded = p.transferred;
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

                    {
                        const downloader = new Downloader(qiniuAdapter);
                        const targetFilePath = tempfile();
                        let fileDownloaded = 0;

                        await downloader.getObjectToFile(bucketRegionId, { bucket: bucketName, key: key }, targetFilePath, undefined, {
                            getCallback: {
                                progressCallback: (p) => {
                                    expect(p.total).to.equal((1 << 20) * 11);
                                    fileDownloaded = p.transferred;
                                },
                                headerCallback: (header) => {
                                    expect(header.size).to.equal((1 << 20) * 11);
                                },
                            },
                            partSize: 1 << 20,
                            chunkTimeout: 30000,
                        });
                        expect(fileDownloaded).to.equal((1 << 20) * 11);

                        const md5FromSource = await new Promise((resolve, reject) => {
                            fs.readFile(tmpfilePath, { encoding: 'binary' }, (err, buf) => {
                                if (err) {
                                    reject(err);
                                    return;
                                }
                                resolve(md5(buf, { encoding: 'binary', asBytes: true }));
                            });
                        });
                        const md5FromObject = await new Promise((resolve, reject) => {
                            fs.readFile(targetFilePath, { encoding: 'binary' }, (err, buf) => {
                                if (err) {
                                    reject(err);
                                    return;
                                }
                                resolve(md5(buf, { encoding: 'binary', asBytes: true }));
                            });

                        });
                        expect(md5FromSource).to.eql(md5FromObject);
                    }

                    await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
                } finally {
                    await tmpfile.close();
                }
            });

            it('upload small object by uploader', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;

                const key = `11k-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                const tmpfilePath = tempfile();

                const tmpfile = await fs.promises.open(tmpfilePath, 'w+');
                try {
                    await tmpfile.write(randomBytes((1 << 10) * 11));
                    const uploader = new Uploader(qiniuAdapter);
                    let fileUploaded = 0;
                    await uploader.putObjectFromFile(bucketRegionId, { bucket: bucketName, key: key }, tmpfilePath, (1 << 10) * 11, originalFileName,
                        {
                            header: { metadata: { 'Key-A': 'Value-A', 'Key-B': 'Value-B' } },
                            putCallback: {
                                progressCallback: (p) => {
                                    expect(p.total).to.at.least((1 << 10) * 11);
                                    fileUploaded = p.transferred;
                                },
                            },
                        });
                    expect(fileUploaded).to.at.least((1 << 10) * 11);

                    const header = await qiniuAdapter.getObjectHeader(bucketRegionId, { bucket: bucketName, key: key });
                    expect(header.metadata['key-a']).to.equal('Value-A');
                    expect(header.metadata['key-b']).to.equal('Value-B');

                    {
                        const downloader = new Downloader(qiniuAdapter);
                        const targetFilePath = tempfile();
                        let fileDownloaded = 0;

                        await downloader.getObjectToFile(bucketRegionId, { bucket: bucketName, key: key }, targetFilePath, undefined, {
                            getCallback: {
                                progressCallback: (p) => {
                                    expect(p.total).to.equal((1 << 10) * 11);
                                    fileDownloaded = p.transferred;
                                },
                                headerCallback: (header) => {
                                    expect(header.size).to.equal((1 << 10) * 11);
                                },
                            },
                            partSize: 1 << 10,
                            chunkTimeout: 30000,
                        });
                        expect(fileDownloaded).to.equal((1 << 10) * 11);

                        const md5FromSource = await new Promise((resolve, reject) => {
                            fs.readFile(tmpfilePath, { encoding: 'binary' }, (err, buf) => {
                                if (err) {
                                    reject(err);
                                    return;
                                }
                                resolve(md5(buf, { encoding: 'binary', asBytes: true }));
                            });
                        });
                        const md5FromObject = await new Promise((resolve, reject) => {
                            fs.readFile(targetFilePath, { encoding: 'binary' }, (err, buf) => {
                                if (err) {
                                    reject(err);
                                    return;
                                }
                                resolve(md5(buf, { encoding: 'binary', asBytes: true }));
                            });

                        });
                        expect(md5FromSource).to.eql(md5FromObject);
                    }

                    await qiniuAdapter.deleteObject(bucketRegionId, { bucket: bucketName, key: key });
                } finally {
                    await tmpfile.close();
                }
            });

            if (mode === KODO_MODE) {
              it('upload with accelerate uploading by uploader', async () => {
                const tmpfilePath = tempfile();
                const tmpfile = await fs.promises.open(tmpfilePath, 'w');
                try {
                  await tmpfile.write(randomBytes((1 << 20) * 11));
                } finally {
                  tmpfile.close();
                }

                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;
                const key = `11m-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                const uploader = new Uploader(qiniuAdapter);
                await uploader.putObjectFromFile(
                  bucketRegionId,
                  {
                    bucket: bucketName,
                    key: key,
                  },
                  tmpfilePath,
                  (1 << 20) * 11,
                  originalFileName,
                  {
                    accelerateUploading: true,
                    putCallback: {
                      partsInitCallback: (info) => {
                          expect(info.uploadId).to.be.ok;
                          expect(info.parts).to.be.empty;
                      },
                      progressCallback: (p) => {
                          expect(p.total).to.equal((1 << 20) * 11);
                      },
                    },
                  }
                );

                const downloader = new Downloader(qiniuAdapter);
                const targetFilePath = tempfile();
                await downloader.getObjectToFile(
                  bucketRegionId,
                  {
                    bucket: bucketName,
                    key: key
                  },
                  targetFilePath
                );
                const md5FromSource = await new Promise((resolve, reject) => {
                    fs.readFile(tmpfilePath, { encoding: 'binary' }, (err, buf) => {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(md5(buf, { encoding: 'binary', asBytes: true }));
                    });
                });
                const md5FromObject = await new Promise((resolve, reject) => {
                    fs.readFile(targetFilePath, { encoding: 'binary' }, (err, buf) => {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(md5(buf, { encoding: 'binary', asBytes: true }));
                    });
                });
                expect(md5FromSource).to.eql(md5FromObject);
              });

              it('upload with retry by uploader', async () => {
                // create adapter and mock its getServiceUrls method for UP
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                qiniuAdapter.storageClasses = availableStorageClasses;
                // @ts-ignore
                const actualGetServiceUrls = qiniuAdapter.client.getServiceUrls;
                // @ts-ignore
                qiniuAdapter.client.getServiceUrls = async (...args: any) => {
                  // @ts-ignore
                  const result = await actualGetServiceUrls.apply(qiniuAdapter.client, args);
                  if (args[0] === ServiceName.Up) {
                    result.unshift('http://up.fake.qiniu.com');
                  }
                  return result;
                };

                // test upload
                // create file to upload
                const tmpfilePath = tempfile();
                const tmpfile = await fs.promises.open(tmpfilePath, 'w');
                try {
                  await tmpfile.write(randomBytes((1 << 20) * 11));
                } finally {
                  tmpfile.close();
                }

                // upload
                const key = `11m-文件-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                const uploader = new Uploader(qiniuAdapter);
                await uploader.putObjectFromFile(
                  bucketRegionId,
                  {
                    bucket: bucketName,
                    key: key,
                  },
                  tmpfilePath,
                  (1 << 20) * 11,
                  originalFileName,
                  {
                    accelerateUploading: true,
                    putCallback: {
                      partsInitCallback: (info) => {
                          expect(info.uploadId).to.be.ok;
                          expect(info.parts).to.be.empty;
                      },
                      progressCallback: (p) => {
                          expect(p.total).to.equal((1 << 20) * 11);
                      },
                    },
                  }
                );

                // download to check
                const downloader = new Downloader(qiniuAdapter);
                const targetFilePath = tempfile();
                await downloader.getObjectToFile(
                  bucketRegionId,
                  {
                    bucket: bucketName,
                    key: key
                  },
                  targetFilePath
                );
                const md5FromSource = await new Promise((resolve, reject) => {
                    fs.readFile(tmpfilePath, { encoding: 'binary' }, (err, buf) => {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(md5(buf, { encoding: 'binary', asBytes: true }));
                    });
                });
                const md5FromObject = await new Promise((resolve, reject) => {
                    fs.readFile(targetFilePath, { encoding: 'binary' }, (err, buf) => {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(md5(buf, { encoding: 'binary', asBytes: true }));
                    });
                });
                expect(md5FromSource).to.eql(md5FromObject);
              });
            }
        });

        context('bucket', () => {
            it('creates a bucket and drops it', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);
                const bucketName = `test-bucket-${Math.floor(Math.random() * (2 ** 64 - 1))}`;
                await qiniuAdapter.createBucket(bucketRegionId, bucketName);
                const regionId = await qiniuAdapter.getBucketLocation(bucketName);
                expect(regionId).to.equal(bucketRegionId);

                await qiniuAdapter.deleteBucket(bucketRegionId, bucketName);
                try {
                    await qiniuAdapter.getBucketLocation(bucketName);
                    assert.fail();
                } catch {
                    // pass
                }
            });

            it('lists all buckets', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);

                const buckets = await qiniuAdapter.listBuckets();

                const bucket = buckets.find((bucket) => bucket.name === bucketName);
                expect(bucket?.regionId).to.equal(bucketRegionId);
                expect(bucket?.grantedPermission).to.be.undefined;
            });

            it('lists domain', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);

                const domains = await qiniuAdapter.listDomains(bucketRegionId, bucketName);
                if (mode === KODO_MODE) {
                    expect(domains).to.have.lengthOf.at.least(1);
                } else {
                    expect(domains).to.have.lengthOf.at.least(1);
                }
            });

            it('set remark', async () => {
                const qiniu = new Qiniu(accessKey, secretKey);
                const qiniuAdapter = qiniu.mode(mode);

                const remark = 'kodo-s3-adapter-sdk-test-remark';
                await qiniuAdapter.updateBucketRemark(bucketName, remark);

                const buckets = await qiniuAdapter.listBuckets();
                const bucket = buckets.find(bucket => bucket.name === bucketName);
                expect(bucket?.remark === remark);
            });
        });
    });
});
