import process from 'process';
import fs from 'fs';
import { expect, assert } from 'chai';
import {
    UplogBufferFilePath,
    UplogBuffer,
    GenRequestUplogEntry,
} from '../uplog';

const fsPromises = fs.promises;

process.on('uncaughtException', (err: any, origin: any) => {
    fs.writeSync(
        process.stderr.fd,
        `Caught exception: ${err}\n` +
        `Exception origin: ${origin}`
    );
    assert.fail();
});

describe('UplogBuffer', () => {
    context('Uplog buffer write', () => {
        it('writes multiple logs into buffer and read them out', async () => {
            await fsPromises.truncate(UplogBufferFilePath);

            let onBufferFullCallbackTimes = 0, buffer = '';
            const uplogBuffer = new UplogBuffer({
                bufferSize: 100,
                onBufferFull: (chunk): Promise<void> => {
                    buffer += chunk.toString();
                    onBufferFullCallbackTimes += 1;
                    return Promise.resolve();
                },
            });
            const uplogMaker = new GenRequestUplogEntry(
                'fakeApiName',
                {
                    apiType: 'kodo',
                    httpVersion: '2',
                    method: 'GET',
                    sdkName: 'fakeAppName',
                    sdkVersion: 'fakeAppVersion',
                    targetBucket: 'fakeBucket',
                    targetKey: 'fake/key',
                    url: new URL('https://fake.qiniu.com/uplog/url'),
                }
            );

            const logRequest1 = uplogBuffer.log(uplogMaker.getRequestUplogEntry({
                bytesReceived: 0,
                bytesSent: 0,
                costDuration: 20,
                remoteIp: '127.0.0.1',
                reqId: 'fakeReqId',
                statusCode: 200,
            }));
            const logRequest2 = uplogBuffer.log(uplogMaker.getRequestUplogEntry({
                bytesReceived: 0,
                bytesSent: 0,
                costDuration: 21,
                remoteIp: '127.0.0.2',
                reqId: 'fakeReqId',
                statusCode: 200,
            }));
            await Promise.all([logRequest1, logRequest2]);
            const uplogBufferFileStat = await fsPromises.stat(UplogBufferFilePath);
            expect(uplogBufferFileStat.size).to.equal(0);
            expect(onBufferFullCallbackTimes).to.equal(1);
            const logEntries = buffer.split('\n').filter(line => line.length > 0).map(line => JSON.parse(line));
            expect(logEntries).to.have.lengthOf(2);
            expect(logEntries[0].log_type).to.equal('request');
            expect(logEntries[0].status_code).to.equal(200);
            expect(logEntries[0].remote_ip).to.equal('127.0.0.1');
            expect(logEntries[0].sdk_name).to.equal('fakeAppName');
            expect(logEntries[0].sdk_version).to.equal('fakeAppVersion');
            expect(logEntries[0]).to.have.property('up_time');
            expect(logEntries[1].log_type).to.equal('request');
            expect(logEntries[1].status_code).to.equal(200);
            expect(logEntries[1].remote_ip).to.equal('127.0.0.2');
            expect(logEntries[1].sdk_name).to.equal('fakeAppName');
            expect(logEntries[1].sdk_version).to.equal('fakeAppVersion');
            expect(logEntries[1]).to.have.property('up_time');
        });
    });
});
