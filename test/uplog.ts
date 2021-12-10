import process from 'process';
import fs from 'fs';
import { expect, assert } from 'chai';
import { UplogBufferFilePath, UplogBuffer, RequestUplogEntry, LogType } from '../uplog';

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
            const logRequest1 = uplogBuffer.log({
                log_type: LogType.Request,
                status_code: 200,
                host: 'fake.host.1',
                port: 80,
                method: 'GET',
                total_elapsed_time: 20,
            } as RequestUplogEntry);
            const logRequest2 = uplogBuffer.log({
                log_type: LogType.Request,
                status_code: 200,
                host: 'fake.host.2',
                port: 80,
                method: 'GET',
                total_elapsed_time: 21,
            } as RequestUplogEntry);
            const logRequest3 = uplogBuffer.log({
                log_type: LogType.Request,
                status_code: 200,
                host: 'fake.host.3',
                port: 80,
                method: 'GET',
                total_elapsed_time: 21,
            } as RequestUplogEntry);
            const logRequest4 = uplogBuffer.log({
                log_type: LogType.Request,
                status_code: 200,
                host: 'fake.host.4',
                port: 80,
                method: 'GET',
                total_elapsed_time: 21,
            } as RequestUplogEntry);
            await Promise.all([logRequest1, logRequest2, logRequest3, logRequest4]);
            const uplogBufferFileStat = await fsPromises.stat(UplogBufferFilePath);
            expect(uplogBufferFileStat.size).to.equal(0);
            expect(onBufferFullCallbackTimes).to.equal(1);
            const logEntries = buffer.split('\n').filter(line => line.length > 0).map(line => JSON.parse(line));
            expect(logEntries).to.have.lengthOf(4);
            expect(logEntries[0].log_type).to.equal('request');
            expect(logEntries[0].status_code).to.equal(200);
            expect(logEntries[0].host).to.equal('fake.host.1');
            expect(logEntries[0].sdk_name).to.equal('fakeAppName');
            expect(logEntries[0].sdk_version).to.equal('fakeAppVersion');
            expect(logEntries[0]).to.have.property('up_time');
            expect(logEntries[1].log_type).to.equal('request');
            expect(logEntries[1].status_code).to.equal(200);
            expect(logEntries[1].host).to.equal('fake.host.2');
            expect(logEntries[1].sdk_name).to.equal('fakeAppName');
            expect(logEntries[1].sdk_version).to.equal('fakeAppVersion');
            expect(logEntries[1]).to.have.property('up_time');
            expect(logEntries[2].log_type).to.equal('request');
            expect(logEntries[2].status_code).to.equal(200);
            expect(logEntries[2].host).to.equal('fake.host.3');
            expect(logEntries[2].sdk_name).to.equal('fakeAppName');
            expect(logEntries[2].sdk_version).to.equal('fakeAppVersion');
            expect(logEntries[2]).to.have.property('up_time');
            expect(logEntries[3].log_type).to.equal('request');
            expect(logEntries[3].status_code).to.equal(200);
            expect(logEntries[3].host).to.equal('fake.host.4');
            expect(logEntries[3].sdk_name).to.equal('fakeAppName');
            expect(logEntries[3].sdk_version).to.equal('fakeAppVersion');
            expect(logEntries[3]).to.have.property('up_time');
        });
    });
});
