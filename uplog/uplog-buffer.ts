import path from 'path';
import os from 'os';
import fs from 'fs';
import { UplogEntry } from './fields';
import lockFile from 'lockfile';
import pkg from '../package.json';

export interface UplogOption {
    appName?: string;
    appVersion?: string;
    bufferSize?: number;
    onBufferFull?: (buffer: Buffer) => Promise<void>,
}

export const UplogBufferFilePath = path.join(os.homedir(), '.kodo-s3-adapter-sdk', 'uplog-buffer');
export const UplogBufferFileLockPath = path.join(os.homedir(), '.kodo-s3-adapter-sdk', 'uplog-buffer.lock');

export class UplogBuffer {
    private static uploadBufferedEntries: string[] = [];
    private static uploadBufferFd: number | undefined = undefined;

    constructor(private readonly option: UplogOption) {
        if (!UplogBuffer.uploadBufferFd) {
            const dirPath = path.dirname(UplogBufferFilePath);
            if (!fs.existsSync(dirPath)) {
                fs.mkdirSync(dirPath, { recursive: true, mode: 0o700 });
            }
            UplogBuffer.uploadBufferFd = fs.openSync(UplogBufferFilePath, 'a', 0o600);
        }
    }

    async log(entry: UplogEntry): Promise<void> {
        if (this.option.bufferSize && this.option.bufferSize <= 0) {
            return;
        }
        UplogBuffer.uploadBufferedEntries.push(this.convertUplogEntryToJSON(entry) + '\n');
        const fileSize = await this.flushBufferToLogFile();
        if (fileSize && fileSize >= (this.option.bufferSize ?? 1 << 20)) {
            await this.exportLogs();
        }
    }

    private async flushBufferToLogFile(): Promise<number | undefined> {
        try {
            await new Promise<any>((resolve, reject) => {
                lockFile.lock(UplogBufferFileLockPath, this.lockOptions(), err => !err ? resolve() : reject(err));
            });
        } catch (err) {
            if (err?.code === 'EEXIST') {
                return;
            }
            console.warn('locked fail:', err);
            throw err;
        }

        const uploadBufferedEntries = UplogBuffer.uploadBufferedEntries;
        UplogBuffer.uploadBufferedEntries = [];

        let stats: fs.Stats;
        try {
            for (const data of uploadBufferedEntries) {
                await new Promise((resolve, reject) => {
                    fs.write(UplogBuffer.uploadBufferFd!, data, err => !err ? resolve() : reject(err));
                });
            }
            stats = await new Promise<fs.Stats>((resolve, reject) => {
                fs.fstat(UplogBuffer.uploadBufferFd!, (err, stats) => {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve(stats);
                });
            });
        } catch (err) {
            await new Promise(resolve => {
                lockFile.unlock(UplogBufferFileLockPath, resolve);
            });
            throw err;
        }

        await new Promise((resolve, reject) => {
            lockFile.unlock(UplogBufferFileLockPath, (err) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve();
            });
        });
        return stats.size;
    }

    private async exportLogs(): Promise<void> {
        if (!this.option?.onBufferFull) {
            return;
        }
        try {
            await new Promise<any>((resolve, reject) => {
                lockFile.lock(UplogBufferFileLockPath, this.lockOptions(), err => !err ? resolve() : reject(err));
            });
        } catch (err) {
            if (err?.code === 'EEXIST') {
                return;
            }
            console.warn('locked fail:', err);
            throw err;
        }

        try {
            const buffer = await new Promise<Buffer>((resolve, reject) => {
                fs.readFile(UplogBufferFilePath, (err, buffer) => {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve(buffer);
                });
            });
            if (this.option.onBufferFull) {
                await this.option.onBufferFull(buffer);
                await new Promise((resolve, reject) => {
                    fs.truncate(UplogBufferFilePath, (err) => {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve();
                    });
                });
            }
        } catch (err) {
            await new Promise(resolve => {
                lockFile.unlock(UplogBufferFileLockPath, resolve);
            });
            throw err;
        }
        await new Promise((resolve, reject) => {
            lockFile.unlock(UplogBufferFileLockPath, (err) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve();
            });
        });
    }

    private convertUplogEntryToJSON(entry: UplogEntry) {
        entry.os_name = os.platform();
        entry.os_version = os.release();
        entry.sdk_name = this.option.appName;
        entry.sdk_version = this.option.appVersion;
        entry.http_client = 'kodo-s3-adapter-sdk';
        entry.http_client_version = pkg.version;
        entry.up_time = Math.trunc(Date.now() / 1000);
        return JSON.stringify(entry);
    }

    private lockOptions(): lockFile.Options {
        return { retries: 10, retryWait: 100 };
    }

    public static async forceUnlock(): Promise<Error | null> {
        return await new Promise((resolve, reject) => {
            lockFile.unlock(UplogBufferFileLockPath, (err) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve();
            });
        });
    }
}
