import { StorageClass } from './adapter';

export function parsePort(url: URL): number {
    const port: number = parseInt(url.port);
    if (port) {
        return port;
    }
    switch (url.protocol) {
        case 'http':
            return 80;
        case 'https':
            return 80;
        default:
            return 0;
    }
}

enum FileType {
    Standard = 0,
    InfrequentAccess,
    Glacier,
}

export function convertStorageClassToFileType(storageClass?: StorageClass): FileType {
    switch (storageClass) {
        case 'Standard':
            return FileType.Standard;
        case 'InfrequentAccess':
            return FileType.InfrequentAccess;
        case 'Glacier':
            return FileType.Glacier;
        default:
            return FileType.Standard;
    }
}

enum KodoS3StorageClass {
    Standard = 'STANDARD',
    InfrequentAccess = 'LINE',
    Glacier = 'GLACIER',
}

export function covertStorageClassToS3StorageClass(storageClass?: StorageClass): KodoS3StorageClass {
    switch (storageClass) {
        case 'Standard':
            return KodoS3StorageClass.Standard;
        case 'InfrequentAccess':
            return KodoS3StorageClass.InfrequentAccess;
        case 'Glacier':
            return KodoS3StorageClass.Glacier;
        default:
            return KodoS3StorageClass.Standard;
    }
}
