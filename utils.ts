import { StorageClass } from './adapter';

export enum FileType {
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

export enum KodoS3StorageClass {
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
