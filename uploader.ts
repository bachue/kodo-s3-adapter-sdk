import { Adapter, SetObjectHeader, Part } from './adapter';
import { FileHandle } from 'fs/promises';

export class Uploader {
    constructor(private adapter: Adapter) {
    }

    putObjectFromFile(_region: string, _object: Object, _file: FileHandle, _putCallback?: PutCallback): Promise<void> {
        return new Promise((resolve) => {
            console.log(this.adapter);
            resolve();
        });
    }
}

export interface PutCallback {
    progressCallback?: (uploaded: number, total: number) => void;
    partPutCallback?: (part: Part) => void;
}

export interface PutFileOption {
    header?: SetObjectHeader;
    recovered?: Array<Part>;
    putCallback?: PutCallback;
}
