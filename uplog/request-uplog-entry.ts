import { ErrorType, UplogEntry } from './fields';

export interface RequestUplogEntry extends UplogEntry {
    status_code?: number;
    req_id?: string;
    host: string;
    port: number;
    method: string;
    path: string;
    remote_ip?: string;
    total_elapsed_time: number;
    bytes_sent?: number;
    error_type?: ErrorType;
    error_description?: string;
}
