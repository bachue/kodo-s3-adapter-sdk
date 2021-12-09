import { ErrorType, LogType } from './fields';

export interface SdkApiUplogEntry {
    log_type: LogType;
    os_name?: string;
    os_version?: string;
    sdk_name?: string;
    sdk_version?: string;
    http_client?: string;
    http_client_version?: string;
    up_time?: number;
    api_name: string;
    total_elapsed_time: number;
    requests_count: number;
    error_type?: ErrorType;
    error_description?: string;
}
