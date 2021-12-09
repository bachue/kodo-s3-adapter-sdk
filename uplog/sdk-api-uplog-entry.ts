import { ErrorType, UplogEntry } from './fields';

export interface SdkApiUplogEntry extends UplogEntry {
    api_name: string;
    total_elapsed_time: number;
    requests_count: number;
    error_type?: ErrorType;
    error_description?: string;
}
