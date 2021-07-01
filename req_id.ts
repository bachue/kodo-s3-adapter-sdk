import { HttpMethod } from 'urllib';
import { IncomingHttpHeaders } from 'http';
import hash from 'object-hash';
import Timestamp from 'timestamp-nano';

interface KodoRequest {
	url?: string;
	method?: HttpMethod | string;
	contentType?: string;
	dataType?: string;
	headers?: IncomingHttpHeaders;
}

export function generateReqId(request: KodoRequest): string {
	const t = Timestamp.fromDate(new Date());
	const value = {
		'secs': t.getTimeT(), 'nsecs': t.getNano(), 'r': Math.random(),
	};
	Object.assign(value, request);
	return hash(value);
}
