import { createHmac, Hmac } from 'crypto';
import { URL } from 'url';
import { encode as base64Encode } from 'js-base64';

function hmacSha1(data: string, secretKey: string): string {
    const hmac: Hmac = createHmac('sha1', secretKey);
    hmac.update(data);
    return hmac.digest('base64');
}

export function base64ToUrlSafe(v: string): string {
    return v.replace(/\//g, '_').replace(/\+/g, '-');
}

export function generateAccessTokenV2(
    accessKey: string, secretKey: string, requestURI: string, requestMethod: string,
    contentType?: string, requestBody?: string): string {
    const url: URL = new URL(requestURI);
    let data = `${requestMethod} ${url.pathname}${url.search}\nHost: ${url.host}\n`;

    contentType = contentType ?? '';
    if (contentType === 'application/json' || contentType === 'application/x-www-form-urlencoded') {
        data += `Content-Type: ${contentType}\n`;
    }

    requestBody = requestBody ?? '';
    data += `\n${requestBody}`;

    return `Qiniu ${accessKey}:${base64ToUrlSafe(hmacSha1(data, secretKey))}`;
}

export interface UploadPolicy {
    scope: string;
    deadline: number;
}

export function NewUploadPolicy(bucket: string, key?: string, deadline?: Date): UploadPolicy {
    let scope = bucket;
    if (key) {
        scope += `:${key}`;
    }
    deadline ??= new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);

    return {
        scope: scope,
        deadline: ~~(deadline.getTime() / 1000),
    };
}

export function MakeUploadToken(accessKey: string, secretKey: string, uploadPolicy: UploadPolicy): string {
    const data = base64ToUrlSafe(base64Encode(JSON.stringify(uploadPolicy)));
    const sign = base64ToUrlSafe(hmacSha1(data, secretKey));
    return `${accessKey}:${sign}:${data}`;
}
