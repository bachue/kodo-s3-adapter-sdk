import { createHmac, Hmac } from 'crypto';
import { URL } from 'url';
import { encode as base64Encode } from 'js-base64';
import { StorageClass } from './adapter';

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
    fileType?: number;
}

export function newUploadPolicy({
    bucket,
    key,
    deadline,
    fileType,
}: {
    bucket: string,
    key?: string,
    deadline?: Date,
    fileType?: StorageClass['fileType'],
}): UploadPolicy {
    let scope = bucket;
    if (key) {
        scope += `:${key}`;
    }
    deadline ??= new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);

    return {
        scope,
        deadline: Math.floor(deadline.getTime() / 1000),
        fileType: fileType,
    };
}

export function makeUploadToken(accessKey: string, secretKey: string, uploadPolicy: UploadPolicy): string {
    const data = base64ToUrlSafe(base64Encode(JSON.stringify(uploadPolicy)));
    const sign = base64ToUrlSafe(hmacSha1(data, secretKey));
    return `${accessKey}:${sign}:${data}`;
}

export function signPrivateURL(accessKey: string, secretKey: string, baseURL: URL, deadline?: Date): URL {
    let baseURLString = baseURL.toString();
    deadline ??= new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);
    const deadlineTimestamp = ~~(deadline.getTime() / 1000);

    baseURLString += `?e=${deadlineTimestamp}`;
    const sign = base64ToUrlSafe(hmacSha1(baseURLString, secretKey));
    const token = `${accessKey}:${sign}`;
    baseURLString += `&token=${token}`;
    return new URL(baseURLString);
}
