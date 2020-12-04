import { createHmac, Hmac } from 'crypto';
import { URL } from 'url';

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
