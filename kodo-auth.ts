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
    accessKey: string,
    secretKey: string,
    requestURI: string,
    requestMethod: string,
    contentType?: string,
    headers?: Record<string, string>,
    requestBody?: string,
): string {
    const url: URL = new URL(requestURI);
    let data = `${requestMethod} ${url.pathname}${url.search}\nHost: ${url.host}\n`;

    contentType = contentType ?? '';
    if (contentType === 'application/json' || contentType === 'application/x-www-form-urlencoded') {
        data += `Content-Type: ${contentType}\n`;
    }

    if (headers) {
        const canonicalHeaders = Object.keys(headers)
            .reduce((acc: Record<string, string>, k) => {
                acc[canonicalMimeHeaderKey(k)] = headers[k];
                return acc;
            }, {});
        const headerText = Object.keys(canonicalHeaders)
            .filter(k => {
                return k.startsWith('X-Qiniu-') && k.length > 'X-Qiniu-'.length;
            })
            .sort()
            .map(k => {
                return k + ': ' + canonicalHeaders[k];
            })
            .join('\n');
        if (headerText) {
            data += `${headerText}\n`;
        }
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

export function getXQiniuDate(date: Date = new Date()) {
    let result = '';
    result += date.getUTCFullYear();
    result += (date.getUTCMonth() + 1).toString().padStart(2, '0');
    result += date.getUTCDate().toString().padStart(2, '0');
    result += 'T';
    result += date.getUTCHours().toString().padStart(2, '0');
    result += date.getUTCMinutes().toString().padStart(2, '0');
    result += date.getUTCSeconds().toString().padStart(2, '0');
    result += 'Z';

    return result;
}

const isTokenTable: Record<string, boolean> = {
    '!': true,
    '#': true,
    $: true,
    '%': true,
    '&': true,
    '\\': true,
    '*': true,
    '+': true,
    '-': true,
    '.': true,
    0: true,
    1: true,
    2: true,
    3: true,
    4: true,
    5: true,
    6: true,
    7: true,
    8: true,
    9: true,
    A: true,
    B: true,
    C: true,
    D: true,
    E: true,
    F: true,
    G: true,
    H: true,
    I: true,
    J: true,
    K: true,
    L: true,
    M: true,
    N: true,
    O: true,
    P: true,
    Q: true,
    R: true,
    S: true,
    T: true,
    U: true,
    W: true,
    V: true,
    X: true,
    Y: true,
    Z: true,
    '^': true,
    _: true,
    '`': true,
    a: true,
    b: true,
    c: true,
    d: true,
    e: true,
    f: true,
    g: true,
    h: true,
    i: true,
    j: true,
    k: true,
    l: true,
    m: true,
    n: true,
    o: true,
    p: true,
    q: true,
    r: true,
    s: true,
    t: true,
    u: true,
    v: true,
    w: true,
    x: true,
    y: true,
    z: true,
    '|': true,
    '~': true
};

function validHeaderKeyChar(ch: string): boolean {
    if (ch.charCodeAt(0) >= 128) {
        return false;
    }
    return isTokenTable[ch];
}

function canonicalMimeHeaderKey(fieldName: string) {
    for (const ch of fieldName) {
        if (!validHeaderKeyChar(ch)) {
            return fieldName;
        }
    }
    return fieldName.split('-')
        .map(function (text) {
            return text.substring(0, 1).toUpperCase() + text.substring(1).toLowerCase();
        })
        .join('-');
}
