const os = require('os');
const pkg = require('./package.json');

const QINIU_MODE = 'qiniu';
const S3_MODE = 's3';
const USER_AGENT = `Qiniu-Kodo-S3-Adapter-NodeJS-SDK/${pkg.version} (${os.type()}; ${os.platform()}; ${os.arch();}; )`;

class Qiniu {
    constructor(accessKey, secret_key) {
        this.accessKey = accessKey;
        this.secret_key = secret_key;
        this.userAgent = USER_AGENT;
        this.adapters = {};
        this.regions = [];
    }

    register(modeName, adapter) {
        this.adapters[modeName] = adapter;
    }

    mode(modeName) {
        const Adapter = this.adapters[modeName];
        if (!Adapter) {
            throw new Error(`Invalid qiniu mode: ${modeName}`);
        }
        return new Adapter({
            accessKey: this.accessKey,
            secretKey: this.secretKey,
            userAgent: this.userAgent,
        });
    }
}

exports.QINIU_MODE = QINIU_MODE;
exports.S3_MODE = S3_MODE;
exports.USER_AGENT = USER_AGENT;
exports.Qiniu = Qiniu;
