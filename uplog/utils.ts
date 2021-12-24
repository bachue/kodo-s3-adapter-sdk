import { ErrorType } from './fields';

export const getErrorTypeFromStatusCode = (statusCode: number): ErrorType => {
    if (statusCode > 399 && statusCode < 500
        || [573, 579, 608, 612, 614, 630, 631, 701].includes(statusCode)
    ) {
        return ErrorType.BadRequest;
    } else {
        return ErrorType.ResponseError;
    }
};

export const getErrorTypeFromRequestError = (err: any): ErrorType => {
    switch (err.code) {
        case 'ENOTFOUND':
            return ErrorType.UnknownHost;
        case 'ECONNREFUSED':
            return ErrorType.CannotConnectToHost;
        case 'ECONNRESET':
            return ErrorType.CannotConnectToHost;
        case 'EMFILE':
            return ErrorType.UnexpectedSyscallError;
        case 'EACCES':
            return ErrorType.UnexpectedSyscallError;
        case 'ETIMEDOUT':
            return ErrorType.Timeout;
        case 'EPIPE':
            return ErrorType.TransmissionError;
        case 'EPROTO':
            return ErrorType.NetworkError;
        case 'UNABLE_TO_VERIFY_LEAF_SIGNATURE':
            return ErrorType.SslError;
    }
    if (err.name && err.name.endsWith('TimeoutError')) {
        return ErrorType.Timeout;
    }
    switch (err.name) {
        case 'JSONResponseFormatError':
            return ErrorType.ParseError;
        default:
            return ErrorType.UnknownError;
    }
};

export const getErrorTypeFromS3Error = (err: any): ErrorType => {
    switch (err.code) {
        case 'TimeoutError':
            return ErrorType.Timeout;
        case 'NetworkingError':
            return ErrorType.NetworkError;
        case 'UnknownEndpoint':
            return ErrorType.UnknownHost;
        case 'XMLParserError':
            return ErrorType.ParseError;
        case 'CredentialsError':
            return ErrorType.BadRequest;
        case 'InvalidHeader':
            return ErrorType.BadRequest;
        case 'InvalidParameter':
            return ErrorType.BadRequest;
        case 'InvalidDigest':
            return ErrorType.BadRequest;
        case 'RequestAbortedError':
            return ErrorType.UserCanceled;
        default:
            return ErrorType.UnknownError;
    }
};
