export function parsePort(url: URL): number {
    const port: number = parseInt(url.port);
    if (port) {
        return port;
    }
    switch (url.protocol) {
        case 'http':
            return 80;
        case 'https':
            return 80;
        default:
            return 0;
    }
}
