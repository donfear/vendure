import { CorsOptions } from '@nestjs/common/interfaces/external/cors-options.interface';
import cors from 'cors';
import { IncomingMessage } from 'http';

/**
 * Applies the configured CORS rules to a WebSocket upgrade request, which never passes through
 * the Express middleware stack. Without this, any web page could open an authenticated
 * subscription on behalf of a logged-in visitor, since browsers send cookies with a
 * cross-origin WebSocket connection and CORS does not apply to it.
 *
 * A request with no `Origin` header did not come from a browser, and is allowed.
 */
export function isOriginAllowed(
    request: IncomingMessage | undefined,
    corsOptions: boolean | CorsOptions,
): Promise<boolean> {
    const origin = request?.headers?.origin;
    if (!origin || corsOptions === true) {
        return Promise.resolve(true);
    }
    if (corsOptions === false) {
        return Promise.resolve(isSameOrigin(origin, request?.headers?.host));
    }
    return new Promise<boolean>(resolve => {
        const headers: Record<string, string> = {};
        const response = {
            setHeader: (name: string, value: string) => (headers[name] = value),
            getHeader: () => undefined,
            end: () => undefined,
        };
        try {
            // `cors` sets Access-Control-Allow-Origin only for an origin it allows.
            cors(corsOptions as cors.CorsOptions)(request, response, () => {
                resolve(!!headers['Access-Control-Allow-Origin']);
            });
        } catch {
            resolve(false);
        }
    });
}

function isSameOrigin(origin: string, host: string | undefined): boolean {
    if (!host) {
        return false;
    }
    try {
        return new URL(origin).host.toLowerCase() === host.trim().toLowerCase();
    } catch {
        return false;
    }
}
