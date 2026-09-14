import { CorsOptions } from '@nestjs/common/interfaces/external/cors-options.interface';
import cors from 'cors';
import { Request } from 'express';

/**
 * Applies the configured CORS rules to a WebSocket upgrade request, which never passes through
 * the Express middleware stack. Without this, any web page could open an authenticated
 * subscription on behalf of a logged-in visitor, since browsers send cookies with a
 * cross-origin WebSocket connection and CORS does not apply to it.
 *
 * A request with no `Origin` header did not come from a browser, and is allowed. The `cors`
 * package decides the rest, exactly as for an http request: an origin is allowed when the
 * `Access-Control-Allow-Origin` it would respond with names that origin, and - for a connection
 * which carries cookies - `Access-Control-Allow-Credentials` is set, since the browser would not
 * let a credentialed cross-origin http request read the response otherwise.
 */
export function isOriginAllowed(request: Request, corsOptions: boolean | CorsOptions): Promise<boolean> {
    const origin = request.headers.origin;
    if (!origin) {
        return Promise.resolve(true);
    }
    if (corsOptions === false) {
        return Promise.resolve(isSameOrigin(origin, request.host));
    }
    return new Promise<boolean>(resolve => {
        const headers: Record<string, string> = {};
        const response = {
            setHeader: (name: string, value: string) => (headers[name] = value),
            getHeader: () => undefined,
            end: () => undefined,
        };
        const options = corsOptions === true ? {} : (corsOptions as cors.CorsOptions);
        try {
            cors(options)(request, response as any, () => {
                const allowedOrigin = headers['Access-Control-Allow-Origin'];
                const credentialed = !!request.headers.cookie;
                const allowsCredentials = headers['Access-Control-Allow-Credentials'] === 'true';
                resolve(
                    (allowedOrigin === origin && (!credentialed || allowsCredentials)) ||
                        (allowedOrigin === '*' && !credentialed),
                );
            });
        } catch {
            resolve(false);
        }
    });
}

/** `request.host` honours `X-Forwarded-Host` per the Express `trust proxy` setting. */
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
