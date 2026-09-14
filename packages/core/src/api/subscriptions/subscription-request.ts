import { DEFAULT_COOKIE_NAME } from '@vendure/common/lib/shared-constants';
import cookieSession from 'cookie-session';
import express, { Express, Request, Response } from 'express';
import { IncomingMessage } from 'http';

import { ConfigService } from '../../config/config.service';
import { CookieOptions } from '../../config/vendure-config';
import { I18nService } from '../../i18n/i18n.service';
import { ApiType } from '../common/get-api-type';
import { tokenMethodIncludes } from '../common/token-method-includes';

/**
 * Builds the `{ req, res }` context value for an operation which arrived over a WebSocket.
 *
 * The upgrade request is a Node `IncomingMessage`, so it is given the Express request prototype
 * and app instance: `get()`, `protocol`, `hostname`, `ip` and the query parser are then Express's
 * own, including `X-Forwarded-*` handling per the `trust proxy` setting. Only what the WebSocket
 * protocol carries differently is set explicitly, which leaves `extractSessionToken`, the
 * `AuthGuard`, `RequestContextService` and `@Ctx()` working unchanged.
 *
 * A connection carries many operations, so each one gets its own request object which inherits
 * from the upgrade request: the operation-specific state (`body`, `query`, `session`, and the
 * RequestContext the AuthGuard attaches) never leaks from one subscription to another.
 */
export function createSubscriptionRequest(options: {
    upgradeRequest: IncomingMessage;
    connectionParams: Record<string, unknown> | undefined;
    payload: { query?: string; variables?: Record<string, unknown> | null; operationName?: string | null };
    apiType: ApiType;
    configService: ConfigService;
    i18nService: I18nService;
    expressApp: Express;
}): { req: Request; res: Response } {
    const { upgradeRequest, connectionParams, payload, apiType, configService, i18nService } = options;
    const { channelTokenKey } = configService.apiOptions;
    const res = createInertResponse();

    // The prototype swap is idempotent, so it is safe to repeat for every operation on the connection.
    if (Object.getPrototypeOf(upgradeRequest) !== express.request) {
        Object.setPrototypeOf(upgradeRequest, express.request);
    }
    const req: any = Object.create(upgradeRequest);
    req.app = options.expressApp;
    req.res = res;
    (res as any).req = req;

    // A WebSocket client cannot set request headers, so it passes them as connection params.
    req.headers = { ...upgradeRequest.headers };
    const allowed = getAllowedConnectionParams(configService);
    for (const [key, value] of Object.entries(connectionParams ?? {})) {
        if (typeof value === 'string' && allowed.includes(key.toLowerCase())) {
            req.headers[key.toLowerCase()] = value;
        }
    }

    // ... and the values which an http client would pass as query params.
    const query = { ...req.query };
    for (const key of [channelTokenKey, 'languageCode', 'currencyCode']) {
        const value = connectionParams?.[key];
        if (typeof value === 'string') {
            query[key] = value;
        }
    }
    Object.defineProperty(req, 'query', { configurable: true, value: query });
    // Keeps the `@Relations()` decorator, which inspects the incoming query, working.
    req.body = {
        query: payload.query,
        variables: payload.variables,
        operationName: payload.operationName,
    };

    // Express never runs the middleware stack over an upgrade request, so the i18n language
    // detection and, where used, the `cookie-session` middleware (the browser does send the session
    // cookie) are run here. The latter gets a bare response object: given an Express-like one, the
    // `cookies` library would reach for Node's real `setHeader`.
    i18nService.handle()(req, res, () => undefined);
    const { tokenMethod, cookieOptions } = configService.authOptions;
    if (tokenMethodIncludes(tokenMethod, 'cookie')) {
        const name = getCookieName(cookieOptions, apiType);
        const bareResponse: any = {
            getHeader: () => undefined,
            setHeader: () => undefined,
            writeHead: () => undefined,
        };
        cookieSession({ ...cookieOptions, name })(req, bareResponse, () => undefined);
    }
    return { req: req as Request, res };
}

/** Mirrors the cookie name used by the middleware in `bootstrap.ts` and `app.module.ts`. */
function getCookieName(cookieOptions: CookieOptions, apiType: ApiType): string {
    const { name } = cookieOptions;
    if (typeof name === 'string') {
        return name;
    }
    if (name && (apiType === 'admin' || apiType === 'shop')) {
        return name[apiType];
    }
    return DEFAULT_COOKIE_NAME;
}

/** Nothing can be written back over a WebSocket, so whatever the pipeline sets is discarded. */
function createInertResponse(): Response {
    const res: any = {
        set: () => res,
        setHeader: () => res,
        getHeader: () => undefined,
        cookie: () => res,
    };
    return res as Response;
}

/** Connection params which may be applied to the request; anything else is ignored. */
function getAllowedConnectionParams(configService: ConfigService): string[] {
    const { channelTokenKey } = configService.apiOptions;
    const { apiKeyHeaderKey } = configService.authOptions;
    return ['authorization', 'accept-language', channelTokenKey, apiKeyHeaderKey]
        .filter((key): key is string => !!key)
        .map(key => key.toLowerCase());
}
