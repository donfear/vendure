import { DEFAULT_COOKIE_NAME } from '@vendure/common/lib/shared-constants';
import cookieSession from 'cookie-session';
import express, { Express, Handler, Request, Response } from 'express';
import { IncomingMessage } from 'http';

import { ConfigService } from '../../config/config.service';
import { CookieOptions } from '../../config/vendure-config';
import { ApiType } from '../common/get-api-type';
import { tokenMethodIncludes } from '../common/token-method-includes';

/**
 * Gives the upgrade request - a Node `IncomingMessage` - the Express request prototype and app
 * instance, so that `get()`, `protocol`, `host`, `hostname`, `ip` and the query parser are
 * Express's own, including `X-Forwarded-*` handling per the `trust proxy` setting. Idempotent.
 */
export function asExpressRequest(upgradeRequest: IncomingMessage, app: Express): Request {
    if (Object.getPrototypeOf(upgradeRequest) !== express.request) {
        Object.setPrototypeOf(upgradeRequest, express.request);
    }
    (upgradeRequest as any).app = app;
    return upgradeRequest as Request;
}

/**
 * The middleware which Express would run over an http request but never runs over an upgrade
 * request, and which the request pipeline relies on: i18n language detection, and the
 * `cookie-session` middleware where the cookie token method is used (the browser does send the
 * session cookie with the upgrade request). Built once per API.
 */
export function createSubscriptionMiddleware(options: {
    apiType: ApiType;
    configService: ConfigService;
    i18nHandler: Handler;
}): Handler[] {
    const { tokenMethod, cookieOptions } = options.configService.authOptions;
    const middleware = [options.i18nHandler];
    if (tokenMethodIncludes(tokenMethod, 'cookie')) {
        const name = getCookieName(cookieOptions, options.apiType);
        middleware.push(cookieSession({ ...cookieOptions, name }));
    }
    return middleware;
}

/**
 * Builds the `{ req, res }` context value for an operation which arrived over a WebSocket.
 *
 * A connection carries many operations, so each one gets its own request object which inherits
 * from the upgrade request: the operation-specific state (`body`, `query`, `session`, and the
 * RequestContext the AuthGuard attaches) never leaks from one subscription to another. Only what
 * the WebSocket protocol carries differently is set explicitly, which leaves
 * `extractSessionToken`, the `AuthGuard`, `RequestContextService` and `@Ctx()` working unchanged.
 */
export function createSubscriptionRequest(options: {
    upgradeRequest: Request;
    connectionParams: Record<string, unknown> | undefined;
    payload: { query?: string; variables?: Record<string, unknown> | null; operationName?: string | null };
    configService: ConfigService;
    middleware: Handler[];
}): { req: Request; res: Response } {
    const { upgradeRequest, connectionParams, payload, configService, middleware } = options;
    const { channelTokenKey } = configService.apiOptions;
    const res = createInertResponse();
    const req: any = Object.create(upgradeRequest);
    req.res = res;
    (res as any).req = req;

    // A WebSocket client cannot set request headers, so it passes them as connection params ...
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

    // The middleware gets a bare response: given an Express-like one with `set()`, the `cookies`
    // library reaches for Node's real `setHeader`, which a plain object cannot satisfy.
    const bareResponse: any = {
        getHeader: () => undefined,
        setHeader: () => undefined,
        writeHead: () => undefined,
    };
    for (const handler of middleware) {
        handler(req, bareResponse, () => undefined);
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
        locals: {},
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
