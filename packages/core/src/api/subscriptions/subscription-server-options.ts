import { ID } from '@vendure/common/lib/shared-types';
import { Express, Request } from 'express';
import {
    ExecutionArgs,
    GraphQLError,
    GraphQLSchema,
    parse,
    specifiedRules,
    validate,
    ValidationContext,
} from 'graphql';
import { ServerOptions } from 'graphql-ws';

import { idsAreEqual } from '../../common/utils';
import { ConfigService } from '../../config/config.service';
import { Logger } from '../../config/logger/vendure-logger';
import { CachedSession } from '../../config/session-cache/session-cache-strategy';
import { I18nService } from '../../i18n/i18n.service';
import { SessionService } from '../../service/services/session.service';
import { ApiType } from '../common/get-api-type';
import { GraphqlValueTransformer } from '../common/graphql-value-transformer';
import { IdCodecService } from '../common/id-codec.service';
import { internal_getRequestContext } from '../common/request-context';
import { encodeIdsInResult, prefixAssetUrlsInResult } from '../common/result-transformers';

import { isOriginAllowed } from './origin-check';
import {
    asExpressRequest,
    createSubscriptionMiddleware,
    createSubscriptionRequest,
} from './subscription-request';
import { subscriptionOnlyRule } from './validation-rules';

const loggerCtx = 'Subscriptions';

/** A user-supplied `cors.origin` callback which never answers must not hang the handshake. */
const ORIGIN_CHECK_TIMEOUT_MS = 5000;
/** Connection params are credentials and tokens; anything longer is not one. */
const MAX_CONNECTION_PARAM_LENGTH = 4096;
/**
 * How often, at most, an open subscription re-reads its session. Bounds the cost of a burst of
 * events to one session lookup per subscriber, rather than one per delivered event.
 */
export const SESSION_REVALIDATION_INTERVAL_MS = 1000;

const SESSION_VALIDATED_AT = Symbol('sessionValidatedAt');
type SubscriptionRequest = Request & { [SESSION_VALIDATED_AT]?: number };

/**
 * The `graphql-ws` options passed to the Nest GraphQL module, which apply the Vendure request
 * pipeline to each operation arriving over a WebSocket connection.
 *
 * The Apollo Server plugins which transform an http response (`willSendResponse`) never run for
 * a WebSocket event, so `onNext` applies the same id encoding and asset url transformations.
 */
export function createSubscriptionServerOptions(options: {
    schema: GraphQLSchema;
    apiType: ApiType;
    /** The validation rules of the corresponding http API. */
    validationRules: Array<(context: ValidationContext) => any>;
    /** Whether a non-default EntityIdStrategy is configured, as decided for the http API. */
    encodeIds: boolean;
    configService: ConfigService;
    i18nService: I18nService;
    idCodecService: IdCodecService;
    sessionService: SessionService;
    expressApp: Express;
}): ServerOptions {
    const { schema, apiType, encodeIds, configService, i18nService, idCodecService, sessionService } =
        options;
    const { maxSubscriptionsPerConnection, maxOperationSizeBytes } = configService.apiOptions.subscriptions;
    const graphqlValueTransformer = new GraphqlValueTransformer(schema);
    const validationRules = [...specifiedRules, ...options.validationRules, subscriptionOnlyRule];
    const { assetStorageStrategy } = configService.assetOptions;
    const toAbsoluteUrl = assetStorageStrategy.toAbsoluteUrl?.bind(assetStorageStrategy);
    const middleware = createSubscriptionMiddleware({
        apiType,
        configService,
        i18nHandler: i18nService.handle(),
    });
    const upgradeRequestOf = (ctx: { extra: unknown }) =>
        asExpressRequest((ctx.extra as { request: Request }).request, options.expressApp);

    return {
        onConnect: async ctx => {
            const request = upgradeRequestOf(ctx);
            const tooLong = Object.values(ctx.connectionParams ?? {}).some(
                value => typeof value === 'string' && value.length > MAX_CONNECTION_PARAM_LENGTH,
            );
            if (tooLong) {
                return false;
            }
            let timeout: NodeJS.Timeout | undefined;
            const allowed = await Promise.race([
                isOriginAllowed(request, configService.apiOptions.cors),
                new Promise<boolean>(resolve => {
                    timeout = setTimeout(() => resolve(false), ORIGIN_CHECK_TIMEOUT_MS);
                }),
            ]).finally(() => clearTimeout(timeout));
            if (!allowed) {
                Logger.verbose(
                    `Rejected a WebSocket connection from the disallowed origin "${request.headers.origin ?? ''}"`,
                    loggerCtx,
                );
            }
            return allowed;
        },
        onSubscribe: (ctx, id, payload) => {
            // The operation being checked is already counted.
            if (Object.keys(ctx.subscriptions).length > maxSubscriptionsPerConnection) {
                return [
                    new GraphQLError(
                        `Cannot open more than ${maxSubscriptionsPerConnection} concurrent ` +
                            'subscriptions on a single connection',
                        { extensions: { code: 'SUBSCRIPTION_LIMIT_EXCEEDED' } },
                    ),
                ];
            }
            if (Buffer.byteLength(JSON.stringify(payload)) > maxOperationSizeBytes) {
                return [
                    new GraphQLError('The subscription operation is too large', {
                        extensions: { code: 'SUBSCRIPTION_OPERATION_TOO_LARGE' },
                    }),
                ];
            }
            // graphql-ws parses the query itself, but lets a syntax error escape and close the
            // whole connection, taking every other subscription on it down too.
            try {
                parse(payload.query);
            } catch (e) {
                return [e as GraphQLError];
            }
        },
        validate: (validationSchema, document) =>
            validate(validationSchema, document, validationRules as any),
        context: (ctx, id, payload) => {
            const contextValue = createSubscriptionRequest({
                upgradeRequest: upgradeRequestOf(ctx),
                connectionParams: ctx.connectionParams,
                payload,
                configService,
                middleware,
            });
            // The AuthGuard is about to validate the session; the clock for re-reading it starts now.
            (contextValue.req as SubscriptionRequest)[SESSION_VALIDATED_AT] = Date.now();
            return contextValue;
        },
        onNext: async (ctx, id, payload, args: ExecutionArgs, result) => {
            const req = (args.contextValue as { req: Request }).req;
            // An errors-only result is the AuthGuard refusing the subscription, possibly before a
            // RequestContext existed; there is nothing to guard or transform in it.
            if (result.data) {
                await assertSessionStillValid(req);
                if (encodeIds) {
                    encodeIdsInResult(graphqlValueTransformer, idCodecService, args.document, result.data);
                }
                prefixAssetUrlsInResult(
                    graphqlValueTransformer,
                    toAbsoluteUrl,
                    req,
                    args.document,
                    result.data,
                );
            }
            if (result.errors?.length) {
                result.errors = result.errors.map(error => i18nService.translateError(req, error));
            }
            return result;
        },
        onError: (ctx, id, payload, errors) => {
            // Reached by a failed stream (relay lost, buffer overflow, a throwing mapFn) as well
            // as by a rejected subscribe; the http path logs these through Apollo, so log here.
            Logger.verbose(
                `Subscription "${payload.operationName ?? id}" failed: ${errors.map(e => e.message).join('; ')}`,
                loggerCtx,
            );
        },
    };

    /**
     * The AuthGuard runs once, when the client subscribes. A session which has since expired or
     * been invalidated (logout, password change, deletion by an administrator), or whose
     * permissions in the Channel have changed, must not keep receiving events: the subscription is
     * failed, and the client re-subscribes, which runs the guard afresh. Throwing here ends only
     * this operation. The session is re-read at most once per interval, so a burst costs one lookup.
     */
    async function assertSessionStillValid(req: SubscriptionRequest) {
        const requestContext = internal_getRequestContext(req);
        const { session } = requestContext;
        if (!session || configService.authOptions.disableAuth) {
            return;
        }
        if (Date.now() - (req[SESSION_VALIDATED_AT] ?? 0) < SESSION_REVALIDATION_INTERVAL_MS) {
            return;
        }
        req[SESSION_VALIDATED_AT] = Date.now();
        let current: CachedSession | undefined;
        try {
            current = await sessionService.getSessionFromToken(session.token);
        } catch (e: any) {
            // A database blip must not read as "logged out", nor leak its message to the client.
            Logger.error(
                `Could not re-read the session of a subscription: ${e.message as string}`,
                loggerCtx,
            );
            return;
        }
        const { channelId } = requestContext;
        if (!current || permissionsIn(current, channelId) !== permissionsIn(session, channelId)) {
            throw new GraphQLError('The session of this subscription is no longer valid', {
                extensions: { code: 'SUBSCRIPTION_SESSION_INVALID' },
            });
        }
    }
}

/**
 * Nest matches the WebSocket upgrade path with `startsWith`, so with one API path being a prefix
 * of the other both servers would claim the same socket and crash the process on connect.
 */
export function assertApiPathsDoNotOverlap({
    adminApiPath,
    shopApiPath,
}: {
    adminApiPath: string;
    shopApiPath: string;
}) {
    if (adminApiPath.startsWith(shopApiPath) || shopApiPath.startsWith(adminApiPath)) {
        throw new Error(
            `With subscriptions enabled, neither API path may be a prefix of the other: ` +
                `adminApiPath "${adminApiPath}" and shopApiPath "${shopApiPath}"`,
        );
    }
}

/** A comparable snapshot of what a session may do in the Channel. */
function permissionsIn(session: CachedSession, channelId: ID): string {
    const forChannel = session.user?.channelPermissions.find(cp => idsAreEqual(cp.id, channelId));
    return [...(forChannel?.permissions ?? [])].sort().join(',');
}
