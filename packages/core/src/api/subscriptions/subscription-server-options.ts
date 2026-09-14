import { Express } from 'express';
import {
    ExecutionArgs,
    GraphQLError,
    GraphQLSchema,
    specifiedRules,
    validate,
    ValidationContext,
} from 'graphql';
import { ServerOptions } from 'graphql-ws';

import { ConfigService } from '../../config/config.service';
import { Logger } from '../../config/logger/vendure-logger';
import { I18nService } from '../../i18n/i18n.service';
import { ApiType } from '../common/get-api-type';
import { GraphqlValueTransformer } from '../common/graphql-value-transformer';
import { IdCodecService } from '../common/id-codec.service';
import { encodeIdsInResult, prefixAssetUrlsInResult } from '../common/result-transformers';

import { isOriginAllowed } from './origin-check';
import { createSubscriptionRequest } from './subscription-request';
import { subscriptionOnlyRule } from './validation-rules';

const loggerCtx = 'Subscriptions';

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
    expressApp: Express;
}): ServerOptions {
    const { schema, apiType, encodeIds, configService, i18nService, idCodecService, expressApp } = options;
    const { maxSubscriptionsPerConnection, maxOperationSizeBytes } = configService.apiOptions.subscriptions;
    const graphqlValueTransformer = new GraphqlValueTransformer(schema);
    const validationRules = [...specifiedRules, ...options.validationRules, subscriptionOnlyRule];
    const { assetStorageStrategy } = configService.assetOptions;
    const toAbsoluteUrl = assetStorageStrategy.toAbsoluteUrl?.bind(assetStorageStrategy);

    return {
        onConnect: async ctx => {
            const request = (ctx.extra as any)?.request;
            const allowed = await isOriginAllowed(request, configService.apiOptions.cors);
            if (!allowed) {
                Logger.warn(
                    `Rejected a WebSocket connection from the disallowed origin ` +
                        `"${request?.headers?.origin as string}"`,
                    loggerCtx,
                );
            }
            return allowed;
        },
        onSubscribe: (ctx, id, payload) => {
            if (Object.keys(ctx.subscriptions).length >= maxSubscriptionsPerConnection) {
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
        },
        validate: (validationSchema, document) =>
            validate(validationSchema, document, validationRules as any),
        context: (ctx, id, payload) =>
            createSubscriptionRequest({
                upgradeRequest: (ctx.extra as any).request,
                connectionParams: ctx.connectionParams,
                payload,
                apiType,
                configService,
                i18nService,
                expressApp,
            }),
        onNext: (ctx, id, payload, args: ExecutionArgs, result) => {
            const req = (args.contextValue as any)?.req;
            if (result.data) {
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
            if (result.errors?.length && req) {
                result.errors = result.errors.map(error => i18nService.translateError(req, error));
            }
            return result;
        },
    };
}
