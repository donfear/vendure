import { GraphQLTypesLoader } from '@nestjs/graphql';
import { CurrencyCode, LanguageCode, Permission } from '@vendure/common/lib/generated-types';
import { SUPER_ADMIN_USER_IDENTIFIER, SUPER_ADMIN_USER_PASSWORD } from '@vendure/common/lib/shared-constants';
import { Json, Type } from '@vendure/common/lib/shared-types';
import {
    defaultConfig,
    DefaultEntityAccessControlStrategy,
    getFinalVendureSchema,
    Logger,
    mergeConfig,
    ProductVariant,
    RedisSubscriptionRelayStrategy,
    RequestContext,
    VENDURE_SHOP_API_TYPE_PATHS,
    VendureEntity,
    VendurePlugin,
} from '@vendure/core';
import { createTestEnvironment, E2E_DEFAULT_CHANNEL_TOKEN, SimpleGraphQLClient } from '@vendure/testing';
import { buildSchema, FormattedExecutionResult, GraphQLError, ValidationRule } from 'graphql';
import gql from 'graphql-tag';
import { Client, ClientOptions, createClient } from 'graphql-ws';
import path from 'path';
import { SelectQueryBuilder } from 'typeorm';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import WebSocket from 'ws';

import { initialData } from '../../../e2e-common/e2e-initial-data';
import { TEST_SETUP_TIMEOUT_MS, testConfig } from '../../../e2e-common/test-config';

import {
    ProductSubscriptionPlugin,
    RequestContextSubscriptionPlugin,
    triggerUpdate,
} from './fixtures/test-plugins/with-subscriptions';
import { graphql } from './graphql/graphql-admin';
import {
    assignProductToChannelDocument,
    createAdministratorDocument,
    createChannelDocument,
    createProductDocument,
    createRoleDocument,
    updateProductDocument,
    updateProductVariantsDocument,
    updateRoleDocument,
} from './graphql/shared-definitions';
import { pollUntil } from './utils/poll-until';

const REQUEST_CONTEXT = `
    subscription {
        requestContext {
            channelToken
            languageCode
            activeUserId
            sessionId
        }
    }
`;

const REQUEST_CONTEXT_UPDATES = 'subscription { requestContextUpdates { activeUserId } }';

const createApiKeyDocument = graphql(`
    mutation CreateApiKey($input: CreateApiKeyInput!) {
        createApiKey(input: $input) {
            apiKey
        }
    }
`);

const rejectOperationsNamedRejected: ValidationRule = context => ({
    OperationDefinition(node) {
        if (node.name?.value === 'Rejected') {
            context.reportError(new GraphQLError('Rejected by a validation rule'));
        }
    },
});

/**
 * Restricts the reader to the first ProductVariant.
 */
class ReaderAccessControlStrategy extends DefaultEntityAccessControlStrategy {
    private readerContexts = new WeakSet<RequestContext>();

    async prepareAccessControl(ctx: RequestContext) {
        if (ctx.session?.user?.identifier === 'reader@test.com') {
            this.readerContexts.add(ctx);
        }
    }

    applyAccessControl(qb: SelectQueryBuilder<any>, entityType: Type<VendureEntity>, ctx: RequestContext) {
        if (entityType === ProductVariant && this.readerContexts.has(ctx)) {
            qb.andWhere(`${qb.alias}.id = :readerVariantId`, { readerVariantId: 1 });
        }
    }
}

function createWebSocketClient(url: string, options: Partial<ClientOptions> = {}) {
    return createClient({ url, webSocketImpl: WebSocket, retryAttempts: 0, ...options });
}

async function collectResults(client: Client, query: string, operationName?: string) {
    const results = [];
    for await (const result of client.iterate({ query, operationName })) {
        results.push(result);
    }
    return results;
}

type Results = AsyncIterator<FormattedExecutionResult<Record<string, any>, unknown>>;

/**
 * Triggers an event until the subscription receives its first result, since the server subscribes
 * to the results of an EventSubscription asynchronously. More results of the trigger may follow.
 */
async function receiveFirstResult(results: Results, trigger: () => Promise<unknown>) {
    const first = results.next();
    let settled = false;
    void first.then(
        () => (settled = true),
        () => (settled = true),
    );
    await pollUntil(
        async () => {
            await trigger();
            return settled;
        },
        { interval: 100 },
    );
    return (await first).value;
}

/**
 * Returns the next result whose data passes the predicate, skipping the others.
 */
async function nextResultWhere(results: Results, predicate: (data: Record<string, any>) => boolean) {
    for (;;) {
        const { value } = await results.next();
        if (!value?.data || predicate(value.data)) {
            return value;
        }
    }
}

async function receiveProductEvent(url: string, adminClient: SimpleGraphQLClient) {
    const client = createWebSocketClient(url, {
        connectionParams: { Authorization: `Bearer ${adminClient.getAuthToken()}` },
    });
    try {
        return await receiveFirstResult(
            client.iterate({ query: 'subscription { productEvents { productId type } }' }),
            () => adminClient.query(updateProductDocument, { input: { id: 'T_1', enabled: true } }),
        );
    } finally {
        await client.dispose();
    }
}

async function subscribe(url: string, query: string, connectionParams?: Record<string, unknown>) {
    const client = createWebSocketClient(url, { connectionParams });
    try {
        return await collectResults(client, query);
    } finally {
        await client.dispose();
    }
}

describe('GraphQL subscriptions', () => {
    const config = mergeConfig(testConfig(), {
        apiOptions: {
            subscriptions: true,
            shopApiValidationRules: [rejectOperationsNamedRejected],
        },
        authOptions: {
            tokenMethod: ['cookie', 'bearer', 'api-key'],
            entityAccessControlStrategy: new ReaderAccessControlStrategy(),
        },
        plugins: [RequestContextSubscriptionPlugin, ProductSubscriptionPlugin],
    });
    const { port, adminApiPath, shopApiPath } = config.apiOptions;
    const adminApiUrl = `ws://localhost:${port}/${adminApiPath}`;
    const shopApiUrl = `ws://localhost:${port}/${shopApiPath}`;
    const { server, adminClient, shopClient } = createTestEnvironment(config);
    let readerRoleId: string;
    let secondChannelReaderRoleId: string;

    async function subscribeAsReader(query: string) {
        const reader = new SimpleGraphQLClient(config, `http://localhost:${port}/${adminApiPath}`);
        await reader.asUserWithCredentials('reader@test.com', 'test');
        const client = createWebSocketClient(adminApiUrl, {
            connectionParams: { Authorization: `Bearer ${reader.getAuthToken()}` },
        });
        return { reader, client, results: client.iterate({ query }) };
    }

    beforeAll(async () => {
        await server.init({
            initialData,
            productsCsvPath: path.join(__dirname, 'fixtures/e2e-products-minimal.csv'),
            customerCount: 1,
        });
        await adminClient.asSuperAdmin();
        await adminClient.query(createChannelDocument, {
            input: {
                code: 'second-channel',
                token: 'second-channel-token',
                defaultLanguageCode: LanguageCode.en,
                currencyCode: CurrencyCode.GBP,
                pricesIncludeTax: true,
                defaultShippingZoneId: 'T_1',
                defaultTaxZoneId: 'T_1',
            },
        });
        const { createRole } = await adminClient.query(createRoleDocument, {
            input: {
                code: 'catalog-reader',
                description: 'Catalog reader',
                permissions: [Permission.ReadCatalog],
                channelIds: ['T_1'],
            },
        });
        readerRoleId = createRole.id;
        const { createRole: secondChannelRole } = await adminClient.query(createRoleDocument, {
            input: {
                code: 'second-channel-catalog-reader',
                description: 'Catalog reader in the second channel',
                permissions: [Permission.ReadCatalog],
                channelIds: ['T_2'],
            },
        });
        secondChannelReaderRoleId = secondChannelRole.id;
        await adminClient.query(createAdministratorDocument, {
            input: {
                emailAddress: 'reader@test.com',
                firstName: 'Cat',
                lastName: 'Reader',
                password: 'test',
                roleIds: [readerRoleId, secondChannelReaderRoleId],
            },
        });
    }, TEST_SETUP_TIMEOUT_MS);

    afterAll(async () => {
        await server.destroy();
    });

    it('resolves a subscription with the RequestContext of the client', async () => {
        const results = await subscribe(shopApiUrl, REQUEST_CONTEXT);

        expect(results).toEqual([
            {
                data: {
                    requestContext: {
                        channelToken: E2E_DEFAULT_CHANNEL_TOKEN,
                        languageCode: 'en',
                        activeUserId: null,
                        sessionId: null,
                    },
                },
            },
        ]);
    });

    it('reads query params from the URL', async () => {
        const results = await subscribe(`${shopApiUrl}?languageCode=de`, REQUEST_CONTEXT);

        expect(results[0].data?.requestContext).toMatchObject({ languageCode: 'de' });
    });

    it('reads the channel token from the connection params', async () => {
        const results = await subscribe(shopApiUrl, REQUEST_CONTEXT, {
            'vendure-token': 'second-channel-token',
        });

        expect(results[0].data?.requestContext).toMatchObject({ channelToken: 'second-channel-token' });
    });

    it('rejects a client without the required permissions', async () => {
        const results = await subscribe(adminApiUrl, REQUEST_CONTEXT);

        expect(results[0].errors?.[0]).toMatchObject({
            message: 'You are not currently authorized to perform this action',
            extensions: { code: 'FORBIDDEN' },
        });
    });

    it('authenticates with a bearer token from the connection params', async () => {
        const results = await subscribe(adminApiUrl, REQUEST_CONTEXT, {
            Authorization: `Bearer ${adminClient.getAuthToken()}`,
        });

        expect(results[0].data?.requestContext).toMatchObject({
            activeUserId: 'T_1',
            sessionId: expect.any(String),
        });
    });

    it('authenticates with an API key from the connection params for each result', async () => {
        const { createApiKey } = await adminClient.query(createApiKeyDocument, {
            input: {
                roleIds: ['T_1'],
                translations: [{ languageCode: LanguageCode.en, name: 'Subscriptions' }],
            },
        });

        const client = createWebSocketClient(adminApiUrl, {
            connectionParams: { 'vendure-api-key': createApiKey.apiKey },
        });
        const results = client.iterate({ query: REQUEST_CONTEXT_UPDATES });
        const authenticated = {
            value: { data: { requestContextUpdates: { activeUserId: expect.any(String) } } },
        };
        try {
            await expect(results.next()).resolves.toMatchObject(authenticated);

            triggerUpdate.next();

            await expect(results.next()).resolves.toMatchObject(authenticated);
        } finally {
            await client.dispose();
        }
    });

    it('treats an invalid token as an anonymous client', async () => {
        const results = await subscribe(shopApiUrl, REQUEST_CONTEXT, { Authorization: 'Bearer invalid' });

        expect(results[0].data?.requestContext).toMatchObject({ activeUserId: null, sessionId: null });
    });

    it('refuses an Owner subscription to a client without a session', async () => {
        const query = 'subscription { ownerRequestContext { sessionId } }';

        const withoutSession = await subscribe(shopApiUrl, query);
        expect(withoutSession[0].errors?.[0]).toMatchObject({ extensions: { code: 'FORBIDDEN' } });

        await shopClient.query(gql`
            query {
                activeOrder {
                    id
                }
            }
        `);
        const withSession = await subscribe(shopApiUrl, query, {
            Authorization: `Bearer ${shopClient.getAuthToken()}`,
        });
        expect(withSession[0].data?.ownerRequestContext).toMatchObject({ sessionId: expect.any(String) });
    });

    it('does not read the session cookie', async () => {
        const adminApiHttpUrl = `http://localhost:${port}/${adminApiPath}`;
        const login = await fetch(adminApiHttpUrl, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
                query: `mutation { login(username: "${SUPER_ADMIN_USER_IDENTIFIER}", password: "${SUPER_ADMIN_USER_PASSWORD}") { ... on CurrentUser { id } } }`,
            }),
        });
        const cookie = login.headers
            .getSetCookie()
            .map(setCookie => setCookie.split(';')[0])
            .join('; ');
        const me = await fetch(adminApiHttpUrl, {
            method: 'POST',
            headers: { 'content-type': 'application/json', cookie },
            body: JSON.stringify({ query: '{ me { id } }' }),
        });
        expect(await me.json()).toEqual({ data: { me: { id: 'T_1' } } });

        class WebSocketWithCookie extends WebSocket {
            constructor(address: string, protocols?: string | string[]) {
                super(address, protocols, { headers: { cookie } });
            }
        }
        const client = createWebSocketClient(shopApiUrl, { webSocketImpl: WebSocketWithCookie });
        try {
            const results = await collectResults(
                client,
                'subscription { requestContext { activeUserId cookie } }',
            );

            expect(results[0].data?.requestContext).toEqual({ activeUserId: null, cookie: null });
        } finally {
            await client.dispose();
        }
    });

    it('ends a subscription once its session has been logged out', async () => {
        const { reader, client, results } = await subscribeAsReader(REQUEST_CONTEXT_UPDATES);
        try {
            await expect(results.next()).resolves.toMatchObject({ done: false });

            await reader.asAnonymousUser();
            triggerUpdate.next();

            await expect(results.next()).rejects.toEqual([
                expect.objectContaining({ extensions: { code: 'FORBIDDEN' } }),
            ]);
        } finally {
            await client.dispose();
        }
    });

    it('checks the session before sending a failed result', async () => {
        const { reader, client, results } = await subscribeAsReader(
            'subscription { failingUpdates { activeUserId } }',
        );
        try {
            await expect(results.next()).resolves.toMatchObject({ value: { data: null } });

            await reader.asAnonymousUser();
            triggerUpdate.next();

            await expect(results.next()).rejects.toEqual([
                expect.objectContaining({ extensions: { code: 'FORBIDDEN' } }),
            ]);
        } finally {
            await client.dispose();
        }
    });

    it('ends a subscription once one of its permissions has been revoked', async () => {
        const { client, results } = await subscribeAsReader(REQUEST_CONTEXT_UPDATES);
        try {
            await expect(results.next()).resolves.toMatchObject({ done: false });

            await adminClient.query(updateRoleDocument, {
                input: { id: readerRoleId, permissions: [Permission.ReadOrder] },
            });
            triggerUpdate.next();

            await expect(results.next()).rejects.toEqual([
                expect.objectContaining({ extensions: { code: 'FORBIDDEN' } }),
            ]);
        } finally {
            await client.dispose();
            await adminClient.query(updateRoleDocument, {
                input: { id: readerRoleId, permissions: [Permission.ReadCatalog] },
            });
        }
    });

    it('ends a subscription once one of its permissions has been revoked in another Channel', async () => {
        const { client, results } = await subscribeAsReader(REQUEST_CONTEXT_UPDATES);
        try {
            await expect(results.next()).resolves.toMatchObject({ done: false });

            await adminClient.query(updateRoleDocument, {
                input: { id: secondChannelReaderRoleId, permissions: [Permission.ReadOrder] },
            });
            triggerUpdate.next();

            await expect(results.next()).rejects.toEqual([
                expect.objectContaining({ extensions: { code: 'FORBIDDEN' } }),
            ]);
        } finally {
            await client.dispose();
            await adminClient.query(updateRoleDocument, {
                input: { id: secondChannelReaderRoleId, permissions: [Permission.ReadCatalog] },
            });
        }
    });

    it('encodes ids and resolves asset urls in the results', async () => {
        const results = await subscribe(
            shopApiUrl,
            `subscription {
                product(id: "T_1") {
                    id
                    featuredAsset {
                        preview
                    }
                }
            }`,
        );

        expect(results[0].data?.product).toEqual({
            id: 'T_1',
            featuredAsset: { preview: 'test-url/test-assets/derick-david-409858-unsplash__preview.jpg' },
        });
    });

    it('resolves each result with a fresh RequestContext', async () => {
        const client = createWebSocketClient(adminApiUrl, {
            connectionParams: { Authorization: `Bearer ${adminClient.getAuthToken()}` },
        });
        const results = client.iterate({
            query: 'subscription { productVariantUpdates(id: "T_1") { price } }',
        });
        try {
            // ProductVariant.price is cached per RequestContext by hydratePriceFields()
            await expect(results.next()).resolves.toMatchObject({
                value: { data: { productVariantUpdates: { price: 129900 } } },
            });

            await adminClient.query(updateProductVariantsDocument, { input: [{ id: 'T_1', price: 139900 }] });
            triggerUpdate.next();

            await expect(results.next()).resolves.toMatchObject({
                value: { data: { productVariantUpdates: { price: 139900 } } },
            });
        } finally {
            await client.dispose();
        }
    });

    it('delivers the results of an EventSubscription', async () => {
        expect(await receiveProductEvent(adminApiUrl, adminClient)).toEqual({
            data: { productEvents: { productId: 'T_1', type: 'updated' } },
        });
    });

    it('delivers the results of an EventSubscription in the Channel of the event', async () => {
        await adminClient.query(assignProductToChannelDocument, {
            input: { channelId: 'T_2', productIds: ['T_1'], priceFactor: 1 },
        });
        const createProduct = async (channelToken: string, slug: string) => {
            adminClient.setChannelToken(channelToken);
            const { createProduct: product } = await adminClient.query(createProductDocument, {
                input: {
                    translations: [{ languageCode: LanguageCode.en, name: slug, slug, description: '' }],
                },
            });
            return product.id;
        };
        const client = createWebSocketClient(adminApiUrl, {
            connectionParams: {
                Authorization: `Bearer ${adminClient.getAuthToken()}`,
                'vendure-token': 'second-channel-token',
            },
        });
        const results = client.iterate({ query: 'subscription { productEvents { productId type } }' });
        try {
            await receiveFirstResult(results, () => {
                adminClient.setChannelToken('second-channel-token');
                return adminClient.query(updateProductDocument, { input: { id: 'T_1', enabled: true } });
            });

            await createProduct(E2E_DEFAULT_CHANNEL_TOKEN, 'in-the-default-channel');
            const secondChannelProductId = await createProduct(
                'second-channel-token',
                'in-the-second-channel',
            );

            await expect(
                nextResultWhere(results, data => data.productEvents.type === 'created'),
            ).resolves.toEqual({
                data: { productEvents: { productId: secondChannelProductId, type: 'created' } },
            });
        } finally {
            adminClient.setChannelToken(E2E_DEFAULT_CHANNEL_TOKEN);
            await client.dispose();
        }
    });

    it('delivers a keyed result to the clients whose filter accepts it, and resolves it for each', async () => {
        const { client, results } = await subscribeAsReader(
            'subscription { productVariantEvents(productId: "T_1") { variantIds variants { id } } }',
        );
        const updateVariants = (...ids: string[]) =>
            adminClient.query(updateProductVariantsDocument, {
                input: ids.map(id => ({ id, enabled: true })),
            });
        try {
            // The reader may only see the variant T_1
            await receiveFirstResult(results, () => updateVariants('T_1'));

            await updateVariants('T_2');
            await updateVariants('T_2', 'T_1');

            await expect(
                nextResultWhere(results, data => data.productVariantEvents.variantIds.includes('T_2')),
            ).resolves.toEqual({
                data: {
                    productVariantEvents: {
                        variantIds: expect.arrayContaining(['T_1', 'T_2']),
                        variants: [{ id: 'T_1' }],
                    },
                },
            });
        } finally {
            await client.dispose();
        }
    });

    it('applies the EntityAccessControlStrategy to each result', async () => {
        const { client, results } = await subscribeAsReader(
            'subscription { productVariantUpdates(id: "T_1") { product { variants { id } } } }',
        );
        const firstVariantOnly = {
            value: { data: { productVariantUpdates: { product: { variants: [{ id: 'T_1' }] } } } },
        };
        try {
            await expect(results.next()).resolves.toMatchObject(firstVariantOnly);

            triggerUpdate.next();

            await expect(results.next()).resolves.toMatchObject(firstVariantOnly);
        } finally {
            await client.dispose();
        }
    });

    it('applies the validation rules of the API', async () => {
        await expect(subscribe(shopApiUrl, 'subscription { requestContext { nope } }')).rejects.toEqual([
            expect.objectContaining({ message: 'Cannot query field "nope" on type "TestRequestContext".' }),
        ]);
        await expect(
            subscribe(shopApiUrl, 'subscription Rejected { requestContext { languageCode } }'),
        ).rejects.toEqual([expect.objectContaining({ message: 'Rejected by a validation rule' })]);
    });

    it('rejects operations other than subscriptions', async () => {
        for (const query of ['{ activeChannel { id } }', 'mutation { logout { success } }']) {
            await expect(subscribe(shopApiUrl, query)).rejects.toEqual([
                expect.objectContaining({
                    message: 'Only subscription operations are supported over WebSocket',
                }),
            ]);
        }
    });

    it('runs the selected operation of a document with several operations', async () => {
        const query = `
            query ActiveChannel { activeChannel { code } }
            subscription RequestContext { requestContext { languageCode } }
        `;
        const response = await fetch(`http://localhost:${port}/${shopApiPath}`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ query, operationName: 'ActiveChannel' }),
        });
        expect(await response.json()).toEqual({ data: { activeChannel: { code: '__default_channel__' } } });

        const client = createWebSocketClient(shopApiUrl);
        try {
            await expect(collectResults(client, query, 'RequestContext')).resolves.toEqual([
                { data: { requestContext: { languageCode: 'en' } } },
            ]);
        } finally {
            await client.dispose();
        }
    });

    it('holds connection params and operations to the size limits of HTTP requests', async () => {
        await expect(
            subscribe(shopApiUrl, REQUEST_CONTEXT, { Authorization: `Bearer ${'x'.repeat(20_000)}` }),
        ).rejects.toMatchObject({ code: 4403 });
        await expect(subscribe(shopApiUrl, `${REQUEST_CONTEXT} # ${'x'.repeat(200_000)}`)).rejects.toEqual([
            expect.objectContaining({ message: 'The operation is too large' }),
        ]);
    });

    it('returns a syntax error without closing the connection', async () => {
        const closed = vi.fn();
        const client = createWebSocketClient(shopApiUrl, { lazy: false, on: { closed } });
        try {
            await expect(collectResults(client, 'subscription {')).rejects.toEqual([
                expect.objectContaining({ message: 'Syntax Error: Expected Name, found <EOF>.' }),
            ]);
            await expect(collectResults(client, REQUEST_CONTEXT)).resolves.toHaveLength(1);
            expect(closed).not.toHaveBeenCalled();
        } finally {
            await client.dispose();
        }
    });
});

describe('GraphQL subscriptions (disabled)', () => {
    const config = mergeConfig(testConfig(), {
        plugins: [RequestContextSubscriptionPlugin, ProductSubscriptionPlugin],
    });
    const { port, shopApiPath } = config.apiOptions;
    const { server } = createTestEnvironment(config);

    beforeAll(async () => {
        await server.init({
            initialData,
            productsCsvPath: path.join(__dirname, 'fixtures/e2e-products-minimal.csv'),
            customerCount: 1,
        });
    }, TEST_SETUP_TIMEOUT_MS);

    afterAll(async () => {
        await server.destroy();
    });

    it('does not accept WebSocket connections', async () => {
        await expect(
            subscribe(`ws://localhost:${port}/${shopApiPath}`, REQUEST_CONTEXT),
        ).rejects.toMatchObject({
            message: expect.stringContaining('Unexpected server response'),
        });
    });

    it('rejects subscriptions over HTTP', async () => {
        const response = await fetch(`http://localhost:${port}/${shopApiPath}`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ query: REQUEST_CONTEXT }),
        });

        expect(response.status).toBe(400);
        expect(await response.json()).toMatchObject({
            errors: [
                {
                    message: 'Subscriptions are only supported over WebSocket',
                    extensions: { code: 'BAD_REQUEST' },
                },
            ],
        });
    });
});

describe('GraphQL subscriptions (Redis relay)', async () => {
    const { default: IORedis } = await import('ioredis');
    const redisOptions = {
        host: '127.0.0.1',
        port: process.env.CI ? +(process.env.E2E_REDIS_PORT || 6379) : 6379,
    };
    const probe = new IORedis.Redis({ ...redisOptions, lazyConnect: true, retryStrategy: () => null });
    const redisAvailable = await probe
        .on('error', () => undefined)
        .ping()
        .then(
            () => true,
            () => false,
        );
    probe.disconnect();

    describe.skipIf(!redisAvailable)('with Redis', () => {
        const config = mergeConfig(testConfig(), {
            apiOptions: {
                subscriptions: true,
                subscriptionRelayStrategy: new RedisSubscriptionRelayStrategy({
                    redisOptions,
                    namespace: 'vendure-subscriptions-e2e',
                }),
            },
            plugins: [RequestContextSubscriptionPlugin, ProductSubscriptionPlugin],
        });
        const { port, adminApiPath } = config.apiOptions;
        const { server, adminClient } = createTestEnvironment(config);

        beforeAll(async () => {
            await server.init({
                initialData,
                productsCsvPath: path.join(__dirname, 'fixtures/e2e-products-minimal.csv'),
                customerCount: 1,
            });
            await adminClient.asSuperAdmin();
        }, TEST_SETUP_TIMEOUT_MS);

        afterAll(async () => {
            await server.destroy();
        });

        it('relays the results of an EventSubscription through Redis', async () => {
            expect(await receiveProductEvent(`ws://localhost:${port}/${adminApiPath}`, adminClient)).toEqual({
                data: { productEvents: { productId: 'T_1', type: 'updated' } },
            });
        });

        it('relays the messages of its namespace only', async () => {
            const relays = ['a', 'b'].map(
                suffix =>
                    new RedisSubscriptionRelayStrategy({
                        redisOptions,
                        namespace: `vendure-subscriptions-e2e-${suffix}`,
                    }),
            );
            await Promise.all(relays.map(relay => relay.init()));
            const received: Json[] = [];
            try {
                await relays[1].subscribe('topic', message => received.push(message));
                await relays[0].publish('topic', 'from namespace a');
                await relays[1].publish('topic', 'from namespace b');
                await pollUntil(() => received.length > 0);

                expect(received).toEqual(['from namespace b']);
            } finally {
                await Promise.all(relays.map(relay => relay.destroy()));
            }
        });

        it('logs and skips a message which is not JSON', async () => {
            const error = vi.spyOn(Logger, 'error');
            const relay = new RedisSubscriptionRelayStrategy({
                redisOptions,
                namespace: 'vendure-subscriptions-e2e',
            });
            const publisher = new IORedis.Redis(redisOptions);
            await relay.init();
            const received: Json[] = [];
            try {
                await relay.subscribe('topic', message => received.push(message));
                await publisher.publish('vendure-subscriptions-e2e:topic', 'not JSON');
                await relay.publish('topic', 'JSON');
                await pollUntil(() => received.length > 0);

                expect(received).toEqual(['JSON']);
                expect(error).toHaveBeenCalledWith(
                    expect.stringContaining('JSON'),
                    'RedisSubscriptionRelayStrategy',
                    expect.any(String),
                );
            } finally {
                error.mockRestore();
                publisher.disconnect();
                await relay.destroy();
            }
        });
    });
});

describe('GraphQL subscriptions (auth disabled)', () => {
    const config = mergeConfig(testConfig(), {
        apiOptions: { subscriptions: true },
        authOptions: { disableAuth: true },
        plugins: [RequestContextSubscriptionPlugin, ProductSubscriptionPlugin],
    });
    const { port, adminApiPath, shopApiPath } = config.apiOptions;
    const { server, adminClient } = createTestEnvironment(config);

    beforeAll(async () => {
        await server.init({
            initialData,
            productsCsvPath: path.join(__dirname, 'fixtures/e2e-products-minimal.csv'),
            customerCount: 1,
        });
    }, TEST_SETUP_TIMEOUT_MS);

    afterAll(async () => {
        await server.destroy();
    });

    it('accepts an Owner subscription without a session', async () => {
        const results = await subscribe(
            `ws://localhost:${port}/${shopApiPath}`,
            'subscription { ownerRequestContext { sessionId } }',
        );

        expect(results[0].data?.ownerRequestContext).toEqual({ sessionId: expect.any(String) });
    });

    it('does not end a subscription once its session has been logged out', async () => {
        await adminClient.asSuperAdmin();
        const client = createWebSocketClient(`ws://localhost:${port}/${adminApiPath}`, {
            connectionParams: { Authorization: `Bearer ${adminClient.getAuthToken()}` },
        });
        const results = client.iterate({ query: REQUEST_CONTEXT_UPDATES });
        try {
            await expect(results.next()).resolves.toMatchObject({ done: false });

            await adminClient.asAnonymousUser();
            triggerUpdate.next();

            await expect(results.next()).resolves.toMatchObject({ done: false });
        } finally {
            await client.dispose();
        }
    });
});

describe('GraphQL subscriptions schema', () => {
    it('lets plugins extend a Subscription type which another plugin defines', async () => {
        @VendurePlugin({
            shopApiExtensions: {
                schema: gql`
                    type Subscription {
                        defined: String
                    }
                `,
            },
        })
        class SubscriptionTypePlugin {}

        const schema = await getFinalVendureSchema({
            config: mergeConfig(defaultConfig, {
                plugins: [SubscriptionTypePlugin, RequestContextSubscriptionPlugin],
            }),
            typePaths: VENDURE_SHOP_API_TYPE_PATHS,
            typesLoader: new GraphQLTypesLoader(),
            apiType: 'shop',
            output: 'sdl',
        });

        expect(Object.keys(buildSchema(schema).getSubscriptionType()?.getFields() ?? {})).toEqual([
            'defined',
            'requestContext',
            'ownerRequestContext',
        ]);
    });
});
