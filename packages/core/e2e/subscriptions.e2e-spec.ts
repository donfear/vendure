import {
    AssetStorageStrategy,
    CurrencyCode,
    LanguageCode,
    mergeConfig,
    SubscriptionsOptions,
} from '@vendure/core';
import { SimpleGraphQLClient } from '@vendure/testing';
import { print } from 'graphql';
import gql from 'graphql-tag';
import { createClient } from 'graphql-ws';
import path from 'path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import WebSocket from 'ws';

import { initialData } from '../../../e2e-common/e2e-initial-data';
import { TEST_SETUP_TIMEOUT_MS, testConfig } from '../../../e2e-common/test-config';
import { createTestEnvironment } from '../../testing/lib/create-test-environment';

import { SESSION_REVALIDATION_INTERVAL_MS } from '../src/api/subscriptions/subscription-server-options';
import { SubscriptionsTestPlugin } from './fixtures/test-plugins/with-subscriptions';
import { createChannelDocument } from './graphql/shared-definitions';
import { addItemToOrderDocument } from './graphql/shop-definitions';

import { collectSubscription, delay, triggerUntilReceived } from './utils/collect-subscription';
import { pollUntil } from './utils/poll-until';

const TRIGGER_TEST_EVENT = gql`
    mutation TriggerTestEvent($message: String!, $productId: ID!) {
        triggerTestEvent(message: $message, productId: $productId)
    }
`;

const TEST_EVENT_SUBSCRIPTION = gql`
    subscription TestEvent {
        testEvent {
            message
            productId
            product {
                id
                name
                featuredAsset {
                    preview
                }
            }
        }
    }
`;

const FILTERED_TEST_EVENT_SUBSCRIPTION = gql`
    subscription FilteredTestEvent {
        testEvent(message: "wanted") {
            message
        }
    }
`;

const CREATE_API_KEY = gql`
    mutation CreateApiKey($input: CreateApiKeyInput!) {
        createApiKey(input: $input) {
            apiKey
            entityId
        }
    }
`;

const LOGOUT = gql`
    mutation Logout {
        logout {
            success
        }
    }
`;

const TOUCH_ACTIVE_ORDER = gql`
    mutation TouchActiveOrder {
        setOrderCustomFields(input: {}) {
            ... on Order {
                id
            }
            ... on ErrorResult {
                errorCode
                message
            }
        }
    }
`;

const ACTIVE_ORDER_UPDATED_SUBSCRIPTION = gql`
    subscription ActiveOrderUpdated {
        activeOrderUpdated {
            orderId
            type
            order {
                id
                totalQuantity
            }
        }
    }
`;

describe('GraphQL subscriptions', () => {
    const config = mergeConfig(testConfig(), {
        plugins: [SubscriptionsTestPlugin],
        apiOptions: {
            subscriptions: { enabled: true },
        },
        assetOptions: {
            // The default test strategy ignores the request; this one (inheriting the rest, which
            // the product import relies on) proves that a payload carries what is needed to build
            // an absolute Asset url. It is a separate object, so the shared default stays untouched.
            assetStorageStrategy: Object.assign(
                Object.create(testConfig().assetOptions.assetStorageStrategy as AssetStorageStrategy),
                {
                    toAbsoluteUrl: (request: any, identifier: string) =>
                        `${request.protocol as string}://${request.get('host') as string}/assets/${identifier}`,
                },
            ),
        },
    });
    // The api-key token method is enabled so that a subscription can also be authenticated
    // with an API key.
    config.authOptions.tokenMethod = ['bearer', 'api-key'];
    const { server, adminClient, shopClient } = createTestEnvironment(config);
    const { port, adminApiPath, shopApiPath } = config.apiOptions;
    const subscriptions = config.apiOptions.subscriptions as Required<SubscriptionsOptions>;
    const shopApiUrl = `http://localhost:${port}/${shopApiPath}`;
    const adminApiUrl = `http://localhost:${port}/${adminApiPath}`;

    let secondChannelToken: string;

    const triggerTestEvent = (message: string) =>
        adminClient.query(TRIGGER_TEST_EVENT, { message, productId: 'T_1' });
    const messagesOf = (subscription: { results: any[] }) =>
        subscription.results.map(r => r.data.testEvent.message);
    /** Subscribes over a raw graphql-ws client, so that several subscriptions share a connection. */
    const subscribeOn = (client: ReturnType<typeof createClient>, query: string) =>
        collectSubscription(Object.assign(client.iterate({ query }), { close: () => undefined }));

    beforeAll(async () => {
        await server.init({
            initialData,
            productsCsvPath: path.join(__dirname, 'fixtures/e2e-products-minimal.csv'),
            customerCount: 2,
        });
        await adminClient.asSuperAdmin();
        const { createChannel } = await adminClient.query(createChannelDocument, {
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
        secondChannelToken = (createChannel as any).token;
    }, TEST_SETUP_TIMEOUT_MS);

    afterAll(async () => {
        await server.destroy();
    });

    it('delivers an event to an authorized subscriber', async () => {
        const subscription = collectSubscription(adminClient.subscribe(TEST_EVENT_SUBSCRIPTION));

        await triggerUntilReceived(
            () => triggerTestEvent('hello'),
            () => subscription.results.length > 0,
        );
        subscription.close();

        expect(messagesOf(subscription)[0]).toBe('hello');
    });

    it('encodes ids and makes asset urls absolute in the payload', async () => {
        const subscription = collectSubscription(adminClient.subscribe(TEST_EVENT_SUBSCRIPTION));

        await triggerUntilReceived(
            () => triggerTestEvent('ids'),
            () => subscription.results.length > 0,
        );
        subscription.close();

        const payload = subscription.results[0].data.testEvent;
        expect(payload.productId).toBe('T_1');
        expect(payload.product.id).toBe('T_1');
        // built from the protocol & host of the WebSocket upgrade request
        expect(payload.product.featuredAsset.preview).toMatch(
            new RegExp(`^http://localhost:${port}/assets/`),
        );
    });

    it('applies the arguments of the subscription field', async () => {
        const subscription = collectSubscription(adminClient.subscribe(FILTERED_TEST_EVENT_SUBSCRIPTION));
        await triggerUntilReceived(
            () => triggerTestEvent('wanted'),
            () => subscription.results.length > 0,
        );

        // now that the subscription is demonstrably live, an unwanted event must be skipped
        const received = subscription.results.length;
        await triggerTestEvent('unwanted');
        await triggerTestEvent('wanted');
        await pollUntil(() => subscription.results.length > received);
        subscription.close();

        expect(messagesOf(subscription)).not.toContain('unwanted');
        expect(messagesOf(subscription).length).toBeGreaterThan(received);
    });

    describe('channel scoping', () => {
        it('delivers only what was published in the subscriber Channel', async () => {
            const subscription = collectSubscription(
                adminClient.subscribe(TEST_EVENT_SUBSCRIPTION, undefined, {
                    connectionParams: {
                        Authorization: `Bearer ${adminClient.getAuthToken()}`,
                        'vendure-token': secondChannelToken,
                    },
                }),
            );
            // an event in the subscriber's own Channel proves the subscription is live ...
            adminClient.setChannelToken(secondChannelToken);
            try {
                await triggerUntilReceived(
                    () => triggerTestEvent('second-channel'),
                    () => subscription.results.length > 0,
                );
            } finally {
                adminClient.setChannelToken(null);
            }
            // ... so an event in another Channel, followed by one in its own Channel which does
            // arrive (delivery order is preserved on one connection), was filtered out
            await triggerTestEvent('default-channel');
            adminClient.setChannelToken(secondChannelToken);
            try {
                await triggerUntilReceived(
                    () => triggerTestEvent('second-channel-again'),
                    () => messagesOf(subscription).includes('second-channel-again'),
                );
            } finally {
                adminClient.setChannelToken(null);
            }
            subscription.close();

            expect(messagesOf(subscription)).not.toContain('default-channel');
        });
    });

    describe('authorization', () => {
        it('authenticates a subscription with an API key', async () => {
            const { apiKey } = (
                await adminClient.query(CREATE_API_KEY, {
                    input: {
                        roleIds: ['1'],
                        translations: [{ languageCode: LanguageCode.en, name: 'Subscriptions API Key' }],
                    },
                })
            ).createApiKey;
            const subscription = collectSubscription(
                adminClient.subscribe(TEST_EVENT_SUBSCRIPTION, undefined, {
                    connectionParams: {
                        [String(config.authOptions.apiKeyHeaderKey)]: apiKey,
                    },
                }),
            );

            await triggerUntilReceived(
                () => triggerTestEvent('via-api-key'),
                () => subscription.results.length > 0,
            );
            subscription.close();

            expect(messagesOf(subscription)[0]).toBe('via-api-key');
        });

        it('rejects a subscription from a client without the required permission', async () => {
            const subscription = collectSubscription(
                adminClient.subscribe(TEST_EVENT_SUBSCRIPTION, undefined, { connectionParams: null }),
            );

            await pollUntil(() => subscription.hasError);
            subscription.close();

            expect(subscription.errorText).toContain('authorized');
        });

        it('rejects an owner-scoped subscription when there is no session', async () => {
            const subscription = collectSubscription(
                shopClient.subscribe(ACTIVE_ORDER_UPDATED_SUBSCRIPTION, undefined, {
                    connectionParams: null,
                }),
            );

            await pollUntil(() => subscription.hasError);
            subscription.close();

            expect(subscription.errorText).toContain('authorized');
        });

        it('ends a subscription once the subscriber has logged out', async () => {
            const client = new SimpleGraphQLClient(config, adminApiUrl);
            await client.asSuperAdmin();
            const subscription = collectSubscription(client.subscribe(TEST_EVENT_SUBSCRIPTION));
            await triggerUntilReceived(
                () => triggerTestEvent('before logout'),
                () => subscription.results.length > 0,
            );

            try {
                await client.query(LOGOUT);
                // logging out invalidates every session of the user, the shared adminClient's included
                await adminClient.asSuperAdmin();
                await delay(SESSION_REVALIDATION_INTERVAL_MS + 100);
                await triggerTestEvent('after logout');
                await pollUntil(() => subscription.hasEnded);
            } finally {
                subscription.close();
                await adminClient.asSuperAdmin();
            }

            expect(subscription.errorText).toContain('no longer valid');
            expect(new Set(messagesOf(subscription))).toEqual(new Set(['before logout']));
        });
    });

    describe('transport hardening', () => {
        it('rejects a query sent over the WebSocket connection', async () => {
            const subscription = collectSubscription(
                adminClient.subscribe(gql`
                    query {
                        me {
                            id
                        }
                    }
                `),
            );

            await pollUntil(() => subscription.hasError);
            subscription.close();

            expect(subscription.errorText).toContain(
                'Only subscription operations are permitted over WebSocket',
            );
        });

        it('rejects a subscription sent over http', async () => {
            const result = await adminClient
                .query(TEST_EVENT_SUBSCRIPTION)
                .then(() => undefined)
                .catch(e => e);

            expect(JSON.stringify(result)).toContain('not supported over HTTP');
        });

        it('reports a bad channel token as an error without closing the connection', async () => {
            // the token is per connection, so the error precedes any RequestContext existing
            const client = createClient({
                url: `ws://localhost:${port}/${adminApiPath}`,
                webSocketImpl: WebSocket,
                connectionParams: {
                    Authorization: `Bearer ${adminClient.getAuthToken()}`,
                    'vendure-token': 'no-such-channel',
                },
                retryAttempts: 0,
            });
            const first = subscribeOn(client, print(TEST_EVENT_SUBSCRIPTION));
            await pollUntil(() => first.hasError);
            // a second operation on the same connection proves the connection survived
            const second = subscribeOn(client, print(TEST_EVENT_SUBSCRIPTION));
            await pollUntil(() => second.hasError);
            await client.dispose();

            expect(first.errorText).toContain('Channel');
            expect(second.errorText).toContain('Channel');
        });

        it('rejects an oversized subscribe operation', async () => {
            const oversized = collectSubscription(
                adminClient.subscribe(TEST_EVENT_SUBSCRIPTION, {
                    padding: 'x'.repeat(subscriptions.maxOperationSizeBytes),
                }),
            );

            await pollUntil(() => oversized.hasError);
            oversized.close();

            expect(oversized.errorText).toContain('SUBSCRIPTION_OPERATION_TOO_LARGE');
        });

        it('rejects a malformed subscribe operation without closing the connection', async () => {
            // both operations share the connection, so a syntax error in one must not kill the other
            const client = createClient({
                url: `ws://localhost:${port}/${adminApiPath}`,
                webSocketImpl: WebSocket,
                connectionParams: { Authorization: `Bearer ${adminClient.getAuthToken()}` },
                retryAttempts: 0,
            });
            const healthy = subscribeOn(client, print(TEST_EVENT_SUBSCRIPTION));
            const malformed = subscribeOn(client, 'subscription { testEvent {');

            await pollUntil(() => malformed.hasError);
            await triggerUntilReceived(
                () => triggerTestEvent('still alive'),
                () => healthy.results.length > 0,
            );
            await client.dispose();

            expect(malformed.errorText).toContain('Syntax Error');
            expect(new Set(messagesOf(healthy))).toEqual(new Set(['still alive']));
        });

        it('limits the number of subscriptions on a single connection', async () => {
            const client = createClient({
                url: `ws://localhost:${port}/${adminApiPath}`,
                webSocketImpl: WebSocket,
                connectionParams: { Authorization: `Bearer ${adminClient.getAuthToken()}` },
                retryAttempts: 0,
            });
            const limit = subscriptions.maxSubscriptionsPerConnection;
            const withinLimit = Array.from({ length: limit }, () =>
                subscribeOn(client, print(TEST_EVENT_SUBSCRIPTION)),
            );
            await triggerUntilReceived(
                () => triggerTestEvent('within limit'),
                () => withinLimit.every(s => s.results.length > 0),
            );
            const beyondLimit = subscribeOn(client, print(TEST_EVENT_SUBSCRIPTION));
            await pollUntil(() => beyondLimit.hasError);
            await client.dispose();

            expect(withinLimit.some(s => s.hasError)).toBe(false);
            expect(beyondLimit.errorText).toContain('SUBSCRIPTION_LIMIT_EXCEEDED');
        });
    });

    describe('owner-scoped shop subscription', () => {
        it('delivers the events of the subscriber own Order, and nothing else', async () => {
            // Each client establishes its own session & active Order over http first: an
            // owner-scoped subscription requires an existing session.
            const ownerClient = new SimpleGraphQLClient(config, shopApiUrl);
            const otherClient = new SimpleGraphQLClient(config, shopApiUrl);
            const addItem = async (client: SimpleGraphQLClient) =>
                (await client.query(addItemToOrderDocument, { productVariantId: 'T_1', quantity: 2 }))
                    .addItemToOrder as { id: string };
            const ownerOrder = await addItem(ownerClient);
            const otherOrder = await addItem(otherClient);
            // an (empty) custom fields update publishes an OrderEvent every time, so it can be repeated
            const touchOrder = (client: SimpleGraphQLClient) => client.query(TOUCH_ACTIVE_ORDER);

            const ownerSubscription = collectSubscription(
                ownerClient.subscribe(ACTIVE_ORDER_UPDATED_SUBSCRIPTION),
            );
            const otherSubscription = collectSubscription(
                otherClient.subscribe(ACTIVE_ORDER_UPDATED_SUBSCRIPTION),
            );
            await triggerUntilReceived(
                () => touchOrder(ownerClient),
                () => ownerSubscription.results.length > 0,
            );
            // the other subscriber is live too, so not having received the owner's event means it was filtered
            await triggerUntilReceived(
                () => touchOrder(otherClient),
                () => otherSubscription.results.length > 0,
            );
            await delay(500);
            ownerSubscription.close();
            otherSubscription.close();

            const orderIdsOf = (subscription: { results: any[] }) =>
                new Set(subscription.results.map(r => r.data.activeOrderUpdated.orderId));
            expect(orderIdsOf(ownerSubscription)).toEqual(new Set([ownerOrder.id]));
            expect(orderIdsOf(otherSubscription)).toEqual(new Set([otherOrder.id]));
            expect(ownerSubscription.results[0].data.activeOrderUpdated.order.totalQuantity).toBe(2);
        });
    });
});
