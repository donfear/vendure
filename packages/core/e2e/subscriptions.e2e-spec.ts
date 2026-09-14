import { AssetStorageStrategy, CurrencyCode, LanguageCode, mergeConfig } from '@vendure/core';
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

import { SubscriptionsTestPlugin } from './fixtures/test-plugins/with-subscriptions';
import { createChannelDocument } from './graphql/shared-definitions';
import { addItemToOrderDocument, getActiveOrderDocument } from './graphql/shop-definitions';
import { collectSubscription, delay } from './utils/collect-subscription';
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

const CREATE_API_KEY = gql`
    mutation CreateApiKey($input: CreateApiKeyInput!) {
        createApiKey(input: $input) {
            apiKey
            entityId
        }
    }
`;

const ORDER_UPDATED_SUBSCRIPTION = gql`
    subscription OrderUpdated($orderId: ID) {
        orderUpdated(orderId: $orderId) {
            type
            orderId
            order {
                id
                state
            }
        }
    }
`;

const CREATE_DRAFT_ORDER = gql`
    mutation CreateDraftOrder {
        createDraftOrder {
            id
        }
    }
`;

const DELETE_DRAFT_ORDER = gql`
    mutation DeleteDraftOrder($orderId: ID!) {
        deleteDraftOrder(orderId: $orderId) {
            result
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

/** No client-side signal exists for "subscription established", so harmless triggers repeat. */
async function triggerUntilReceived(
    trigger: () => Promise<unknown>,
    received: () => boolean,
    attempts = 20,
): Promise<void> {
    for (let i = 0; i < attempts && !received(); i++) {
        await trigger();
        await pollUntil(received, { timeout: 200 }).catch(() => undefined);
    }
}

describe('GraphQL subscriptions', () => {
    const config = mergeConfig(testConfig(), {
        plugins: [SubscriptionsTestPlugin],
        apiOptions: {
            subscriptions: { enabled: true },
        },
    });
    // The default test strategy ignores the request; overriding just this method proves that a
    // payload carries what is needed to build an absolute Asset url. The rest of the strategy
    // (which the product import relies on) is left alone.
    (config.assetOptions.assetStorageStrategy as AssetStorageStrategy).toAbsoluteUrl = (
        request: any,
        identifier: string,
    ) => `${request.protocol as string}://${request.get('host') as string}/assets/${identifier}`;
    // The api-key token method is enabled so that a subscription can also be authenticated
    // with an API key.
    config.authOptions.tokenMethod = ['bearer', 'api-key'];
    const { server, adminClient, shopClient } = createTestEnvironment(config);
    const shopApiUrl = `http://localhost:${config.apiOptions.port}/${config.apiOptions.shopApiPath}`;
    const adminApiUrl = `http://localhost:${config.apiOptions.port}/${config.apiOptions.adminApiPath}`;

    let secondChannelToken: string;

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
            () => adminClient.query(TRIGGER_TEST_EVENT, { message: 'hello', productId: 'T_1' }),
            () => subscription.results.length > 0,
        );
        subscription.close();

        expect(subscription.results[0].data.testEvent.message).toBe('hello');
    });

    it('encodes ids and makes asset urls absolute in the payload', async () => {
        const subscription = collectSubscription(adminClient.subscribe(TEST_EVENT_SUBSCRIPTION));

        await triggerUntilReceived(
            () => adminClient.query(TRIGGER_TEST_EVENT, { message: 'ids', productId: 'T_1' }),
            () => subscription.results.length > 0,
        );
        subscription.close();

        const payload = subscription.results[0].data.testEvent;
        expect(payload.productId).toBe('T_1');
        expect(payload.product.id).toBe('T_1');
        // built from the protocol & host of the WebSocket upgrade request
        expect(payload.product.featuredAsset.preview).toMatch(
            new RegExp(`^http://localhost:${config.apiOptions.port}/assets/`),
        );
    });

    it('applies the arguments of the subscription field', async () => {
        const subscription = collectSubscription(
            adminClient.subscribe(gql`
                subscription FilteredTestEvent {
                    testEvent(message: "wanted") {
                        message
                    }
                }
            `),
        );
        await triggerUntilReceived(
            async () => {
                await adminClient.query(TRIGGER_TEST_EVENT, { message: 'unwanted', productId: 'T_1' });
                await adminClient.query(TRIGGER_TEST_EVENT, { message: 'wanted', productId: 'T_1' });
            },
            () => subscription.results.length > 0,
        );
        subscription.close();

        expect(subscription.results.every(r => r.data.testEvent.message === 'wanted')).toBe(true);
    });

    describe('channel scoping', () => {
        it('does not deliver an event published in another Channel', async () => {
            const subscription = collectSubscription(
                adminClient.subscribe(TEST_EVENT_SUBSCRIPTION, undefined, {
                    connectionParams: {
                        Authorization: `Bearer ${adminClient.getAuthToken()}`,
                        'vendure-token': secondChannelToken,
                    },
                }),
            );
            await delay(500);

            await adminClient.query(TRIGGER_TEST_EVENT, {
                message: 'default-channel',
                productId: 'T_1',
            });
            await delay(500);
            subscription.close();

            expect(subscription.results).toEqual([]);
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
            await delay(500);

            await adminClient.query(TRIGGER_TEST_EVENT, { message: 'via-api-key', productId: 'T_1' });
            await triggerUntilReceived(
                () => adminClient.query(TRIGGER_TEST_EVENT, { message: 'via-api-key', productId: 'T_1' }),
                () => subscription.results.length > 0,
            );
            subscription.close();

            expect(subscription.results[0].data.testEvent.message).toBe('via-api-key');
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
                () => adminClient.query(TRIGGER_TEST_EVENT, { message: 'before logout', productId: 'T_1' }),
                () => subscription.results.length > 0,
            );

            await client.query(LOGOUT);
            // logging out invalidates every session of the user, the shared adminClient's included
            await adminClient.asSuperAdmin();
            await adminClient.query(TRIGGER_TEST_EVENT, { message: 'after logout', productId: 'T_1' });
            await Promise.race([subscription.done, delay(2000)]);
            subscription.close();

            expect(subscription.results.map(r => r.data.testEvent.message)).toEqual(['before logout']);
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

        it('limits the number of subscriptions on a single connection', async () => {
            // the limit is per connection, so all the subscriptions share one client
            const client = createClient({
                url: `ws://localhost:${config.apiOptions.port}/${config.apiOptions.adminApiPath}`,
                webSocketImpl: WebSocket,
                connectionParams: { Authorization: `Bearer ${adminClient.getAuthToken()}` },
                retryAttempts: 0,
            });
            const subscriptions = Array.from({ length: 21 }, () =>
                collectSubscription(
                    Object.assign(client.iterate({ query: print(TEST_EVENT_SUBSCRIPTION) }), {
                        close: () => undefined,
                    }) as any,
                ),
            );

            await pollUntil(() => subscriptions.some(s => s.hasError));
            const errors = subscriptions.map(s => s.errorText).join(' ');
            await client.dispose();

            expect(errors).toContain('SUBSCRIPTION_LIMIT_EXCEEDED');
        });
    });

    describe('built-in orderUpdated subscription', () => {
        it('emits when an Order is created and modified', async () => {
            const subscription = collectSubscription(adminClient.subscribe(ORDER_UPDATED_SUBSCRIPTION));
            await delay(500);

            await shopClient.asAnonymousUser();
            await shopClient.query(addItemToOrderDocument, {
                productVariantId: 'T_1',
                quantity: 1,
            });
            await pollUntil(() => subscription.results.length > 0);
            subscription.close();

            const payload = subscription.results[0].data.orderUpdated;
            expect(payload.type).toBe('CREATED');
            expect(payload.order.id).toBe(payload.orderId);
        });

        it('narrows to a single Order via the orderId argument, and emits its deletion', async () => {
            const { createDraftOrder: watched } = await adminClient.query(CREATE_DRAFT_ORDER);
            const subscription = collectSubscription(
                adminClient.subscribe(ORDER_UPDATED_SUBSCRIPTION, { orderId: watched.id }),
            );
            await delay(500);

            // another Order changing must not be delivered
            const { createDraftOrder: other } = await adminClient.query(CREATE_DRAFT_ORDER);
            await adminClient.query(DELETE_DRAFT_ORDER, { orderId: other.id });
            // ... while the watched one is
            await adminClient.query(DELETE_DRAFT_ORDER, { orderId: watched.id });
            await pollUntil(() => subscription.results.some(r => r.data.orderUpdated.type === 'DELETED'));
            subscription.close();

            const payloads = subscription.results.map(r => r.data.orderUpdated);
            expect(payloads.every(p => p.orderId === watched.id)).toBe(true);
            expect(payloads.map(p => p.type)).toContain('DELETED');
            // the deleted Order can no longer be resolved for the subscriber
            expect(payloads.find(p => p.type === 'DELETED').order).toBeNull();
        });
    });

    describe('owner-scoped shop subscription', () => {
        it('delivers the events of the subscriber own Order, and nothing else', async () => {
            // Each client establishes its own session & active Order over http first
            const ownerClient = new SimpleGraphQLClient(config, shopApiUrl);
            const otherClient = new SimpleGraphQLClient(config, shopApiUrl);
            // An owner-scoped subscription requires an existing session, which the
            // `activeOrder` query (which is itself owner-scoped) creates.
            await ownerClient.query(getActiveOrderDocument);
            await otherClient.query(getActiveOrderDocument);

            const ownerSubscription = collectSubscription(
                ownerClient.subscribe(ACTIVE_ORDER_UPDATED_SUBSCRIPTION),
            );
            const otherSubscription = collectSubscription(
                otherClient.subscribe(ACTIVE_ORDER_UPDATED_SUBSCRIPTION),
            );
            await delay(500);

            await ownerClient.query(addItemToOrderDocument, { productVariantId: 'T_1', quantity: 2 });
            await pollUntil(() => ownerSubscription.results.length > 0);
            await delay(500);
            ownerSubscription.close();
            otherSubscription.close();

            const payload = ownerSubscription.results[0].data.activeOrderUpdated;
            expect(payload.type).toBe('created');
            expect(payload.order.totalQuantity).toBe(2);
            expect(otherSubscription.results).toEqual([]);
        });
    });
});
