import {
    CurrencyCode,
    LanguageCode,
    mergeConfig,
    RedisSubscriptionRelayStrategy,
    RequestContextService,
    SubscriptionService,
} from '@vendure/core';
import { createTestEnvironment } from '@vendure/testing';
import gql from 'graphql-tag';
import path from 'path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { initialData } from '../../../e2e-common/e2e-initial-data';
import { TEST_SETUP_TIMEOUT_MS, testConfig } from '../../../e2e-common/test-config';

import { createChannelDocument } from './graphql/shared-definitions';
import { collectSubscription, delay } from './utils/collect-subscription';
import { pollUntil } from './utils/poll-until';

const ORDER_UPDATED = gql`
    subscription OrderUpdated {
        orderUpdated {
            type
            orderId
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

async function isRedisAvailable(host: string, port: number): Promise<boolean> {
    try {
        const IORedis = await import('ioredis').then(m => m.default);
        const client = new IORedis.Redis({
            host,
            port,
            connectTimeout: 2000,
            lazyConnect: true,
            maxRetriesPerRequest: 1,
        });
        await client.ping();
        await client.quit();
        return true;
    } catch (e) {
        return false;
    }
}

const SECOND_CHANNEL_TOKEN = 'second-channel-token';
const redisHost = '127.0.0.1';
const redisPort = process.env.CI ? +(process.env.E2E_REDIS_PORT || 6379) : 6379;

describe('GraphQL subscriptions with the RedisSubscriptionRelayStrategy', async () => {
    const redisAvailable = await isRedisAvailable(redisHost, redisPort);
    if (!redisAvailable) {
        // eslint-disable-next-line no-console
        console.warn(
            `Redis not available at ${redisHost}:${redisPort}. Skipping the Redis subscription tests.`,
        );
    }

    describe.skipIf(!redisAvailable)('two instances sharing a Redis relay', () => {
        const relayOptions = { redisOptions: { host: redisHost, port: redisPort } };
        const firstConfig = mergeConfig(testConfig(), {
            apiOptions: {
                subscriptions: {
                    enabled: true,
                    relayStrategy: new RedisSubscriptionRelayStrategy(relayOptions),
                },
            },
        });
        const secondConfig = mergeConfig(testConfig(), {
            apiOptions: {
                // a port outside the range assigned to e2e test files
                port: 3206,
                subscriptions: {
                    enabled: true,
                    relayStrategy: new RedisSubscriptionRelayStrategy(relayOptions),
                },
            },
        });
        const first = createTestEnvironment(firstConfig);
        const second = createTestEnvironment(secondConfig);

        beforeAll(async () => {
            // As in a real horizontally-scaled deployment, both instances share one database:
            // the first one initializes it, the second one only bootstraps against it. (Calling
            // `init()` twice would have the second instance drop the database the first one is
            // connected to, on postgres & mysql.)
            await first.server.init({
                initialData,
                productsCsvPath: path.join(__dirname, 'fixtures/e2e-products-minimal.csv'),
                customerCount: 1,
            });
            Object.assign(secondConfig.dbConnectionOptions, firstConfig.dbConnectionOptions);
            await second.server.bootstrap();
            await first.adminClient.asSuperAdmin();
            await second.adminClient.asSuperAdmin();
            // Only the subscribing instance needs to know the second Channel.
            await second.adminClient.query(createChannelDocument, {
                input: {
                    code: 'second-channel',
                    token: SECOND_CHANNEL_TOKEN,
                    defaultLanguageCode: LanguageCode.en,
                    currencyCode: CurrencyCode.GBP,
                    pricesIncludeTax: true,
                    defaultShippingZoneId: 'T_1',
                    defaultTaxZoneId: 'T_1',
                },
            });
        }, TEST_SETUP_TIMEOUT_MS);

        afterAll(async () => {
            await first.server.destroy();
            await second.server.destroy();
        });

        it('delivers an event published on one instance to a subscriber on the other', async () => {
            const subscription = collectSubscription(second.adminClient.subscribe(ORDER_UPDATED));
            await delay(500);

            const { createDraftOrder } = await first.adminClient.query(CREATE_DRAFT_ORDER);
            await pollUntil(() =>
                subscription.results.some(r => r.data.orderUpdated.orderId === createDraftOrder.id),
            );
            subscription.close();

            const payloads = subscription.results.map(r => r.data.orderUpdated);
            expect(payloads.some(p => p.orderId === createDraftOrder.id && p.type === 'CREATED')).toBe(true);
        });

        it('delivers the event exactly once, not once per instance', async () => {
            const subscription = collectSubscription(second.adminClient.subscribe(ORDER_UPDATED));
            await delay(500);

            const { createDraftOrder } = await first.adminClient.query(CREATE_DRAFT_ORDER);
            await pollUntil(() =>
                subscription.results.some(r => r.data.orderUpdated.orderId === createDraftOrder.id),
            );
            await delay(500);
            subscription.close();

            const created = subscription.results.filter(
                r =>
                    r.data.orderUpdated.orderId === createDraftOrder.id &&
                    r.data.orderUpdated.type === 'CREATED',
            );
            expect(created.length).toBe(1);
        });

        it('delivers a payload published outside the API layer, as the worker does', async () => {
            const subscription = collectSubscription(second.adminClient.subscribe(ORDER_UPDATED));
            await delay(500);

            // the worker has no API layer: it publishes through the service directly
            const subscriptionService = first.server.app.get(SubscriptionService);
            const requestContextService = first.server.app.get(RequestContextService);
            const ctx = await requestContextService.create({ apiType: 'admin' });
            // the internal id is published; the API encodes it on the way out
            await subscriptionService.publish(ctx, 'orderUpdated', { type: 'UPDATED', orderId: 42 });

            await pollUntil(() => subscription.results.some(r => r.data.orderUpdated.orderId === 'T_42'));
            subscription.close();

            const received = subscription.results.map(r => r.data.orderUpdated);
            expect(received.some(p => p.orderId === 'T_42' && p.type === 'UPDATED')).toBe(true);
        });

        it('does not deliver an event published in another Channel', async () => {
            const otherChannelSub = collectSubscription(
                second.adminClient.subscribe(ORDER_UPDATED, undefined, {
                    connectionParams: {
                        Authorization: `Bearer ${second.adminClient.getAuthToken()}`,
                        'vendure-token': SECOND_CHANNEL_TOKEN,
                    },
                }),
            );
            const defaultChannelSub = collectSubscription(second.adminClient.subscribe(ORDER_UPDATED));
            await delay(500);

            // published on the first instance, in the default Channel
            const { createDraftOrder } = await first.adminClient.query(CREATE_DRAFT_ORDER);
            await pollUntil(() =>
                defaultChannelSub.results.some(r => r.data.orderUpdated.orderId === createDraftOrder.id),
            );
            await delay(500);
            otherChannelSub.close();
            defaultChannelSub.close();

            expect(
                defaultChannelSub.results.some(r => r.data.orderUpdated.orderId === createDraftOrder.id),
            ).toBe(true);
            expect(otherChannelSub.results).toEqual([]);
        });
    });
});
