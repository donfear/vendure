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
import net from 'net';
import path from 'path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { initialData } from '../../../e2e-common/e2e-initial-data';
import { TEST_SETUP_TIMEOUT_MS, testConfig } from '../../../e2e-common/test-config';

import { createChannelDocument } from './graphql/shared-definitions';
import { collectSubscription, delay, triggerUntilReceived } from './utils/collect-subscription';
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

/** The second instance needs a port no other e2e file could be using at the same time. */
function getFreePort(): Promise<number> {
    return new Promise((resolve, reject) => {
        const probe = net.createServer();
        probe.on('error', reject);
        probe.listen(0, () => {
            const { port } = probe.address() as net.AddressInfo;
            probe.close(() => resolve(port));
        });
    });
}

describe('GraphQL subscriptions with the RedisSubscriptionRelayStrategy', async () => {
    const redisAvailable = await isRedisAvailable(redisHost, redisPort);
    if (!redisAvailable) {
        if (process.env.CI) {
            // CI declares a Redis service, so its absence is a broken pipeline, not a skip.
            throw new Error(`Redis not available at ${redisHost}:${redisPort}`);
        }
        // eslint-disable-next-line no-console
        console.warn(
            `Redis not available at ${redisHost}:${redisPort}. Skipping the Redis subscription tests.`,
        );
    }
    const secondInstancePort = await getFreePort();

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
                port: secondInstancePort,
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
            // connected to, on postgres & mysql.) With sql.js each instance loads the file into
            // its own memory, so rows written by one are not visible to the other there; the
            // assertions below therefore only rely on what travels through Redis.
            await first.server.init({
                initialData,
                productsCsvPath: path.join(__dirname, 'fixtures/e2e-products-minimal.csv'),
                customerCount: 1,
            });
            Object.assign(secondConfig.dbConnectionOptions, firstConfig.dbConnectionOptions, {
                // the schema already exists; a second synchronize against a live instance is a hazard
                synchronize: false,
            });
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

        /** Creates a draft Order on the first instance, returning its id. */
        const createOrderOnFirst = async () =>
            (await first.adminClient.query(CREATE_DRAFT_ORDER)).createDraftOrder.id as string;
        const createdIdsOf = (subscription: { results: any[] }) =>
            subscription.results
                .filter(r => r.data.orderUpdated.type === 'CREATED')
                .map(r => r.data.orderUpdated.orderId);

        it('delivers an event published on one instance to a subscriber on the other', async () => {
            const subscription = collectSubscription(second.adminClient.subscribe(ORDER_UPDATED));
            let orderId = '';

            await triggerUntilReceived(
                async () => (orderId = await createOrderOnFirst()),
                () => createdIdsOf(subscription).includes(orderId),
            );
            subscription.close();

            expect(createdIdsOf(subscription)).toContain(orderId);
        });

        it('delivers the event exactly once, on the publishing instance and on the other', async () => {
            const local = collectSubscription(first.adminClient.subscribe(ORDER_UPDATED));
            const remote = collectSubscription(second.adminClient.subscribe(ORDER_UPDATED));
            let orderId = '';

            await triggerUntilReceived(
                async () => (orderId = await createOrderOnFirst()),
                () => createdIdsOf(local).includes(orderId) && createdIdsOf(remote).includes(orderId),
            );
            await delay(500);
            local.close();
            remote.close();

            expect(createdIdsOf(local).filter(id => id === orderId).length).toBe(1);
            expect(createdIdsOf(remote).filter(id => id === orderId).length).toBe(1);
        });

        it('delivers a payload published outside the API layer, as the worker does', async () => {
            const subscription = collectSubscription(second.adminClient.subscribe(ORDER_UPDATED));

            // the worker has no API layer: it publishes through the service directly
            const subscriptionService = first.server.app.get(SubscriptionService);
            const requestContextService = first.server.app.get(RequestContextService);
            const ctx = await requestContextService.create({ apiType: 'admin' });
            // the internal id is published; the API encodes it on the way out
            await triggerUntilReceived(
                () => subscriptionService.publish(ctx, 'orderUpdated', { type: 'UPDATED', orderId: 42 }),
                () => subscription.results.length > 0,
            );

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
            // an Order created in the second Channel proves that subscription is live ...
            second.adminClient.setChannelToken(SECOND_CHANNEL_TOKEN);
            let inSecondChannel: { id: string };
            try {
                await triggerUntilReceived(
                    async () => {
                        inSecondChannel = (await second.adminClient.query(CREATE_DRAFT_ORDER))
                            .createDraftOrder;
                    },
                    () => otherChannelSub.results.length > 0,
                );
            } finally {
                second.adminClient.setChannelToken(null);
            }
            // ... so one created on the first instance in the default Channel not arriving means it was filtered
            const { createDraftOrder: inDefaultChannel } = await first.adminClient.query(CREATE_DRAFT_ORDER);
            await delay(500);
            otherChannelSub.close();

            const receivedIds = otherChannelSub.results.map(r => r.data.orderUpdated.orderId);
            expect(receivedIds).toContain(inSecondChannel!.id);
            expect(receivedIds).not.toContain(inDefaultChannel.id);
        });
    });
});
