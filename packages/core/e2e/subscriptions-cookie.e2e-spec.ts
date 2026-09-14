import { mergeConfig } from '@vendure/core';
import { createTestEnvironment } from '@vendure/testing';
import gql from 'graphql-tag';
import path from 'path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import WebSocket from 'ws';

import { initialData } from '../../../e2e-common/e2e-initial-data';
import { TEST_SETUP_TIMEOUT_MS, testConfig } from '../../../e2e-common/test-config';

import { SubscriptionsTestPlugin } from './fixtures/test-plugins/with-subscriptions';
import { collectSubscription, delay } from './utils/collect-subscription';
import { pollUntil } from './utils/poll-until';

const ALLOWED_ORIGIN = 'https://storefront.example.com';

const config = mergeConfig(testConfig(), {
    plugins: [SubscriptionsTestPlugin],
    apiOptions: {
        subscriptions: { enabled: true },
    },
    authOptions: {
        tokenMethod: 'cookie' as const,
        cookieOptions: { secret: 'subscriptions-cookie-e2e' },
    },
});
// mergeConfig will not merge an object over the `cors: true` of the test config
config.apiOptions.cors = { origin: [ALLOWED_ORIGIN], credentials: true };

const TEST_EVENT_SUBSCRIPTION = gql`
    subscription TestEvent {
        testEvent {
            message
        }
    }
`;

const TRIGGER_TEST_EVENT = gql`
    mutation TriggerTestEvent($message: String!, $productId: ID!) {
        triggerTestEvent(message: $message, productId: $productId)
    }
`;

/** Resolves with the close code if the server refuses the connection, else 'connected'. */
function tryConnect(headers: Record<string, string>): Promise<number | string> {
    const url = `ws://localhost:${config.apiOptions.port}/${config.apiOptions.adminApiPath ?? 'admin-api'}`;
    return new Promise(resolve => {
        const socket = new WebSocket(url, 'graphql-transport-ws', { headers });
        socket.on('open', () => socket.send(JSON.stringify({ type: 'connection_init' })));
        socket.on('message', data => {
            if (JSON.parse(data.toString()).type === 'connection_ack') {
                socket.close();
                resolve('connected');
            }
        });
        socket.on('close', code => resolve(code));
        socket.on('error', () => undefined);
    });
}

describe('GraphQL subscriptions with cookie-based auth', () => {
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

    it('authenticates a subscription with the session cookie set over http', async () => {
        const subscription = collectSubscription(
            adminClient.subscribe(TEST_EVENT_SUBSCRIPTION, undefined, {
                connectionParams: null,
                withCookies: true,
            }),
        );
        await delay(250);

        await adminClient.query(TRIGGER_TEST_EVENT, { message: 'via-cookie', productId: 'T_1' });
        await pollUntil(() => subscription.results.length > 0);
        subscription.close();

        expect(subscription.results[0].data.testEvent.message).toBe('via-cookie');
    });

    it('treats a tampered cookie as anonymous', async () => {
        const tamperedCookie = adminClient
            .getCookieHeader()
            .replace(/session=[^;]+/, `session=${Buffer.from('{"token":"stolen"}').toString('base64')}`);
        const subscription = collectSubscription(
            adminClient.subscribe(TEST_EVENT_SUBSCRIPTION, undefined, {
                connectionParams: null,
                headers: { Cookie: tamperedCookie },
            }),
        );

        await pollUntil(() => subscription.hasError);
        subscription.close();

        expect(subscription.errorText).toContain('authorized');
    });

    it('accepts a connection from an allowed Origin', async () => {
        await expect(tryConnect({ Origin: ALLOWED_ORIGIN })).resolves.toBe('connected');
    });

    it('rejects a connection from a disallowed Origin', async () => {
        // 4403: Forbidden
        await expect(tryConnect({ Origin: 'https://evil.example.com' })).resolves.toBe(4403);
    });

    it('accepts a connection from a non-browser client which sends no Origin', async () => {
        await expect(tryConnect({})).resolves.toBe('connected');
    });
});
