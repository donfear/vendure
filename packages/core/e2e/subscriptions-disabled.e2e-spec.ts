import { createTestEnvironment } from '@vendure/testing';
import gql from 'graphql-tag';
import path from 'path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import WebSocket from 'ws';

import { initialData } from '../../../e2e-common/e2e-initial-data';
import { TEST_SETUP_TIMEOUT_MS, testConfig } from '../../../e2e-common/test-config';

// subscriptions are opt-in: the default config leaves them disabled
const config = testConfig();

describe('GraphQL subscriptions when disabled', () => {
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

    it('serves no WebSocket endpoint', async () => {
        const url = `ws://localhost:${config.apiOptions.port}/${config.apiOptions.adminApiPath}`;
        const outcome = await new Promise<string>(settle => {
            const timeout = setTimeout(() => settle('timeout'), 5000);
            const resolve = (result: string) => {
                clearTimeout(timeout);
                settle(result);
            };
            const socket = new WebSocket(url, 'graphql-transport-ws');
            socket.on('open', () => resolve('connected'));
            socket.on('error', () => resolve('refused'));
        });

        expect(outcome).toBe('refused');
    });

    it('rejects a subscription operation sent over http, and says how to enable them', async () => {
        const result = await adminClient
            .query(gql`
                subscription {
                    orderUpdated {
                        orderId
                    }
                }
            `)
            .then(() => undefined)
            .catch(e => e);

        expect(JSON.stringify(result)).toContain('apiOptions.subscriptions.enabled');
    });
});
