import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { NoopLogger } from '../config/logger/noop-logger';
import { Logger } from '../config/logger/vendure-logger';

import { RedisSubscriptionRelayStrategy } from './redis-subscription-relay-strategy';

// Nothing listens on this port, so Redis is unreachable
const unreachable = { host: '127.0.0.1', port: 1 };

describe('RedisSubscriptionRelayStrategy', () => {
    beforeEach(() => {
        Logger.useLogger(new NoopLogger());
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('connects to Redis on first use', async () => {
        const error = vi.spyOn(Logger, 'error');
        const strategy = new RedisSubscriptionRelayStrategy({ redisOptions: unreachable });
        await strategy.init();
        await new Promise(resolve => setTimeout(resolve, 100));

        await strategy.destroy();

        expect(error).not.toHaveBeenCalled();
    });

    it('forgets the listener of a topic which it could not subscribe to', async () => {
        const strategy = new RedisSubscriptionRelayStrategy({
            redisOptions: { ...unreachable, enableOfflineQueue: false },
        });
        await strategy.init();

        await expect(strategy.subscribe('topic', () => undefined)).rejects.toThrow();

        const { listeners } = strategy as unknown as { listeners: Map<string, unknown> };
        expect(listeners.size).toBe(0);
        await strategy.destroy();
    });

    it('waits for Redis to subscribe, and shuts down in the meantime', async () => {
        const error = vi.spyOn(Logger, 'error');
        const strategy = new RedisSubscriptionRelayStrategy({
            // Reconnects quickly, so that a subscription which gave up would do so during the test
            redisOptions: { ...unreachable, retryStrategy: () => 5 },
        });
        await strategy.init();
        const subscribed = strategy.subscribe('topic', () => undefined);
        const waiting = new Promise(resolve => setTimeout(() => resolve('waiting'), 500));
        expect(
            await Promise.race([
                subscribed.then(
                    () => 'subscribed',
                    () => 'failed',
                ),
                waiting,
            ]),
        ).toBe('waiting');

        await strategy.destroy();
        const connectionErrors = error.mock.calls.length;
        await new Promise(resolve => setTimeout(resolve, 100));

        // It no longer tries to connect
        expect(connectionErrors).toBeGreaterThan(0);
        expect(error).toHaveBeenCalledTimes(connectionErrors);
    });
});
