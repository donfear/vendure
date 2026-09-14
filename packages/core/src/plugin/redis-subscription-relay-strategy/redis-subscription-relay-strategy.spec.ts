import { EventEmitter } from 'events';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { RelayedSubscriptionMessage } from '../../config/subscriptions/subscription-relay-strategy';

import { RedisSubscriptionRelayStrategy } from './redis-subscription-relay-strategy';

/** Enough of ioredis for pub/sub: every client publishes into one shared bus. */
const bus = new EventEmitter();
let failNextSubscribe = false;

class FakeRedis extends EventEmitter {
    channels = new Set<string>();
    constructor() {
        super();
        bus.on('publish', (channel: string, message: string) => {
            if (this.channels.has(channel)) {
                this.emit('message', channel, message);
            }
        });
    }
    async subscribe(channel: string) {
        if (failNextSubscribe) {
            failNextSubscribe = false;
            throw new Error('Connection is closed');
        }
        this.channels.add(channel);
    }
    async unsubscribe(channel: string) {
        this.channels.delete(channel);
    }
    async publish(channel: string, message: string) {
        bus.emit('publish', channel, message);
    }
    async quit() {
        return 'OK';
    }
}

vi.mock('ioredis', () => ({ default: { Redis: FakeRedis } }));

const message: RelayedSubscriptionMessage = {
    key: 'orderUpdated',
    payload: { id: 1 },
    origin: { channelIds: [1] },
};

describe('RedisSubscriptionRelayStrategy', () => {
    let strategy: RedisSubscriptionRelayStrategy;

    beforeEach(async () => {
        bus.removeAllListeners();
        strategy = new RedisSubscriptionRelayStrategy();
        await strategy.init();
    });

    it('delivers a published message to each listener, as its own copy', async () => {
        const received: RelayedSubscriptionMessage[] = [];
        await strategy.subscribe('orderUpdated', { next: m => received.push(m), error: () => undefined });
        await strategy.subscribe('orderUpdated', { next: m => received.push(m), error: () => undefined });

        await strategy.publish(message);

        expect(received).toEqual([message, message]);
        expect(received[0]).not.toBe(received[1]);
    });

    it('rejects every subscriber when Redis refuses the channel, leaving nothing behind', async () => {
        failNextSubscribe = true;
        const listener = { next: vi.fn(), error: vi.fn() };

        // both arrive before Redis has answered, so both share the one SUBSCRIBE and its failure
        const [first, second] = await Promise.allSettled([
            strategy.subscribe('orderUpdated', listener),
            strategy.subscribe('orderUpdated', listener),
        ]);
        expect(first.status).toBe('rejected');
        expect(second.status).toBe('rejected');
        // a later attempt subscribes afresh rather than attaching to a dead subject
        await strategy.subscribe('orderUpdated', listener);
        await strategy.publish(message);

        expect(listener.next).toHaveBeenCalledTimes(1);
    });

    it('fails every open subscription when the connection is lost, since messages are not replayed', async () => {
        const listener = { next: vi.fn(), error: vi.fn() };
        await strategy.subscribe('orderUpdated', listener);

        (await (strategy as any).subscriber).emit('reconnecting');

        expect(listener.error).toHaveBeenCalledWith(
            expect.objectContaining({ message: expect.stringContaining('lost') }),
        );
    });

    it('stops delivering once the last listener has unsubscribed', async () => {
        const listener = { next: vi.fn(), error: vi.fn() };
        const unsubscribe = await strategy.subscribe('orderUpdated', listener);

        await unsubscribe();
        await strategy.publish(message);

        expect(listener.next).not.toHaveBeenCalled();
        // ... and Redis itself was told, so the channel no longer costs anything
        expect((await (strategy as any).subscriber).channels.size).toBe(0);
    });

    it('drops a message published before init rather than throwing', async () => {
        const uninitialized = new RedisSubscriptionRelayStrategy();

        await expect(uninitialized.publish(message)).resolves.toBeUndefined();
    });
});
