import { Json } from '@vendure/common/lib/shared-types';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { Injector } from '../common/injector';
import { ConfigService } from '../config/config.service';
import { Logger } from '../config/logger/vendure-logger';
import { ProcessContext } from '../process-context/process-context';

import { InMemorySubscriptionRelayStrategy } from './in-memory-subscription-relay-strategy';

function createInjector(options: { isWorker: boolean; subscriptions: boolean }) {
    const instances = new Map<unknown, unknown>([
        [ProcessContext, { isWorker: options.isWorker }],
        [ConfigService, { apiOptions: { subscriptions: options.subscriptions } }],
    ]);
    return { get: (token: unknown) => instances.get(token) } as unknown as Injector;
}

describe('InMemorySubscriptionRelayStrategy', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('relays each message as JSON, like a relay between processes', async () => {
        const strategy = new InMemorySubscriptionRelayStrategy();
        const received: Json[] = [];
        await strategy.subscribe('topic', message => received.push(message));

        await strategy.publish('topic', { placedAt: new Date(0) } as unknown as Json);

        expect(received).toEqual([{ placedAt: '1970-01-01T00:00:00.000Z' }]);
    });

    it('warns in the worker while subscriptions are enabled', () => {
        const warn = vi.spyOn(Logger, 'warn').mockImplementation(() => undefined);

        new InMemorySubscriptionRelayStrategy().init(
            createInjector({ isWorker: false, subscriptions: true }),
        );
        new InMemorySubscriptionRelayStrategy().init(
            createInjector({ isWorker: true, subscriptions: false }),
        );
        expect(warn).not.toHaveBeenCalled();

        new InMemorySubscriptionRelayStrategy().init(createInjector({ isWorker: true, subscriptions: true }));
        expect(warn).toHaveBeenCalledWith(
            expect.stringContaining('Use the RedisSubscriptionRelayStrategy'),
            'InMemorySubscriptionRelayStrategy',
        );
    });
});
