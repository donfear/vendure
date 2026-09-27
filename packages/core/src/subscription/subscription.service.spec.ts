import { Injectable, Type } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { ID, Json } from '@vendure/common/lib/shared-types';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { RequestContext } from '../api/common/request-context';
import { ConfigService } from '../config/config.service';
import { Logger } from '../config/logger/vendure-logger';
import { CachedSession } from '../config/session-cache/session-cache-strategy';
import { TransactionSubscriber } from '../connection/transaction-subscriber';
import { Channel } from '../entity/channel/channel.entity';
import { EventBus } from '../event-bus/event-bus';
import { VendureEvent } from '../event-bus/vendure-event';

import { EventSubscription } from './event-subscription';
import { InMemorySubscriptionRelayStrategy } from './in-memory-subscription-relay-strategy';
import { SubscriptionRelayStrategy } from './subscription-relay-strategy';
import { SubscriptionService } from './subscription.service';

class TestEvent extends VendureEvent {
    constructor(
        public ctx: RequestContext,
        public value: string,
    ) {
        super();
    }
}

class OtherTestEvent extends TestEvent {}

@Injectable()
class ChannelSubscription extends EventSubscription<TestEvent> {
    readonly name = 'channelResults';
    readonly event = TestEvent;

    payload({ value }: TestEvent) {
        if (value === 'broken') {
            throw new Error('Cannot create this result');
        }
        return value;
    }
}

@Injectable()
class KeyedSubscription extends EventSubscription<TestEvent> {
    readonly name = 'keyedResults';
    readonly event = TestEvent;

    async key({ value }: TestEvent) {
        return value;
    }

    payload({ ctx, value }: TestEvent) {
        return `${value} in channel ${ctx.channelId}`;
    }
}

@Injectable()
class MultiChannelSubscription extends EventSubscription<TestEvent> {
    readonly name = 'multiChannelResults';
    readonly event = TestEvent;

    async channelIds({ ctx }: TestEvent) {
        return [ctx.channelId, 'default'];
    }

    payload({ value }: TestEvent) {
        return value;
    }
}

@Injectable()
class FilteredSubscription extends EventSubscription<TestEvent> {
    readonly name = 'filteredResults';
    readonly event = TestEvent;

    payload({ value }: TestEvent) {
        return value;
    }

    readonly filtered: Array<{ ctx: RequestContext; value: string }> = [];

    async filter(ctx: RequestContext, value: string) {
        this.filtered.push({ ctx, value });
        if (value === 'broken') {
            throw new Error('Cannot filter this result');
        }
        // Only true lets a result through
        if (value === 'undecided') {
            return undefined as unknown as boolean;
        }
        if (value === 'truthy') {
            return [] as unknown as boolean;
        }
        // A slow filter does not let the next results overtake this one
        await new Promise(resolve => setTimeout(resolve, value === 'slow' ? 20 : 0));
        return value !== `hidden from ${ctx.activeUserId}`;
    }
}

@Injectable()
class MultiEventSubscription extends EventSubscription<TestEvent | OtherTestEvent> {
    readonly name = 'multiEventResults';
    // An event which is listed twice still produces one result
    readonly event = [TestEvent, OtherTestEvent, TestEvent];

    async payload(event: TestEvent | OtherTestEvent) {
        // A slow payload does not let the next results overtake this one
        await new Promise(resolve => setTimeout(resolve, event.value === 'slow' ? 20 : 0));
        return `${event.constructor.name}: ${event.value}`;
    }
}

@Injectable()
class NoEventSubscription extends EventSubscription<TestEvent> {
    readonly name = 'noEventResults';
    readonly event = [];

    payload({ value }: TestEvent) {
        return value;
    }
}

@Injectable()
class UnregisteredSubscription extends ChannelSubscription {
    onApplicationBootstrap() {
        // Does not call super.onApplicationBootstrap()
    }
}

@Injectable()
class SameNameSubscription extends EventSubscription<TestEvent> {
    readonly name = 'channelResults';
    readonly event = TestEvent;

    payload({ value }: TestEvent) {
        return value;
    }
}

function inChannel(channelId: ID, activeUserId?: ID) {
    return new RequestContext({
        apiType: 'admin',
        channel: { id: channelId } as Channel,
        session: activeUserId ? ({ user: { id: activeUserId } } as CachedSession) : undefined,
        isAuthorized: true,
        authorizedAsOwnerOnly: false,
    });
}

function tick() {
    return new Promise(resolve => setImmediate(resolve));
}

/**
 * Starts listening, and returns the next results once the client has joined its topic.
 */
async function listenTo(results: AsyncIterator<Json>) {
    const first = results.next().then(({ value }) => value);
    await tick();
    return { first, next: () => results.next().then(({ value }) => value) };
}

describe('SubscriptionService', () => {
    let moduleRef: TestingModule | undefined;
    let eventBus: EventBus;
    let errors: string[];
    let preparedContexts: Set<RequestContext>;

    async function createModule(
        subscriptions: Array<Type<EventSubscription<TestEvent>>>,
        options: {
            subscriptions?: boolean;
            relayStrategy?: SubscriptionRelayStrategy;
            disableAuth?: boolean;
        } = {},
    ) {
        const apiOptions = {
            subscriptions: options.subscriptions ?? true,
            subscriptionRelayStrategy: options.relayStrategy ?? new InMemorySubscriptionRelayStrategy(),
        };
        const authOptions = {
            disableAuth: options.disableAuth ?? false,
            entityAccessControlStrategy: {
                prepareAccessControl: async (ctx: RequestContext) => void preparedContexts.add(ctx),
            },
        };
        const module = await Test.createTestingModule({
            providers: [
                EventBus,
                { provide: TransactionSubscriber, useValue: {} },
                { provide: ConfigService, useValue: { apiOptions, authOptions } },
                SubscriptionService,
                ...subscriptions,
            ],
        }).compile();
        // A module whose initialization failed cannot be closed
        await module.init();
        moduleRef = module;
        eventBus = module.get(EventBus);
        return module;
    }

    beforeEach(() => {
        errors = [];
        preparedContexts = new Set();
        Logger.useLogger({
            error: message => errors.push(message),
            warn: () => undefined,
            info: () => undefined,
            verbose: () => undefined,
            debug: () => undefined,
        });
    });

    afterEach(async () => {
        await moduleRef?.close();
        moduleRef = undefined;
        vi.restoreAllMocks();
    });

    it('delivers a result to the clients in the Channel of the event', async () => {
        const subscription = (await createModule([ChannelSubscription])).get(ChannelSubscription);
        const channel1 = await listenTo(subscription.listen(inChannel(1)));
        const channel2 = await listenTo(subscription.listen(inChannel(2)));

        await eventBus.publish(new TestEvent(inChannel(2), 'for channel 2'));
        await eventBus.publish(new TestEvent(inChannel(1), 'for channel 1'));

        expect(await channel1.first).toEqual({ channelResults: 'for channel 1' });
        expect(await channel2.first).toEqual({ channelResults: 'for channel 2' });
    });

    it('delivers a keyed result only to the clients of its key in its Channel', async () => {
        const subscription = (await createModule([KeyedSubscription])).get(KeyedSubscription);
        const keyA = await listenTo(subscription.listen(inChannel(1), 'A'));
        const keyB = await listenTo(subscription.listen(inChannel(1), 'B'));
        const keyAInChannel2 = await listenTo(subscription.listen(inChannel(2), 'A'));

        await eventBus.publish(new TestEvent(inChannel(2), 'A'));
        await eventBus.publish(new TestEvent(inChannel(1), 'B'));
        await eventBus.publish(new TestEvent(inChannel(1), 'A'));

        expect(await keyA.first).toEqual({ keyedResults: 'A in channel 1' });
        expect(await keyB.first).toEqual({ keyedResults: 'B in channel 1' });
        expect(await keyAInChannel2.first).toEqual({ keyedResults: 'A in channel 2' });
    });

    it('delivers a result once to every Channel which channelIds() returns', async () => {
        const subscription = (await createModule([MultiChannelSubscription])).get(MultiChannelSubscription);
        const channel1 = await listenTo(subscription.listen(inChannel(1)));
        const defaultChannel = await listenTo(subscription.listen(inChannel('default')));

        await eventBus.publish(new TestEvent(inChannel(1), 'placed in channel 1'));
        await eventBus.publish(new TestEvent(inChannel('default'), 'placed in the default channel'));

        expect(await channel1.first).toEqual({ multiChannelResults: 'placed in channel 1' });
        expect(await defaultChannel.first).toEqual({ multiChannelResults: 'placed in channel 1' });
        expect(await defaultChannel.next()).toEqual({ multiChannelResults: 'placed in the default channel' });
        await eventBus.publish(new TestEvent(inChannel(1), 'placed later'));
        expect(await defaultChannel.next()).toEqual({ multiChannelResults: 'placed later' });
    });

    it('delivers a result to the clients whose filter returns true, in order', async () => {
        const subscription = (await createModule([FilteredSubscription])).get(FilteredSubscription);
        const userA = await listenTo(subscription.listen(inChannel(1, 'A')));
        const userB = await listenTo(subscription.listen(inChannel(1, 'B')));

        for (const value of ['slow', 'hidden from A', 'broken', 'undecided', 'truthy', 'last']) {
            await eventBus.publish(new TestEvent(inChannel(1), value));
        }

        expect([await userA.first, await userA.next()]).toEqual([
            { filteredResults: 'slow' },
            { filteredResults: 'last' },
        ]);
        expect([await userB.first, await userB.next(), await userB.next()]).toEqual([
            { filteredResults: 'slow' },
            { filteredResults: 'hidden from A' },
            { filteredResults: 'last' },
        ]);
        expect(errors).toEqual([
            'Could not filter a result of "filteredResults": Cannot filter this result',
            'Could not filter a result of "filteredResults": Cannot filter this result',
        ]);
    });

    it('filters each result with a copy of the RequestContext, prepared like that of a request', async () => {
        const subscription = (await createModule([FilteredSubscription])).get(FilteredSubscription);
        const ctx = inChannel(1, 'A');
        const userA = await listenTo(subscription.listen(ctx));

        await eventBus.publish(new TestEvent(inChannel(1), 'first'));
        await eventBus.publish(new TestEvent(inChannel(1), 'second'));
        await userA.first;
        await userA.next();

        const [first, second] = subscription.filtered.map(filtered => filtered.ctx);
        expect([first, second].map(filtered => filtered.activeUserId)).toEqual(['A', 'A']);
        expect(new Set([ctx, first, second]).size).toBe(3);
        expect(preparedContexts).toEqual(new Set([first, second]));
    });

    it('does not prepare the RequestContext of the filter while auth is disabled', async () => {
        const subscription = (await createModule([FilteredSubscription], { disableAuth: true })).get(
            FilteredSubscription,
        );
        const userA = await listenTo(subscription.listen(inChannel(1, 'A')));

        await eventBus.publish(new TestEvent(inChannel(1), 'first'));

        expect(await userA.first).toEqual({ filteredResults: 'first' });
        expect(preparedContexts.size).toBe(0);
    });

    it('stops filtering the results of a client which has left', async () => {
        const subscription = (await createModule([FilteredSubscription])).get(FilteredSubscription);
        const results = subscription.listen(inChannel(1, 'A'));
        const client = await listenTo(results);

        for (const value of ['slow', 'second', 'third']) {
            await eventBus.publish(new TestEvent(inChannel(1), value));
        }
        // The later results wait for the filter of the first one
        await tick();
        await results.return?.();
        await new Promise(resolve => setTimeout(resolve, 30));

        expect(subscription.filtered.map(filtered => filtered.value)).toEqual(['slow']);
        expect(await client.first).toBeUndefined();
    });

    it('drops a result which is being filtered when its client leaves', async () => {
        const subscription = (await createModule([FilteredSubscription])).get(FilteredSubscription);
        const results = subscription.listen(inChannel(1, 'A'));
        const first = await listenTo(results);

        await eventBus.publish(new TestEvent(inChannel(1), 'slow'));
        await tick();
        await results.return?.();
        await new Promise(resolve => setTimeout(resolve, 30));

        expect(await first.first).toBeUndefined();
        expect(await first.next()).toBeUndefined();
    });

    it('delivers the results of each of its events, in the order of the events', async () => {
        const subscription = (await createModule([MultiEventSubscription])).get(MultiEventSubscription);
        const channel1 = await listenTo(subscription.listen(inChannel(1)));

        await eventBus.publish(new OtherTestEvent(inChannel(1), 'slow'));
        await eventBus.publish(new TestEvent(inChannel(1), 'second'));
        await eventBus.publish(new OtherTestEvent(inChannel(1), 'third'));

        expect([await channel1.first, await channel1.next(), await channel1.next()]).toEqual([
            { multiEventResults: 'OtherTestEvent: slow' },
            { multiEventResults: 'TestEvent: second' },
            { multiEventResults: 'OtherTestEvent: third' },
        ]);
    });

    it('refuses a subscription without an event', async () => {
        await expect(createModule([NoEventSubscription])).rejects.toThrow('NoEventSubscription has no event');
    });

    it('needs a key to listen to a keyed subscription, and no key otherwise', async () => {
        const module = await createModule([ChannelSubscription, KeyedSubscription]);

        expect(() => module.get(KeyedSubscription).listen(inChannel(1))).toThrow(/delivered by key/);
        expect(() => module.get(ChannelSubscription).listen(inChannel(1), 'A')).toThrow(/have no key/);
    });

    it('refuses two subscriptions with the same name', async () => {
        await expect(createModule([ChannelSubscription, SameNameSubscription])).rejects.toThrow(
            'ChannelSubscription and SameNameSubscription are both named "channelResults"',
        );
    });

    it('refuses to listen to a subscription which is not registered', async () => {
        const subscription = (await createModule([UnregisteredSubscription])).get(UnregisteredSubscription);

        expect(() => subscription.listen(inChannel(1))).toThrow('UnregisteredSubscription is not registered');
    });

    it('registers nothing while subscriptions are disabled', async () => {
        const ofType = vi.spyOn(EventBus.prototype, 'ofType');
        const subscription = (await createModule([ChannelSubscription], { subscriptions: false })).get(
            ChannelSubscription,
        );

        expect(ofType).not.toHaveBeenCalled();
        expect(() => subscription.listen(inChannel(1))).toThrow(
            'Subscriptions are not enabled by apiOptions.subscriptions',
        );
    });

    it('logs a result which cannot be created or published, instead of crashing', async () => {
        const relayStrategy = new InMemorySubscriptionRelayStrategy();
        vi.spyOn(relayStrategy, 'publish').mockRejectedValueOnce('Redis is unreachable');
        const subscription = (await createModule([ChannelSubscription], { relayStrategy })).get(
            ChannelSubscription,
        );
        const channel1 = await listenTo(subscription.listen(inChannel(1)));

        await eventBus.publish(new TestEvent(inChannel(1), 'lost'));
        await eventBus.publish(new TestEvent(inChannel(1), 'broken'));
        await eventBus.publish(new TestEvent(inChannel(1), 'next'));

        expect(await channel1.first).toEqual({ channelResults: 'next' });
        expect(errors).toEqual([
            'Could not publish a result of "channelResults": Redis is unreachable',
            'Could not publish a result of "channelResults": Cannot create this result',
        ]);
    });

    it('does not hold back the next results while the relay publishes one', async () => {
        const relayStrategy = new InMemorySubscriptionRelayStrategy();
        vi.spyOn(relayStrategy, 'publish').mockImplementationOnce(() => new Promise(() => undefined));
        const subscription = (await createModule([ChannelSubscription], { relayStrategy })).get(
            ChannelSubscription,
        );
        const channel1 = await listenTo(subscription.listen(inChannel(1)));

        await eventBus.publish(new TestEvent(inChannel(1), 'never published'));
        await eventBus.publish(new TestEvent(inChannel(1), 'next'));

        expect(await channel1.first).toEqual({ channelResults: 'next' });
    });

    it('publishes a result to its other Channels when the relay fails for one', async () => {
        const relayStrategy = new InMemorySubscriptionRelayStrategy();
        vi.spyOn(relayStrategy, 'publish').mockImplementationOnce(() => {
            throw new Error('Cannot publish');
        });
        const subscription = (await createModule([MultiChannelSubscription], { relayStrategy })).get(
            MultiChannelSubscription,
        );
        const defaultChannel = await listenTo(subscription.listen(inChannel('default')));

        await eventBus.publish(new TestEvent(inChannel(1), 'placed in channel 1'));

        expect(await defaultChannel.first).toEqual({ multiChannelResults: 'placed in channel 1' });
        expect(errors).toEqual(['Could not publish a result of "multiChannelResults": Cannot publish']);
    });

    it('joins its topic once, however often the client reads', async () => {
        const subscription = (await createModule([ChannelSubscription])).get(ChannelSubscription);
        const results = subscription.listen(inChannel(1));
        const first = results.next();
        const second = results.next();
        await tick();

        await eventBus.publish(new TestEvent(inChannel(1), 'first'));
        await eventBus.publish(new TestEvent(inChannel(1), 'second'));

        expect([(await first).value, (await second).value]).toEqual([
            { channelResults: 'first' },
            { channelResults: 'second' },
        ]);
    });

    it('joins the topic of a client when the client first reads', async () => {
        const relayStrategy = new InMemorySubscriptionRelayStrategy();
        const subscribe = vi.spyOn(relayStrategy, 'subscribe');
        const subscription = (await createModule([ChannelSubscription], { relayStrategy })).get(
            ChannelSubscription,
        );

        const unread = subscription.listen(inChannel(1));
        await tick();
        expect(subscribe).not.toHaveBeenCalled();

        await unread.return?.();
        await expect(unread.next()).resolves.toEqual({ value: undefined, done: true });
        expect(subscribe).not.toHaveBeenCalled();
    });

    it('subscribes to the relay once per topic, and unsubscribes when the last client leaves', async () => {
        const relayStrategy = new InMemorySubscriptionRelayStrategy();
        const unsubscribe = vi.fn();
        const subscribe = vi.spyOn(relayStrategy, 'subscribe').mockImplementation(async () => unsubscribe);
        const subscription = (await createModule([ChannelSubscription], { relayStrategy })).get(
            ChannelSubscription,
        );

        const clients = Array.from({ length: 20 }, () => subscription.listen(inChannel(1)));
        clients.forEach(client => void client.next());
        await Promise.all(clients.slice(1).map(client => client.return?.()));
        await tick();
        expect(subscribe).toHaveBeenCalledTimes(1);
        expect(unsubscribe).not.toHaveBeenCalled();

        await clients[0].return?.();
        await tick();
        expect(unsubscribe).toHaveBeenCalledTimes(1);
    });

    it('keeps the relay subscription for a client which arrives while it is being set up', async () => {
        const relayStrategy = new InMemorySubscriptionRelayStrategy();
        let subscribed: (unsubscribe: () => void) => void = () => undefined;
        const unsubscribe = vi.fn();
        const subscribe = vi
            .spyOn(relayStrategy, 'subscribe')
            .mockImplementation(() => new Promise(resolve => (subscribed = resolve)));
        const subscription = (await createModule([ChannelSubscription], { relayStrategy })).get(
            ChannelSubscription,
        );

        const leaving = subscription.listen(inChannel(1));
        void leaving.next();
        await leaving.return?.();
        void subscription.listen(inChannel(1)).next();
        subscribed(unsubscribe);
        await tick();

        expect(subscribe).toHaveBeenCalledTimes(1);
        expect(unsubscribe).not.toHaveBeenCalled();
    });

    it('unsubscribes once when clients come and go while the relay subscription is being set up', async () => {
        const relayStrategy = new InMemorySubscriptionRelayStrategy();
        let subscribed: (unsubscribe: () => void) => void = () => undefined;
        const unsubscribe = vi.fn();
        vi.spyOn(relayStrategy, 'subscribe').mockImplementation(
            () => new Promise(resolve => (subscribed = resolve)),
        );
        const subscription = (await createModule([ChannelSubscription], { relayStrategy })).get(
            ChannelSubscription,
        );

        for (const client of [subscription.listen(inChannel(1)), subscription.listen(inChannel(1))]) {
            void client.next();
            await client.return?.();
        }
        subscribed(unsubscribe);
        await tick();

        expect(unsubscribe).toHaveBeenCalledTimes(1);
    });

    it('logs an error of the relay when the last client leaves', async () => {
        const relayStrategy = new InMemorySubscriptionRelayStrategy();
        vi.spyOn(relayStrategy, 'subscribe').mockImplementation(async () => () => {
            throw new Error('Cannot unsubscribe');
        });
        const subscription = (await createModule([ChannelSubscription], { relayStrategy })).get(
            ChannelSubscription,
        );
        const results = subscription.listen(inChannel(1));
        void results.next();
        await tick();

        await results.return?.();
        await tick();

        expect(errors).toEqual(['Could not unsubscribe from "channelResults:1": Cannot unsubscribe']);
    });

    it('fails the clients of a topic whose relay subscription fails, and subscribes again later', async () => {
        const relayStrategy = new InMemorySubscriptionRelayStrategy();
        const subscribe = vi
            .spyOn(relayStrategy, 'subscribe')
            .mockRejectedValueOnce(new Error('Connection is closed.'));
        const subscription = (await createModule([ChannelSubscription], { relayStrategy })).get(
            ChannelSubscription,
        );

        await expect(subscription.listen(inChannel(1)).next()).rejects.toThrow('Connection is closed.');
        const channel1 = await listenTo(subscription.listen(inChannel(1)));
        await eventBus.publish(new TestEvent(inChannel(1), 'after the failure'));

        expect(await channel1.first).toEqual({ channelResults: 'after the failure' });
        expect(subscribe).toHaveBeenCalledTimes(2);
    });
});
