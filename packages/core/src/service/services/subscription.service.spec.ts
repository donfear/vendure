import { CurrencyCode, LanguageCode, Permission } from '@vendure/common/lib/generated-types';
import { Subject } from 'rxjs';
import { filter } from 'rxjs/operators';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { RequestContext } from '../../api/common/request-context';
import { ForbiddenError } from '../../common/error/errors';
import { MAX_BUFFERED_VALUES } from '../../common/observable-to-async-iterable';
import { InMemorySubscriptionRelayStrategy } from '../../config/subscriptions/in-memory-subscription-relay-strategy';
import { Channel } from '../../entity/channel/channel.entity';
import { EventBus } from '../../event-bus/event-bus';
import { VendureEvent } from '../../event-bus/vendure-event';

import { SubscriptionService } from './subscription.service';

class TestEvent extends VendureEvent {
    constructor(
        public ctx: RequestContext,
        public message: string,
    ) {
        super();
    }
}

class OtherEvent extends VendureEvent {
    constructor(public ctx: RequestContext) {
        super();
    }
}

function superadminCtx(channelId: number | string = 1): RequestContext {
    return new RequestContext({
        apiType: 'admin',
        channel: channelOf(channelId),
        isAuthorized: true,
        authorizedAsOwnerOnly: false,
        session: {
            id: 'superadmin-session',
            token: 'token',
            expires: new Date(Date.now() + 100_000),
            cacheExpiry: Date.now() / 1000 + 1000,
            user: {
                id: 'superadmin',
                identifier: 'superadmin',
                verified: true,
                channelPermissions: [
                    {
                        id: channelId,
                        token: `channel-${channelId.toString()}`,
                        code: `channel-${channelId.toString()}`,
                        permissions: [Permission.SuperAdmin],
                    },
                ],
            },
        } as any,
    });
}

function channelOf(channelId: number | string): Channel {
    return new Channel({
        id: channelId,
        code: `channel-${channelId.toString()}`,
        token: `channel-${channelId.toString()}`,
        defaultCurrencyCode: CurrencyCode.EUR,
        defaultLanguageCode: LanguageCode.en,
        pricesIncludeTax: true,
    });
}

/** A customer with only the Owner permission, i.e. an owner-scoped subscriber or publisher. */
function ownerScopedCtx(sessionId: string): RequestContext {
    return new RequestContext({
        apiType: 'shop',
        channel: channelOf(1),
        isAuthorized: false,
        authorizedAsOwnerOnly: true,
        session: {
            id: sessionId,
            token: 'token',
            expires: new Date(Date.now() + 100_000),
            cacheExpiry: Date.now() / 1000 + 1000,
        } as any,
    });
}

function createCtx(channelId: number | string): RequestContext {
    return new RequestContext({
        apiType: 'admin',
        channel: channelOf(channelId),
        isAuthorized: true,
        authorizedAsOwnerOnly: false,
    });
}

describe('SubscriptionService', () => {
    let service: SubscriptionService;
    let eventStream: Subject<VendureEvent>;
    let relayStrategy: InMemorySubscriptionRelayStrategy;
    let eventBus: EventBus;

    beforeEach(() => {
        eventStream = new Subject<VendureEvent>();
        eventBus = {
            ofType: vi.fn((type: any) => eventStream.pipe(filter(event => event instanceof type))),
        } as unknown as EventBus;
        relayStrategy = new InMemorySubscriptionRelayStrategy();
        service = new SubscriptionService(eventBus, {
            apiOptions: { subscriptions: { relayStrategy } },
            authOptions: { disableAuth: false },
        } as any);
    });

    /** Consumes the iterator in the background, as the GraphQL subscription machinery does. */
    function collect<T>(iterator: AsyncIterableIterator<T>) {
        const values: T[] = [];
        void (async () => {
            for await (const value of iterator) {
                values.push(value);
            }
        })();
        return values;
    }

    const tick = () => new Promise(resolve => setTimeout(resolve, 5));

    /** Drains the iterator, so that a queued error surfaces. */
    async function consumeAll<T>(iterator: AsyncIterableIterator<T>) {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const _ of iterator) {
            // drain
        }
    }

    it('emits the mapped payload of a matching event', async () => {
        const ctx = createCtx(1);
        const values = collect(
            service.fromEvent(ctx, 'testField', TestEvent, event => ({ message: event.message })),
        );
        await tick();

        eventStream.next(new TestEvent(createCtx(1), 'hello'));
        await tick();

        expect(values).toEqual([{ testField: { message: 'hello' } }]);
    });

    it('ignores events of other types', async () => {
        const ctx = createCtx(1);
        const values = collect(
            service.fromEvent(ctx, 'testField', TestEvent, event => ({ message: event.message })),
        );
        await tick();

        eventStream.next(new OtherEvent(createCtx(1)));
        await tick();

        expect(values).toEqual([]);
    });

    it('subscribes to several event types at once', async () => {
        const ctx = createCtx(1);
        const values = collect(
            service.fromEvent(ctx, 'testField', [TestEvent, OtherEvent] as any, (event: any) => ({
                type: event.constructor.name,
            })),
        );
        await tick();

        eventStream.next(new TestEvent(createCtx(1), 'hello'));
        eventStream.next(new OtherEvent(createCtx(1)));
        await tick();

        expect(values).toEqual([{ testField: { type: 'TestEvent' } }, { testField: { type: 'OtherEvent' } }]);
    });

    it('does not emit an event which occurred in another Channel', async () => {
        const ctx = createCtx(1);
        const values = collect(
            service.fromEvent(ctx, 'testField', TestEvent, event => ({ message: event.message })),
        );
        await tick();

        eventStream.next(new TestEvent(createCtx(2), 'other channel'));
        await tick();

        expect(values).toEqual([]);
    });

    it('emits events from any Channel when restrictToChannel is false', async () => {
        const ctx = superadminCtx();
        const values = collect(
            service.fromEvent(ctx, 'testField', TestEvent, event => ({ message: event.message }), {
                restrictToChannel: false,
            }),
        );
        await tick();

        eventStream.next(new TestEvent(createCtx(2), 'other channel'));
        await tick();

        expect(values).toEqual([{ testField: { message: 'other channel' } }]);
    });

    it('skips events for which the map function returns undefined', async () => {
        const ctx = createCtx(1);
        const values = collect(
            service.fromEvent(ctx, 'testField', TestEvent, event =>
                event.message === 'wanted' ? { message: event.message } : undefined,
            ),
        );
        await tick();

        eventStream.next(new TestEvent(createCtx(1), 'unwanted'));
        eventStream.next(new TestEvent(createCtx(1), 'wanted'));
        await tick();

        expect(values).toEqual([{ testField: { message: 'wanted' } }]);
    });

    it('buffers events which arrive before they are consumed', async () => {
        const ctx = createCtx(1);
        const iterator = service.fromEvent(ctx, 'testField', TestEvent, event => ({
            message: event.message,
        }));
        await tick();

        eventStream.next(new TestEvent(createCtx(1), 'first'));
        eventStream.next(new TestEvent(createCtx(1), 'second'));

        expect((await iterator.next()).value).toEqual({ testField: { message: 'first' } });
        expect((await iterator.next()).value).toEqual({ testField: { message: 'second' } });
    });

    it('fails the subscription rather than buffering without limit', async () => {
        const ctx = createCtx(1);
        const iterator = service.fromEvent(ctx, 'testField', TestEvent, event => ({
            message: event.message,
        }));
        await tick();

        for (let i = 0; i <= MAX_BUFFERED_VALUES; i++) {
            eventStream.next(new TestEvent(createCtx(1), `event ${i}`));
        }

        // the buffered events are discarded and the subscription fails
        await expect(consumeAll(iterator)).rejects.toThrow(/faster than this subscription/);
    });

    it('unsubscribes from the EventBus when the subscription ends', async () => {
        const ctx = createCtx(1);
        const iterator = service.fromEvent(ctx, 'testField', TestEvent, event => ({
            message: event.message,
        }));
        await tick();
        expect(eventStream.observed).toBe(true);

        await iterator.return?.();

        expect(eventStream.observed).toBe(false);
    });

    describe('publish() / subscribe() over the relay strategy', () => {
        it('delivers a published payload to a subscriber', async () => {
            const ctx = createCtx(1);
            const values = collect(service.fromPublished<{ message: string }>(ctx, 'testKey'));
            await tick();

            await service.publish(createCtx(1), 'testKey', { message: 'hello' });
            await tick();

            expect(values).toEqual([{ testKey: { message: 'hello' } }]);
        });

        it('does not deliver a payload published in another Channel', async () => {
            const ctx = createCtx(1);
            const values = collect(service.fromPublished(ctx, 'testKey'));
            await tick();

            await service.publish(createCtx(2), 'testKey', { message: 'other channel' });
            await tick();

            expect(values).toEqual([]);
        });

        it('delivers payloads from any Channel when restrictToChannel is false', async () => {
            const ctx = superadminCtx();
            const values = collect(service.fromPublished(ctx, 'testKey', { restrictToChannel: false }));
            await tick();

            await service.publish(createCtx(2), 'testKey', { message: 'other channel' });
            await tick();

            expect(values).toEqual([{ testKey: { message: 'other channel' } }]);
        });

        it('does not deliver payloads published to another key', async () => {
            const ctx = createCtx(1);
            const values = collect(service.fromPublished(ctx, 'testKey'));
            await tick();

            await service.publish(createCtx(1), 'otherKey', { message: 'nope' });
            await tick();

            expect(values).toEqual([]);
        });

        it('applies the accept predicate, which receives the origin of the payload', async () => {
            const ctx = createCtx(1);
            const publisherCtx = createCtx(1);
            (publisherCtx as any)._session = {
                id: 'session-1',
                token: 't',
                expires: new Date(Date.now() + 1000),
            };
            const origins: any[] = [];
            const values = collect(
                service.fromPublished<{ message: string }>(ctx, 'testKey', {
                    accept: (payload, origin) => {
                        origins.push(origin);
                        return payload.message === 'wanted';
                    },
                }),
            );
            await tick();

            await service.publish(publisherCtx, 'testKey', { message: 'unwanted' });
            await service.publish(publisherCtx, 'testKey', { message: 'wanted' });
            await tick();

            expect(values).toEqual([{ testKey: { message: 'wanted' } }]);
            expect(origins[0].sessionId).toBe('session-1');
        });

        it('unsubscribes from the relay strategy when the subscription ends', async () => {
            const unsubscribe = vi.fn().mockResolvedValue(undefined);
            vi.spyOn(relayStrategy, 'subscribe').mockResolvedValue(unsubscribe);
            const iterator = service.fromPublished(createCtx(1), 'testKey');
            await tick();

            await iterator.return?.();

            expect(unsubscribe).toHaveBeenCalled();
        });
    });

    it('errors the subscription when it cannot attach to the relay strategy', async () => {
        vi.spyOn(relayStrategy, 'subscribe').mockRejectedValue(new Error('redis is down'));
        const iterator = service.fromPublished(createCtx(1), 'testKey');

        await expect(iterator.next()).rejects.toThrow('redis is down');
    });

    it('serializes the payload as a distributed relay would', async () => {
        const ctx = createCtx(1);
        const values = collect(service.fromPublished<any>(ctx, 'testKey'));
        await tick();

        // a Date is not JSON-compatible: it arrives as a string, as it would over Redis
        await service.publish(createCtx(1), 'testKey', { at: new Date(0) } as any);
        await tick();

        expect(values).toEqual([{ testKey: { at: '1970-01-01T00:00:00.000Z' } }]);
    });

    it('drops an event which carries no RequestContext, unless the subscription is unscoped', async () => {
        class OrphanEvent extends VendureEvent {
            constructor() {
                super();
            }
        }
        const scoped = collect(
            service.fromEvent(createCtx(1), 'testField', OrphanEvent, () => ({ ok: true })),
        );
        const unscoped = collect(
            service.fromEvent(superadminCtx(), 'testField', OrphanEvent, () => ({ ok: true }), {
                restrictToChannel: false,
            }),
        );
        await tick();

        eventStream.next(new OrphanEvent());
        await tick();

        expect(scoped).toEqual([]);
        expect(unscoped).toEqual([{ testField: { ok: true } }]);
    });

    it('delivers an entity event to a subscriber in any Channel the entity is assigned to', async () => {
        class EntityEvent extends VendureEvent {
            constructor(
                public ctx: RequestContext,
                public entity: { channels: Array<{ id: number }> },
            ) {
                super();
            }
        }
        const values = collect(
            service.fromEvent(createCtx(2), 'testField', EntityEvent, () => ({ ok: true })),
        );
        await tick();

        // raised in Channel 1, about an entity which is also assigned to Channel 2
        eventStream.next(new EntityEvent(createCtx(1), { channels: [{ id: 1 }, { id: 2 }] }));
        await tick();

        expect(values).toEqual([{ testField: { ok: true } }]);
    });

    it('delivers a payload published to several Channels to a subscriber in any of them', async () => {
        const values = collect(service.fromPublished(createCtx(2), 'testKey'));
        await tick();

        await service.publish(createCtx(1), 'testKey', { ok: true }, { channelIds: [1, 2] });
        await tick();

        expect(values).toEqual([{ testKey: { ok: true } }]);
    });

    describe('scope guards', () => {
        it('requires the SuperAdmin permission to subscribe across all Channels', () => {
            expect(() =>
                service.fromPublished(createCtx(1), 'testKey', { restrictToChannel: false }),
            ).toThrow(ForbiddenError);
            expect(() =>
                service.fromEvent(createCtx(1), 'testField', TestEvent, () => ({}), {
                    restrictToChannel: false,
                }),
            ).toThrow(ForbiddenError);
        });

        it('refuses an owner-scoped subscription which has no session', () => {
            const ctx = new RequestContext({
                apiType: 'shop',
                channel: channelOf(1),
                isAuthorized: false,
                authorizedAsOwnerOnly: true,
            });

            expect(() => service.fromPublished(ctx, 'testKey')).toThrow(ForbiddenError);
        });

        it('scopes delivery to the subscriber own session when the resolver requires Owner', async () => {
            const session = {
                id: 'session-1',
                token: 'token',
                expires: new Date(Date.now() + 100_000),
                cacheExpiry: Date.now() / 1000 + 1000,
            } as any;
            const ownerCtx = new RequestContext({
                apiType: 'shop',
                channel: channelOf(1),
                isAuthorized: false,
                authorizedAsOwnerOnly: true,
                session,
            });
            const values = collect(service.fromPublished(ownerCtx, 'testKey'));
            await tick();

            const otherSessionCtx = new RequestContext({
                apiType: 'shop',
                channel: channelOf(1),
                isAuthorized: false,
                authorizedAsOwnerOnly: true,
                session: { ...session, id: 'session-2' },
            });
            await service.publish(otherSessionCtx, 'testKey', { message: 'somebody else' });
            await service.publish(ownerCtx, 'testKey', { message: 'mine' });
            await tick();

            expect(values).toEqual([{ testKey: { message: 'mine' } }]);
        });

        it('lets ownerOnly: false opt out of owner scoping', async () => {
            const ownerCtx = ownerScopedCtx('session-1');
            const values = collect(service.fromPublished(ownerCtx, 'testKey', { ownerOnly: false }));
            await tick();

            await service.publish(ownerScopedCtx('session-2'), 'testKey', { message: 'somebody else' });
            await tick();

            expect(values).toEqual([{ testKey: { message: 'somebody else' } }]);
        });

        it('applies no owner scoping when auth is disabled, since there are no sessions', async () => {
            service = new SubscriptionService(eventBus, {
                apiOptions: { subscriptions: { relayStrategy } },
                authOptions: { disableAuth: true },
            } as any);
            const values = collect(service.fromPublished(createCtx(1), 'testKey', { ownerOnly: true }));
            await tick();

            await service.publish(ownerScopedCtx('session-2'), 'testKey', { message: 'anyone' });
            await tick();

            expect(values).toEqual([{ testKey: { message: 'anyone' } }]);
        });

        it('applies owner scoping to an event stream too', async () => {
            const session = {
                id: 'session-1',
                token: 'token',
                expires: new Date(Date.now() + 100_000),
                cacheExpiry: Date.now() / 1000 + 1000,
            } as any;
            const ownerCtx = new RequestContext({
                apiType: 'shop',
                channel: channelOf(1),
                isAuthorized: false,
                authorizedAsOwnerOnly: true,
                session,
            });
            const values = collect(
                service.fromEvent(ownerCtx, 'testField', TestEvent, event => ({ message: event.message })),
            );
            await tick();

            const otherCtx = new RequestContext({
                apiType: 'shop',
                channel: channelOf(1),
                isAuthorized: false,
                authorizedAsOwnerOnly: true,
                session: { ...session, id: 'session-2' },
            });
            eventStream.next(new TestEvent(otherCtx, 'somebody else'));
            eventStream.next(new TestEvent(ownerCtx, 'mine'));
            await tick();

            expect(values).toEqual([{ testField: { message: 'mine' } }]);
        });
    });
});
