import { Injectable, Type } from '@nestjs/common';
import { Permission } from '@vendure/common/lib/generated-types';
import { JsonCompatible } from '@vendure/common/lib/shared-types';
import { Observable } from 'rxjs';
import { filter, map } from 'rxjs/operators';

import { RequestContext } from '../../api/common/request-context';
import { ForbiddenError } from '../../common/error/errors';
import { observableToAsyncIterable } from '../../common/observable-to-async-iterable';
import { idsAreEqual } from '../../common/utils';
import { ConfigService } from '../../config/config.service';
import { Logger } from '../../config/logger/vendure-logger';
import {
    RelayedSubscriptionMessage,
    SubscriptionMessageOrigin,
} from '../../config/subscriptions/subscription-relay-strategy';
import { EventBus } from '../../event-bus/event-bus';
import { VendureEvent } from '../../event-bus/vendure-event';

import { SessionService } from './session.service';

const loggerCtx = 'SubscriptionService';

/**
 * @description
 * Options shared by both ways of feeding a subscription.
 *
 * @docsCategory services
 * @docsPage SubscriptionService
 */
export interface SubscriptionScopeOptions {
    /**
     * @description
     * By default a subscriber only receives what was published in its own Channel. Setting this
     * to `false` delivers from every Channel, and requires the SuperAdmin permission.
     *
     * @default true
     */
    restrictToChannel?: boolean;
    /**
     * @description
     * Restricts delivery to what the subscriber's own session or user caused, which is what an
     * `\@Allow(Permission.Owner)` subscription needs.
     *
     * It defaults to `ctx.authorizedAsOwnerOnly`, i.e. it is applied automatically when the
     * resolver requires `Permission.Owner` and the subscriber holds no other permission. Such a
     * subscription requires an existing session, since a WebSocket client is never given a new
     * anonymous one.
     */
    ownerOnly?: boolean;
}

/**
 * @description
 * Options available when subscribing to published payloads.
 *
 * @docsCategory services
 * @docsPage SubscriptionService
 */
export interface PublishedSubscriptionOptions<T> extends SubscriptionScopeOptions {
    /**
     * @description
     * Applied to each payload which passed the Channel and owner checks, e.g. to apply the
     * arguments of the subscription field.
     */
    accept?: (payload: T, origin: SubscriptionMessageOrigin) => boolean;
}

/**
 * @description
 * Feeds a `\@Subscription()` resolver, which is otherwise written like any other resolver:
 * `\@Allow()`, `\@Ctx()` and `\@Args()` all behave as they do for a query.
 *
 * There are two ways to feed one, and both yield the payload under the name of the subscription
 * field, so no `resolve` option is needed on the decorator:
 *
 * - {@link SubscriptionService.publish} / {@link SubscriptionService.fromPublished} send a payload
 *   through the configured {@link SubscriptionRelayStrategy}. With a distributed strategy, a
 *   payload published on any instance - or by the worker - reaches every subscriber.
 * - {@link SubscriptionService.fromEvent} streams {@link EventBus} events directly. The EventBus is
 *   in-process, so this only delivers events raised by the instance the client is connected to.
 *
 * Every delivery is scoped to the subscriber's Channel, and to their own session when the
 * subscription is owner-scoped. A subscription ends as soon as the subscriber's session has
 * expired or been invalidated (e.g. by logging out).
 *
 * @example
 * ```ts
 * \@Resolver()
 * export class OrderStateSubscriptionResolver {
 *   constructor(private subscriptionService: SubscriptionService) {}
 *
 *   \@Subscription('orderStateChanged')
 *   \@Allow(Permission.ReadOrder)
 *   orderStateChanged(\@Ctx() ctx: RequestContext) {
 *     return this.subscriptionService.fromPublished(ctx, 'orderStateChanged');
 *   }
 * }
 * ```
 *
 * @docsCategory services
 * @docsPage SubscriptionService
 * @docsWeight 0
 */
@Injectable()
export class SubscriptionService {
    constructor(
        private eventBus: EventBus,
        private configService: ConfigService,
        private sessionService: SessionService,
    ) {}

    /**
     * @description
     * Publishes a payload to every instance which has subscribers of the given subscription field,
     * via the configured {@link SubscriptionRelayStrategy}. The `ctx` determines the Channel the
     * payload belongs to, and the session & user which may receive it when it is owner-scoped.
     *
     * The payload must be JSON-compatible, since it may be serialized on its way to another
     * instance.
     */
    async publish<T extends JsonCompatible<T>>(
        ctx: RequestContext,
        fieldName: string,
        payload: T,
    ): Promise<void> {
        await this.relayStrategy.publish({
            key: fieldName,
            payload,
            origin: {
                channelId: ctx.channelId,
                sessionId: ctx.session?.id,
                activeUserId: ctx.activeUserId,
            },
        });
    }

    /**
     * @description
     * Subscribes to the payloads published to the given subscription field, and can be returned
     * directly from the corresponding `\@Subscription()` resolver.
     */
    fromPublished<T>(
        ctx: RequestContext,
        fieldName: string,
        options: PublishedSubscriptionOptions<T> = {},
    ): AsyncIterableIterator<Record<string, T>> {
        const scope = this.resolveScope(ctx, fieldName, options);
        const source = new Observable<RelayedSubscriptionMessage>(subscriber => {
            let unsubscribe: (() => Promise<void>) | undefined;
            let cancelled = false;
            this.relayStrategy
                .subscribe(fieldName, (message: RelayedSubscriptionMessage) => subscriber.next(message))
                .then((unsub: () => Promise<void>) => {
                    unsubscribe = unsub;
                    if (cancelled) {
                        void unsub();
                    }
                })
                // A subscriber which cannot be attached to the relay would otherwise sit there
                // silently receiving nothing.
                .catch(err => subscriber.error(err));
            return () => {
                cancelled = true;
                void unsubscribe?.();
            };
        }).pipe(
            filter(message => scope.allows(message.origin)),
            filter(message => options.accept?.(message.payload as T, message.origin) ?? true),
            map(message => this.wrap(fieldName, message.payload as T)),
        );
        return this.whileSessionValid(ctx, observableToAsyncIterable(source));
    }

    /**
     * @description
     * Streams the {@link EventBus} events of the given type(s) which occurred in the subscriber's
     * Channel, mapped to the payload of the subscription field, and can be returned directly from
     * the corresponding `\@Subscription()` resolver.
     *
     * The `mapFn` returning `undefined` skips the event, which is how a subscription applies its
     * own filtering, e.g. by the arguments of the field.
     *
     * Note that the EventBus is in-process: use {@link SubscriptionService.publish} with a
     * distributed {@link SubscriptionRelayStrategy} if subscribers must see what happened on
     * another instance or in the worker.
     */
    fromEvent<T extends VendureEvent, R>(
        ctx: RequestContext,
        fieldName: string,
        eventType: Type<T> | Array<Type<T>>,
        mapFn: (event: T) => R | undefined,
        options: SubscriptionScopeOptions = {},
    ): AsyncIterableIterator<Record<string, R>> {
        const scope = this.resolveScope(ctx, fieldName, options);
        const eventTypes = Array.isArray(eventType) ? eventType : [eventType];
        const source = new Observable<T>(subscriber => {
            const subscriptions = eventTypes.map(type =>
                this.eventBus.ofType(type).subscribe(event => subscriber.next(event)),
            );
            return () => subscriptions.forEach(subscription => subscription.unsubscribe());
        }).pipe(
            filter(event => scope.allows(this.originOfEvent(event))),
            map(event => mapFn(event)),
            filter((payload): payload is R => payload !== undefined),
            map(payload => this.wrap(fieldName, payload)),
        );
        return this.whileSessionValid(ctx, observableToAsyncIterable(source));
    }

    /**
     * The payload is yielded under the name of the subscription field, which is what the default
     * GraphQL field resolver reads.
     */
    private wrap<T>(fieldName: string, payload: T): Record<string, T> {
        return { [fieldName]: payload };
    }

    /**
     * Builds the Channel & owner checks which every delivery must pass, and rejects a subscription
     * which could never be scoped safely.
     */
    private resolveScope(
        ctx: RequestContext,
        fieldName: string,
        options: SubscriptionScopeOptions,
    ): { allows: (origin: SubscriptionMessageOrigin | undefined) => boolean } {
        const authDisabled = this.configService.authOptions.disableAuth;
        const restrictToChannel = options.restrictToChannel !== false;
        if (!restrictToChannel && !authDisabled && !ctx.userHasPermissions([Permission.SuperAdmin])) {
            Logger.verbose(
                `Subscription to "${fieldName}" across all Channels was denied: it requires the ` +
                    'SuperAdmin permission.',
                loggerCtx,
            );
            throw new ForbiddenError();
        }
        const ownerOnly = options.ownerOnly ?? ctx.authorizedAsOwnerOnly;
        if (ownerOnly && !authDisabled && !ctx.session) {
            Logger.verbose(
                `Subscription to "${fieldName}" was denied: an owner-scoped subscription requires ` +
                    'an existing session.',
                loggerCtx,
            );
            throw new ForbiddenError();
        }
        const sessionId = ctx.session?.id;
        const activeUserId = ctx.activeUserId;
        return {
            allows: origin => {
                if (!origin) {
                    // Belongs to no Channel and to nobody, so it can only pass an unscoped subscription.
                    return !restrictToChannel && (!ownerOnly || authDisabled);
                }
                if (restrictToChannel && !idsAreEqual(origin.channelId, ctx.channelId)) {
                    return false;
                }
                if (!ownerOnly || authDisabled) {
                    return true;
                }
                if (
                    origin.sessionId != null &&
                    sessionId != null &&
                    idsAreEqual(origin.sessionId, sessionId)
                ) {
                    return true;
                }
                return (
                    origin.activeUserId != null &&
                    activeUserId != null &&
                    idsAreEqual(origin.activeUserId, activeUserId)
                );
            },
        };
    }

    /** Events carry the RequestContext of the operation which caused them, but not all of them do. */
    private originOfEvent(event: VendureEvent): SubscriptionMessageOrigin | undefined {
        const eventCtx = (event as unknown as { ctx?: RequestContext }).ctx;
        if (!(eventCtx instanceof RequestContext)) {
            return undefined;
        }
        return {
            channelId: eventCtx.channelId,
            sessionId: eventCtx.session?.id,
            activeUserId: eventCtx.activeUserId,
        };
    }

    private get relayStrategy() {
        return this.configService.apiOptions.subscriptions.relayStrategy;
    }

    /**
     * Ends the iteration at the first delivery for which the subscriber's session is no longer
     * valid. The session is re-read through the SessionService (i.e. the session cache), so a
     * session which was invalidated while the connection was open - logout, password change,
     * deletion by an administrator - ends the subscription too, not only an expired one.
     *
     * The check happens as the consumer pulls, so it is naturally back-pressured and leaves the
     * bounded buffer of {@link observableToAsyncIterable} in charge of overflow.
     */
    private whileSessionValid<T>(
        ctx: RequestContext,
        iterator: AsyncIterableIterator<T>,
    ): AsyncIterableIterator<T> {
        const end = async (): Promise<IteratorResult<T>> => {
            await iterator.return?.();
            return { value: undefined, done: true };
        };
        return {
            next: async () => {
                const result = await iterator.next();
                if (result.done) {
                    return result;
                }
                return (await this.sessionIsValid(ctx)) ? result : end();
            },
            return: () => end(),
            throw: (error: unknown) => (iterator.throw ? iterator.throw(error) : Promise.reject(error)),
            [Symbol.asyncIterator]() {
                return this;
            },
        };
    }

    private async sessionIsValid(ctx: RequestContext): Promise<boolean> {
        const { session } = ctx;
        if (this.configService.authOptions.disableAuth || !session) {
            return true;
        }
        return !!(await this.sessionService.getSessionFromToken(session.token));
    }
}
