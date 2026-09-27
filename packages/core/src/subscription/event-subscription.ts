import { Inject, Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { ID, Json, Type } from '@vendure/common/lib/shared-types';

import { RequestContext } from '../api/common/request-context';
import { VendureEvent } from '../event-bus/vendure-event';

import { SubscriptionService } from './subscription.service';

/**
 * @description
 * A GraphQL subscription whose results come from Vendure events.
 *
 * To add one, extend this class, list it in the `providers` of a plugin, and return `listen()`
 * from the resolver of the subscription field. Whenever the event is raised, in the server or in
 * the worker, a result is published, and the {@link SubscriptionRelayStrategy} delivers it to the
 * clients which listen in the Channel of the event, or in the Channels which `channelIds()`
 * returns. The methods which create a result are called for one event at a time, so the results
 * keep the order of their events, and a slow method delays the results which follow.
 *
 * Every client which receives a result gets the same payload, so the payload should only contain
 * data which all of them may see, such as ids. The fields of the result are then resolved for each
 * client with its own RequestContext, like the fields of a query result, so a field resolver can
 * return the entity of an id as that client is allowed to see it.
 *
 * @example
 * ```ts
 * import { Injectable } from '\@nestjs/common';
 * import { Parent, ResolveField, Resolver, Subscription } from '\@nestjs/graphql';
 * import { Allow, Ctx, EventSubscription, ID, OrderPlacedEvent, OrderService, Permission, RequestContext } from '\@vendure/core';
 *
 * \@Injectable()
 * export class OrderPlacedSubscription extends EventSubscription<OrderPlacedEvent> {
 *     readonly name = 'orderPlaced';
 *     readonly event = OrderPlacedEvent;
 *
 *     payload({ order }: OrderPlacedEvent) {
 *         return { orderId: order.id };
 *     }
 * }
 *
 * \@Resolver()
 * export class OrderFeedResolver {
 *     constructor(private orderPlacedSubscription: OrderPlacedSubscription) {}
 *
 *     \@Subscription()
 *     \@Allow(Permission.ReadOrder)
 *     orderPlaced(\@Ctx() ctx: RequestContext) {
 *         return this.orderPlacedSubscription.listen(ctx);
 *     }
 * }
 *
 * // Resolves the fields of the OrderPlacedNotification type of the schema
 * \@Resolver('OrderPlacedNotification')
 * export class OrderPlacedNotificationResolver {
 *     constructor(private orderService: OrderService) {}
 *
 *     \@ResolveField()
 *     order(\@Ctx() ctx: RequestContext, \@Parent() notification: { orderId: ID }) {
 *         return this.orderService.findOne(ctx, notification.orderId);
 *     }
 * }
 * ```
 *
 * @docsCategory subscriptions
 * @docsPage EventSubscription
 * @docsWeight 0
 * @since 3.8.0
 */
@Injectable()
export abstract class EventSubscription<
    E extends VendureEvent & { ctx: RequestContext },
> implements OnApplicationBootstrap {
    /**
     * @description
     * The name of the subscription field in the schema. It also identifies the results when they
     * are relayed between processes, so each EventSubscription needs a unique name.
     */
    abstract readonly name: string;

    /**
     * @description
     * The Vendure event which produces the results, or an array of several such events.
     */
    abstract readonly event: Type<E> | Array<Type<E>>;

    @Inject(SubscriptionService)
    private readonly subscriptionService: SubscriptionService;

    /**
     * @description
     * Returns the payload of the result of an event. It is relayed as JSON.
     */
    abstract payload(event: E): Json | Promise<Json>;

    /**
     * @description
     * Returns what the result is about, such as an order code, so that it only reaches the clients
     * which listen with the same key. The resolver decides who may listen to a key. Each key which
     * clients listen to takes a subscription of the relay, so the resolver should also refuse keys
     * which do not exist.
     */
    key?(event: E): ID | Promise<ID>;

    /**
     * @description
     * Returns the Channels whose clients receive the result, in place of the Channel of the event.
     * For example, returning the Channels of an order lets the clients in the default Channel, which
     * lists the orders of every Channel, receive orders placed in any Channel.
     */
    channelIds?(event: E): ID[] | Promise<ID[]>;

    /**
     * @description
     * Decides whether a client receives a result, for rules which depend on both the client and the
     * result, such as those of an EntityAccessControlStrategy. It is called for each client and
     * result with a copy of the client's RequestContext, prepared like the context of a new request,
     * and the result is only delivered if it returns `true`. The payload is shared by all clients,
     * so the filter must not modify it.
     */
    filter?(ctx: RequestContext, payload: Json): boolean | Promise<boolean>;

    /**
     * @description
     * Registers the subscription. A subclass which implements this method must call
     * `super.onApplicationBootstrap()`.
     */
    onApplicationBootstrap() {
        this.subscriptionService.register(this);
    }

    /**
     * @description
     * Returns the results for the client of the given RequestContext, for the resolver of the
     * subscription field to return. A subscription with a `key()` method needs a key. Results of
     * events raised in the same process arrive in the order of the events, and results which arrive
     * while the client is still sending earlier ones are buffered in memory.
     */
    listen(ctx: RequestContext, key?: ID): AsyncIterableIterator<Json> {
        return this.subscriptionService.listen(this, ctx, key);
    }
}
