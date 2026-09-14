import { Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { ID } from '@vendure/common/lib/shared-types';

import { RequestContext } from '../../../api/common/request-context';
import { ConfigService } from '../../../config/config.service';
import { Logger } from '../../../config/logger/vendure-logger';
import { TransactionalConnection } from '../../../connection/transactional-connection';
import { Order } from '../../../entity/order/order.entity';
import { EventBus } from '../../../event-bus/event-bus';
import { OrderEvent } from '../../../event-bus/events/order-event';
import { OrderLineEvent } from '../../../event-bus/events/order-line-event';
import { OrderStateTransitionEvent } from '../../../event-bus/events/order-state-transition-event';
import { SubscriptionService } from '../../services/subscription.service';

export const ORDER_UPDATED_FIELD = 'orderUpdated';

export interface OrderUpdatedPayload {
    type: 'CREATED' | 'UPDATED' | 'DELETED' | 'STATE_TRANSITION';
    orderId: ID;
    fromState?: string;
    toState?: string;
}

const loggerCtx = 'OrderUpdatedPublisher';

/**
 * Publishes Order changes to the built-in `orderUpdated` subscription. It is part of the service
 * layer, so it also runs in the worker process: with a distributed
 * {@link SubscriptionRelayStrategy}, an Order modified by a job reaches subscribers too.
 *
 * A payload is published to every Channel the Order is assigned to, so that an administrator
 * subscribed in the default Channel sees an Order placed through a storefront of another one,
 * exactly as they see it in the Order list.
 */
@Injectable()
export class OrderUpdatedPublisher implements OnApplicationBootstrap {
    constructor(
        private eventBus: EventBus,
        private subscriptionService: SubscriptionService,
        private configService: ConfigService,
        private connection: TransactionalConnection,
    ) {}

    onApplicationBootstrap() {
        if (!this.configService.apiOptions.subscriptions.enabled) {
            // Nothing can be subscribed to, so there is no point in publishing (which with a
            // distributed relay strategy would otherwise cost a round-trip per Order change).
            return;
        }
        this.eventBus.ofType(OrderEvent).subscribe(event =>
            this.publish(event.ctx, event.entity, {
                type: event.type.toUpperCase() as OrderUpdatedPayload['type'],
                orderId: event.entity.id,
            }),
        );
        // a line being added, changed or removed is the most common way an Order changes
        this.eventBus
            .ofType(OrderLineEvent)
            .subscribe(event =>
                this.publish(event.ctx, event.order, { type: 'UPDATED', orderId: event.order.id }),
            );
        this.eventBus.ofType(OrderStateTransitionEvent).subscribe(event =>
            this.publish(event.ctx, event.order, {
                type: 'STATE_TRANSITION',
                orderId: event.order.id,
                fromState: event.fromState,
                toState: event.toState,
            }),
        );
    }

    private publish(ctx: RequestContext, order: Order, payload: OrderUpdatedPayload) {
        this.channelIdsOf(ctx, order, payload.type)
            .then(channelIds =>
                this.subscriptionService.publish(ctx, ORDER_UPDATED_FIELD, payload, { channelIds }),
            )
            .catch(err => {
                Logger.error(
                    `Could not publish the orderUpdated payload: ${err instanceof Error ? err.message : ''}`,
                    loggerCtx,
                );
            });
    }

    /** The Order's Channels, plus the one of the operation, which is all a deleted Order has left. */
    private async channelIdsOf(
        ctx: RequestContext,
        order: Order,
        type: OrderUpdatedPayload['type'],
    ): Promise<ID[]> {
        const channels =
            order.channels ??
            (type === 'DELETED'
                ? []
                : await this.connection
                      .getRepository(ctx, Order)
                      .createQueryBuilder()
                      .relation(Order, 'channels')
                      .of(order.id)
                      .loadMany());
        return [ctx.channelId, ...channels.map(channel => channel.id)];
    }
}
