import { Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { ID } from '@vendure/common/lib/shared-types';

import { RequestContext } from '../../../api/common/request-context';
import { ConfigService } from '../../../config/config.service';
import { Logger } from '../../../config/logger/vendure-logger';
import { EventBus } from '../../../event-bus/event-bus';
import { OrderEvent } from '../../../event-bus/events/order-event';
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
 */
@Injectable()
export class OrderUpdatedPublisher implements OnApplicationBootstrap {
    constructor(
        private eventBus: EventBus,
        private subscriptionService: SubscriptionService,
        private configService: ConfigService,
    ) {}

    onApplicationBootstrap() {
        if (!this.configService.apiOptions.subscriptions.enabled) {
            // Nothing can be subscribed to, so there is no point in publishing (which with a
            // distributed relay strategy would otherwise cost a round-trip per Order change).
            return;
        }
        this.eventBus.ofType(OrderEvent).subscribe(event =>
            this.publish(event.ctx, {
                type: event.type.toUpperCase() as OrderUpdatedPayload['type'],
                orderId: event.entity.id,
            }),
        );
        this.eventBus.ofType(OrderStateTransitionEvent).subscribe(event =>
            this.publish(event.ctx, {
                type: 'STATE_TRANSITION',
                orderId: event.order.id,
                fromState: event.fromState,
                toState: event.toState,
            }),
        );
    }

    private publish(ctx: RequestContext, payload: OrderUpdatedPayload) {
        this.subscriptionService.publish(ctx, ORDER_UPDATED_FIELD, payload).catch(err => {
            Logger.error(
                `Could not publish the orderUpdated payload: ${err instanceof Error ? err.message : ''}`,
                loggerCtx,
            );
        });
    }
}
