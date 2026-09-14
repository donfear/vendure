import { Args, Parent, ResolveField, Resolver, Subscription } from '@nestjs/graphql';
import { Permission } from '@vendure/common/lib/generated-types';
import { ID } from '@vendure/common/lib/shared-types';

import { idsAreEqual } from '../../../common/utils';
import {
    ORDER_UPDATED_FIELD,
    OrderUpdatedPayload,
} from '../../../service/helpers/order-updated-publisher/order-updated-publisher';
import { OrderService } from '../../../service/services/order.service';
import { SubscriptionService } from '../../../service/services/subscription.service';
import { RequestContext } from '../../common/request-context';
import { Allow } from '../../decorators/allow.decorator';
import { Ctx } from '../../decorators/request-context.decorator';

@Resolver()
export class OrderSubscriptionResolver {
    constructor(private subscriptionService: SubscriptionService) {}

    @Subscription(ORDER_UPDATED_FIELD)
    @Allow(Permission.ReadOrder)
    orderUpdated(@Ctx() ctx: RequestContext, @Args() args: { orderId?: ID }) {
        return this.subscriptionService.fromPublished<OrderUpdatedPayload>(ctx, ORDER_UPDATED_FIELD, {
            accept: payload => !args.orderId || idsAreEqual(payload.orderId, args.orderId),
        });
    }
}

@Resolver('OrderUpdatedPayload')
export class OrderUpdatedPayloadResolver {
    constructor(private orderService: OrderService) {}

    /** Resolved with the subscriber's own ctx, so it is scoped to their Channel & language. */
    @ResolveField()
    order(@Ctx() ctx: RequestContext, @Parent() payload: OrderUpdatedPayload) {
        return this.orderService.findOne(ctx, payload.orderId);
    }
}
