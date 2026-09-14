import { Args, Mutation, Parent, ResolveField, Resolver, Subscription } from '@nestjs/graphql';
import {
    Allow,
    Ctx,
    EventBus,
    ID,
    OrderEvent,
    OrderService,
    Permission,
    PluginCommonModule,
    ProductService,
    RequestContext,
    SubscriptionService,
    VendureEvent,
    VendurePlugin,
} from '@vendure/core';
import gql from 'graphql-tag';

/** Published by the `triggerTestEvent` mutation, so tests can emit into a Channel on demand. */
export class TestEvent extends VendureEvent {
    constructor(
        public ctx: RequestContext,
        public message: string,
        public productId: ID,
    ) {
        super();
    }
}

interface TestEventPayload {
    message: string;
    productId: ID;
}

interface ActiveOrderUpdatedPayload {
    orderId: ID;
    type: string;
}

@Resolver()
export class TestEventResolver {
    constructor(
        private subscriptionService: SubscriptionService,
        private eventBus: EventBus,
    ) {}

    @Subscription('testEvent')
    @Allow(Permission.ReadCatalog)
    testEvent(@Ctx() ctx: RequestContext, @Args() args: { message?: string }) {
        return this.subscriptionService.fromEvent(ctx, 'testEvent', TestEvent, event =>
            !args.message || event.message === args.message
                ? { message: event.message, productId: event.productId }
                : undefined,
        );
    }

    @Mutation()
    @Allow(Permission.Authenticated)
    async triggerTestEvent(@Ctx() ctx: RequestContext, @Args() args: { message: string; productId: ID }) {
        await this.eventBus.publish(new TestEvent(ctx, args.message, args.productId));
        return true;
    }
}

@Resolver('TestEventPayload')
export class TestEventPayloadResolver {
    constructor(private productService: ProductService) {}

    @ResolveField()
    product(@Ctx() ctx: RequestContext, @Parent() payload: TestEventPayload) {
        return this.productService.findOne(ctx, payload.productId, ['featuredAsset']);
    }
}

/** The worked example from the subscriptions guide, so the documented path is under test. */
@Resolver()
export class ActiveOrderSubscriptionResolver {
    constructor(private subscriptionService: SubscriptionService) {}

    // Owner scoping is applied automatically, because the resolver requires Permission.Owner.
    @Subscription('activeOrderUpdated')
    @Allow(Permission.Owner)
    activeOrderUpdated(@Ctx() ctx: RequestContext) {
        return this.subscriptionService.fromEvent(ctx, 'activeOrderUpdated', OrderEvent, event => ({
            orderId: event.entity.id,
            type: event.type,
        }));
    }
}

@Resolver('ActiveOrderUpdatedPayload')
export class ActiveOrderUpdatedPayloadResolver {
    constructor(private orderService: OrderService) {}

    @ResolveField()
    order(@Ctx() ctx: RequestContext, @Parent() payload: ActiveOrderUpdatedPayload) {
        return this.orderService.findOne(ctx, payload.orderId);
    }
}

@VendurePlugin({
    imports: [PluginCommonModule],
    shopApiExtensions: {
        resolvers: [ActiveOrderSubscriptionResolver, ActiveOrderUpdatedPayloadResolver],
        schema: gql`
            extend type Subscription {
                activeOrderUpdated: ActiveOrderUpdatedPayload!
            }

            type ActiveOrderUpdatedPayload {
                orderId: ID!
                type: String!
                order: Order
            }
        `,
    },
    adminApiExtensions: {
        resolvers: [TestEventResolver, TestEventPayloadResolver],
        schema: gql`
            extend type Subscription {
                testEvent(message: String): TestEventPayload!
            }

            extend type Mutation {
                triggerTestEvent(message: String!, productId: ID!): Boolean!
            }

            type TestEventPayload {
                message: String!
                productId: ID!
                product: Product
            }
        `,
    },
})
export class SubscriptionsTestPlugin {}
