import { ID } from '@vendure/common/lib/shared-types';

import { InjectableStrategy } from '../../common/types/injectable-strategy';

/**
 * @description
 * Identifies the origin of a relayed subscription message, so that a subscriber on another
 * instance can apply the same Channel and owner scoping it would apply locally.
 *
 * @docsCategory subscriptions
 * @docsPage SubscriptionRelayStrategy
 * @since 3.8.0
 */
export interface SubscriptionMessageOrigin {
    /** The Channels the payload belongs to; a subscriber receives it in any one of them. */
    channelIds: ID[];
    sessionId?: ID;
    activeUserId?: ID;
}

/**
 * @description
 * A subscription payload as it travels between server instances.
 *
 * @docsCategory subscriptions
 * @docsPage SubscriptionRelayStrategy
 * @since 3.8.0
 */
export interface RelayedSubscriptionMessage {
    /** The name of the stream, by convention the name of the GraphQL Subscription field. */
    key: string;
    payload: unknown;
    origin: SubscriptionMessageOrigin;
}

/**
 * @description
 * Receives what a {@link SubscriptionRelayStrategy} delivers for one subscription.
 *
 * @docsCategory subscriptions
 * @docsPage SubscriptionRelayStrategy
 * @since 3.8.0
 */
export interface SubscriptionRelayListener {
    next(message: RelayedSubscriptionMessage): void;
    /**
     * @description
     * Called when the relay can no longer guarantee delivery, e.g. because its connection was
     * lost and messages published in the meantime are gone. The subscription is failed, so that
     * the client refetches its data and subscribes again.
     */
    error(error: Error): void;
}

/**
 * @description
 * Determines how a published subscription payload reaches the server instances which have
 * subscribers. The default {@link InMemorySubscriptionRelayStrategy} delivers within a single
 * process; use the {@link RedisSubscriptionRelayStrategy} for a multi-instance deployment or when
 * publishing from the worker.
 *
 * :::info
 *
 * This is configured via the `apiOptions.subscriptions.relayStrategy` property of your
 * VendureConfig.
 *
 * :::
 *
 * @docsCategory subscriptions
 * @docsPage SubscriptionRelayStrategy
 * @docsWeight 0
 * @since 3.8.0
 */
export interface SubscriptionRelayStrategy extends InjectableStrategy {
    /**
     * @description
     * Delivers the message to every instance subscribed to `message.key`, including the instance
     * it was published from.
     */
    publish(message: RelayedSubscriptionMessage): Promise<void>;

    /**
     * @description
     * Subscribes to the given key, returning a function which cancels the subscription and never
     * rejects.
     */
    subscribe(key: string, listener: SubscriptionRelayListener): Promise<() => Promise<void>>;
}
