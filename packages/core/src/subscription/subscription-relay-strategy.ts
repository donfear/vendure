import { Json } from '@vendure/common/lib/shared-types';

import { InjectableStrategy } from '../common/types/injectable-strategy';

/**
 * @description
 * Relays the results of the {@link EventSubscription}s from the process in which an event is
 * raised to the server processes whose clients subscribe to them.
 *
 * The default {@link InMemorySubscriptionRelayStrategy} relays within a single process. With several
 * server instances, or a worker which runs in its own process, use the
 * {@link RedisSubscriptionRelayStrategy}.
 *
 * :::info
 *
 * This is configured via the `apiOptions.subscriptionRelayStrategy` property of
 * your VendureConfig.
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
     * Delivers the message, which is the payload of a result, to the listener of the topic in every
     * process. The next message may be published before this one is delivered, and the messages of
     * a topic are delivered in the order in which they are published.
     */
    publish(topic: string, message: Json): Promise<void>;

    /**
     * @description
     * Calls `onMessage` with each message which is published to the topic, until the returned
     * function is called. Vendure subscribes to a topic at most once at a time in each process.
     */
    subscribe(topic: string, onMessage: (message: Json) => void): Promise<() => void>;
}
