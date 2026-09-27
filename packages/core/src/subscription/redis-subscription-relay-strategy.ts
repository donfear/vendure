import { Json } from '@vendure/common/lib/shared-types';

import { Logger } from '../config/logger/vendure-logger';

import { SubscriptionRelayStrategy } from './subscription-relay-strategy';

const loggerCtx = 'RedisSubscriptionRelayStrategy';

/**
 * @description
 * Configuration options for the {@link RedisSubscriptionRelayStrategy}.
 *
 * @docsCategory subscriptions
 * @docsPage RedisSubscriptionRelayStrategy
 * @since 3.8.0
 */
export interface RedisSubscriptionRelayStrategyOptions {
    /**
     * @description
     * The options with which to connect to Redis.
     */
    redisOptions?: import('ioredis').RedisOptions;
    /**
     * @description
     * Prefixes the Redis channels. The server instances and the worker of a Vendure app use the same
     * namespace, and apps which share a Redis server, e.g. for staging and production, use different
     * ones, since Redis pub/sub ignores the database index.
     *
     * @default 'vendure-subscriptions'
     */
    namespace?: string;
}

/**
 * @description
 * A {@link SubscriptionRelayStrategy} which relays the results through Redis pub/sub, so that the
 * clients on every server instance receive the results of the events raised on any instance and
 * in the worker.
 *
 * It connects to Redis when it first relays a result or subscribes to one. While Redis is
 * unreachable, the server keeps running: the results of the events raised in the meantime are
 * delayed or lost, with an error in the log, and the subscriptions resume once Redis is reachable
 * again.
 *
 * Note: To use this strategy, you need to manually install the `ioredis` package:
 *
 * ```shell
 * npm install ioredis@^5.3.2
 * ```
 *
 * @example
 * ```ts
 * import { RedisSubscriptionRelayStrategy, VendureConfig } from '\@vendure/core';
 *
 * export const config: VendureConfig = {
 *   apiOptions: {
 *     subscriptions: true,
 *     subscriptionRelayStrategy: new RedisSubscriptionRelayStrategy({
 *       redisOptions: { host: 'localhost', port: 6379 },
 *     }),
 *   },
 *   // ...
 * };
 * ```
 *
 * @docsCategory subscriptions
 * @docsPage RedisSubscriptionRelayStrategy
 * @docsWeight 0
 * @since 3.8.0
 */
export class RedisSubscriptionRelayStrategy implements SubscriptionRelayStrategy {
    private publisher: import('ioredis').Redis;
    private subscriber: import('ioredis').Redis;
    private readonly listeners = new Map<string, (message: Json) => void>();

    constructor(private options: RedisSubscriptionRelayStrategyOptions = {}) {}

    async init() {
        const IORedis = await import('ioredis').then(m => m.default);
        // Connections are opened on first use, so e.g. the worker, which only publishes, opens no
        // subscriber connection
        const redisOptions = { lazyConnect: true, ...this.options.redisOptions };
        this.publisher = new IORedis.Redis(redisOptions).on('error', err => this.logError(err));
        // While Redis is unreachable, subscribing waits for it instead of failing
        this.subscriber = new IORedis.Redis({ ...redisOptions, maxRetriesPerRequest: null })
            .on('error', err => this.logError(err))
            .on('message', (channel: string, message: string) => {
                try {
                    this.listeners.get(channel)?.(JSON.parse(message));
                } catch (err: any) {
                    this.logError(err);
                }
            });
    }

    async destroy() {
        // Quitting would wait for the pending commands, which wait for Redis while it is unreachable
        await Promise.all(
            [this.publisher, this.subscriber].map(client =>
                client.status === 'ready' ? client.quit() : client.disconnect(),
            ),
        );
    }

    async publish(topic: string, message: Json) {
        await this.publisher.publish(this.getChannel(topic), JSON.stringify(message));
    }

    async subscribe(topic: string, onMessage: (message: Json) => void) {
        const channel = this.getChannel(topic);
        this.listeners.set(channel, onMessage);
        try {
            await this.subscriber.subscribe(channel);
        } catch (err) {
            this.listeners.delete(channel);
            throw err;
        }
        return () => {
            this.listeners.delete(channel);
            this.subscriber.unsubscribe(channel).catch(err => this.logError(err));
        };
    }

    private getChannel(topic: string) {
        return `${this.options.namespace ?? 'vendure-subscriptions'}:${topic}`;
    }

    private logError(err: Error) {
        Logger.error(err.message, loggerCtx, err.stack);
    }
}
