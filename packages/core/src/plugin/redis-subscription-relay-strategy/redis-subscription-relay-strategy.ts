import { Subject, Subscription } from 'rxjs';

import { Logger } from '../../config/logger/vendure-logger';
import {
    RelayedSubscriptionMessage,
    SubscriptionRelayStrategy,
} from '../../config/subscriptions/subscription-relay-strategy';

const loggerCtx = 'RedisSubscriptionRelayStrategy';
const DEFAULT_NAMESPACE = 'vendure:subscriptions';

/**
 * @description
 * Configuration options for the {@link RedisSubscriptionRelayStrategy}.
 *
 * @docsCategory subscriptions
 * @docsPage RedisSubscriptionRelayStrategy
 */
export interface RedisSubscriptionRelayStrategyOptions {
    /**
     * @description
     * The prefix used for all Redis channels, so that a Redis instance can be shared between
     * applications.
     *
     * @default 'vendure:subscriptions'
     */
    namespace?: string;
    /**
     * @description
     * Options passed to the `ioredis` client.
     */
    redisOptions?: import('ioredis').RedisOptions;
}

/**
 * @description
 * A {@link SubscriptionRelayStrategy} which uses Redis pub/sub to deliver subscription payloads to
 * every server instance, including those published by the worker process.
 *
 * Note: To use this strategy, you need to manually install the `ioredis` package:
 *
 * ```shell
 * npm install ioredis\@^5.3.2
 * ```
 *
 * @example
 * ```ts
 * import { RedisSubscriptionRelayStrategy, VendureConfig } from '\@vendure/core';
 *
 * export const config: VendureConfig = {
 *   apiOptions: {
 *     subscriptions: {
 *       enabled: true,
 *       relayStrategy: new RedisSubscriptionRelayStrategy({
 *         redisOptions: { host: 'localhost', port: 6379 },
 *       }),
 *     },
 *   },
 *   // ...
 * };
 * ```
 *
 * @docsCategory subscriptions
 * @docsPage RedisSubscriptionRelayStrategy
 * @docsWeight 0
 */
export class RedisSubscriptionRelayStrategy implements SubscriptionRelayStrategy {
    private publisher: import('ioredis').Redis;
    private subscriber: import('ioredis').Redis;
    private subjects = new Map<string, Subject<RelayedSubscriptionMessage>>();

    constructor(private options: RedisSubscriptionRelayStrategyOptions = {}) {}

    async init() {
        if (this.publisher) {
            return;
        }
        const IORedis = await import('ioredis').then(m => m.default);
        this.publisher = new IORedis.Redis(this.options.redisOptions ?? {});
        this.subscriber = new IORedis.Redis(this.options.redisOptions ?? {});
        this.publisher.on('error', err => Logger.error(err.message, loggerCtx, err.stack));
        this.subscriber.on('error', err => Logger.error(err.message, loggerCtx, err.stack));
        this.subscriber.on('message', (channel: string, message: string) => {
            const subject = this.subjects.get(this.keyFromChannel(channel));
            if (!subject) {
                return;
            }
            try {
                subject.next(JSON.parse(message) as RelayedSubscriptionMessage);
            } catch (e: any) {
                Logger.error(`Could not parse a subscription message: ${e.message as string}`, loggerCtx);
            }
        });
    }

    async destroy() {
        for (const subject of this.subjects.values()) {
            subject.complete();
        }
        this.subjects.clear();
        await this.quit(this.subscriber);
        await this.quit(this.publisher);
        this.subscriber = undefined as any;
        this.publisher = undefined as any;
    }

    async publish(message: RelayedSubscriptionMessage): Promise<void> {
        await this.publisher?.publish(this.channel(message.key), JSON.stringify(message));
    }

    async subscribe(
        key: string,
        handler: (message: RelayedSubscriptionMessage) => void,
    ): Promise<() => Promise<void>> {
        let subject = this.subjects.get(key);
        if (!subject) {
            subject = new Subject<RelayedSubscriptionMessage>();
            this.subjects.set(key, subject);
            await this.subscriber.subscribe(this.channel(key));
        }
        const subscription: Subscription = subject.subscribe(message => handler(message));
        return async () => {
            subscription.unsubscribe();
            const existing = this.subjects.get(key);
            if (existing && !existing.observed) {
                existing.complete();
                this.subjects.delete(key);
                await this.subscriber?.unsubscribe(this.channel(key));
            }
        };
    }

    private async quit(client: import('ioredis').Redis | undefined) {
        try {
            await client?.quit();
        } catch (e: any) {
            Logger.debug(`Could not close the Redis connection: ${e.message as string}`, loggerCtx);
        }
    }

    private channel(key: string): string {
        return `${this.options.namespace ?? DEFAULT_NAMESPACE}:${key}`;
    }

    private keyFromChannel(channel: string): string {
        return channel.slice(`${this.options.namespace ?? DEFAULT_NAMESPACE}:`.length);
    }
}
