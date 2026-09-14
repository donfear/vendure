import { Subject } from 'rxjs';

import { Logger } from '../../config/logger/vendure-logger';
import {
    RelayedSubscriptionMessage,
    SubscriptionRelayListener,
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
 * @since 3.8.0
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

/** A Redis channel subscription shared by every local subscriber of one key. */
interface ChannelSubscription {
    /** Carries the serialized message: each listener gets its own copy. */
    subject: Subject<string>;
    /** Resolves once Redis has confirmed the SUBSCRIBE, so every subscriber sees a failure. */
    ready: Promise<void>;
}

/**
 * @description
 * A {@link SubscriptionRelayStrategy} which uses Redis pub/sub to deliver subscription payloads to
 * every server instance, including those published by the worker process.
 *
 * Redis pub/sub does not replay: if the connection drops, whatever was published in the meantime
 * is gone. Every open subscription is therefore failed when that happens, so that clients refetch
 * their data and subscribe again.
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
 * @since 3.8.0
 */
export class RedisSubscriptionRelayStrategy implements SubscriptionRelayStrategy {
    private publisher: import('ioredis').Redis | undefined;
    /** Created on first use: a connection in subscriber mode is useless to a process which never subscribes. */
    private subscriber: Promise<import('ioredis').Redis> | undefined;
    private channels = new Map<string, ChannelSubscription>();

    constructor(private options: RedisSubscriptionRelayStrategyOptions = {}) {}

    async init() {
        this.publisher = this.publisher ?? (await this.createClient());
    }

    async destroy() {
        this.failAll(new Error('The server is shutting down'));
        await this.quit(await this.subscriber?.catch(() => undefined));
        await this.quit(this.publisher);
        this.subscriber = undefined;
        this.publisher = undefined;
    }

    async publish(message: RelayedSubscriptionMessage): Promise<void> {
        if (!this.publisher) {
            Logger.warn(`Not initialized: the "${message.key}" message was dropped`, loggerCtx);
            return;
        }
        await this.publisher.publish(this.channel(message.key), JSON.stringify(message));
    }

    async subscribe(key: string, listener: SubscriptionRelayListener): Promise<() => Promise<void>> {
        let channel = this.channels.get(key);
        if (!channel) {
            const subject = new Subject<string>();
            const ready = this.getSubscriber().then(subscriber => subscriber.subscribe(this.channel(key)));
            channel = { subject, ready: ready.then(() => undefined) };
            this.channels.set(key, channel);
        }
        const { subject } = channel;
        try {
            await channel.ready;
        } catch (e) {
            // Nobody can be attached to a Redis channel which was never subscribed to.
            if (this.channels.get(key) === channel) {
                this.channels.delete(key);
            }
            throw e;
        }
        const subscription = subject.subscribe({
            next: serialized => {
                try {
                    listener.next(JSON.parse(serialized) as RelayedSubscriptionMessage);
                } catch (e: any) {
                    Logger.error(`Could not parse a subscription message: ${e.message as string}`, loggerCtx);
                }
            },
            error: err => listener.error(err),
        });
        return async () => {
            subscription.unsubscribe();
            if (this.channels.get(key) === channel && !subject.observed) {
                this.channels.delete(key);
                try {
                    await (await this.subscriber)?.unsubscribe(this.channel(key));
                } catch (e: any) {
                    Logger.debug(`Could not unsubscribe from Redis: ${e.message as string}`, loggerCtx);
                }
            }
        };
    }

    /** Memoized as a promise, so that concurrent first subscribers share one connection. */
    private getSubscriber(): Promise<import('ioredis').Redis> {
        if (!this.subscriber) {
            this.subscriber = this.createClient().then(subscriber => {
                subscriber.on('message', (channel: string, message: string) => {
                    this.channels.get(this.keyFromChannel(channel))?.subject.next(message);
                });
                // ioredis re-subscribes on reconnect, but nothing published in between is replayed.
                let reconnected = false;
                subscriber.on('reconnecting', () => {
                    reconnected = true;
                    this.failAll(
                        new Error('The connection to Redis was lost, so events may have been missed'),
                    );
                });
                // ... and it re-subscribes to every channel, including those nobody came back for.
                subscriber.on('ready', () => {
                    if (reconnected) {
                        reconnected = false;
                        void this.resubscribe(subscriber);
                    }
                });
                return subscriber;
            });
            this.subscriber.catch(() => (this.subscriber = undefined));
        }
        return this.subscriber;
    }

    /** Leaves the connection subscribed to exactly the channels which have local subscribers. */
    private async resubscribe(subscriber: import('ioredis').Redis) {
        try {
            await subscriber.unsubscribe();
            const wanted = [...this.channels.keys()].map(key => this.channel(key));
            if (wanted.length) {
                await subscriber.subscribe(...wanted);
            }
        } catch (e: any) {
            Logger.warn(`Could not re-subscribe after a reconnect: ${e.message as string}`, loggerCtx);
        }
    }

    private failAll(error: Error) {
        const channels = [...this.channels.values()];
        this.channels.clear();
        for (const { subject } of channels) {
            subject.error(error);
        }
    }

    private async createClient(): Promise<import('ioredis').Redis> {
        const IORedis = await import('ioredis').then(m => m.default);
        const client = new IORedis.Redis(this.options.redisOptions ?? {});
        client.on('error', err => Logger.error(err.message, loggerCtx, err.stack));
        return client;
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
