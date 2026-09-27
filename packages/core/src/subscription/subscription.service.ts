import { Injectable } from '@nestjs/common';
import { ID, Json } from '@vendure/common/lib/shared-types';
import { inspect } from 'util';

import { RequestContext } from '../api/common/request-context';
import { ConfigService } from '../config/config.service';
import { Logger } from '../config/logger/vendure-logger';
import { EventBus } from '../event-bus/event-bus';

// A type import, since event-subscription.ts imports this file
type AnyEventSubscription = import('./event-subscription').EventSubscription<any>;
type Client = (payload: Json) => void;

const loggerCtx = 'SubscriptionService';

interface Topic {
    clients: Set<Client>;
    subscribed: Promise<() => void>;
}

interface Membership {
    subscribed: Promise<unknown>;
    leave: () => void;
}

/**
 * Registers the EventSubscriptions and delivers their results to the clients of this process
 * through the SubscriptionRelayStrategy. It is used by the EventSubscription class.
 */
@Injectable()
export class SubscriptionService {
    private readonly subscriptions = new Map<string, AnyEventSubscription>();
    private readonly topics = new Map<string, Topic>();

    constructor(
        private configService: ConfigService,
        private eventBus: EventBus,
    ) {}

    register(subscription: AnyEventSubscription) {
        if (!this.configService.apiOptions.subscriptions) {
            return;
        }
        const { name } = subscription;
        const registered = this.subscriptions.get(name);
        if (registered) {
            // Their results would reach each other's clients
            throw new Error(
                `${registered.constructor.name} and ${subscription.constructor.name} are both named "${name}"`,
            );
        }
        const events = new Set(Array.isArray(subscription.event) ? subscription.event : [subscription.event]);
        if (events.size === 0) {
            throw new Error(`${subscription.constructor.name} has no event`);
        }
        this.subscriptions.set(name, subscription);
        let published = Promise.resolve();
        for (const event of events) {
            this.eventBus.ofType(event).subscribe(e => {
                // The results are published one at a time, in the order of their events
                published = published.then(() => this.publish(subscription, e));
            });
        }
    }

    listen(subscription: AnyEventSubscription, ctx: RequestContext, key?: ID): AsyncIterableIterator<Json> {
        if (this.subscriptions.get(subscription.name) !== subscription) {
            throw new Error(
                this.configService.apiOptions.subscriptions
                    ? `${subscription.constructor.name} is not registered: it must be a provider of a plugin, ` +
                          'and call super.onApplicationBootstrap() if it overrides it'
                    : 'Subscriptions are not enabled by apiOptions.subscriptions',
            );
        }
        const isKeyed = typeof subscription.key === 'function';
        if (isKeyed !== (key != null)) {
            throw new Error(
                `The results of "${subscription.name}" ` +
                    (isKeyed ? 'are delivered by key, so a key is needed to listen' : 'have no key'),
            );
        }
        const topic = getTopic(subscription.name, ctx.channelId, key);
        return new ResultIterator(
            client => this.join(topic, client),
            payload => ({ [subscription.name]: payload }),
            subscription.filter && (payload => this.accepts(subscription, ctx, payload)),
        );
    }

    private async publish(subscription: AnyEventSubscription, event: any) {
        // An error thrown in an EventBus subscriber would crash the process, so they are all logged
        try {
            const key = await subscription.key?.(event);
            if (subscription.key && key == null) {
                return;
            }
            const channelIds: ID[] = await (subscription.channelIds?.(event) ?? [event.ctx.channelId]);
            // A Channel which is listed twice still receives the result once
            const topics = new Set(channelIds.map(channelId => getTopic(subscription.name, channelId, key)));
            if (topics.size === 0) {
                return;
            }
            const payload = await subscription.payload(event);
            for (const topic of topics) {
                // The next result does not wait for the relay, which keeps the order of the messages
                void this.relay(subscription, topic, payload);
            }
        } catch (err) {
            logError(`Could not publish a result of "${subscription.name}"`, err);
        }
    }

    private async relay(subscription: AnyEventSubscription, topic: string, payload: Json) {
        try {
            await this.configService.apiOptions.subscriptionRelayStrategy.publish(topic, payload);
        } catch (err) {
            logError(`Could not publish a result of "${subscription.name}"`, err);
        }
    }

    private async accepts(subscription: AnyEventSubscription, ctx: RequestContext, payload: Json) {
        try {
            // Services cache data per RequestContext, so each result is filtered with a copy, which is
            // prepared for the EntityAccessControlStrategy like that of an HTTP request
            const current = ctx.copy();
            const { disableAuth, entityAccessControlStrategy } = this.configService.authOptions;
            if (!disableAuth) {
                await entityAccessControlStrategy.prepareAccessControl?.(current);
            }
            return (await subscription.filter?.(current, payload)) === true;
        } catch (err) {
            logError(`Could not filter a result of "${subscription.name}"`, err);
            return false;
        }
    }

    /**
     * Adds a client to a topic, which is subscribed to through the relay while it has clients.
     */
    private join(name: string, client: Client): Membership {
        const topic = this.topics.get(name) ?? this.createTopic(name);
        const { clients, subscribed } = topic;
        clients.add(client);
        const removeTopic = () => {
            // A client which joins in the meantime keeps the topic, and a topic is only removed once
            const isUnused = clients.size === 0 && this.topics.get(name) === topic;
            if (isUnused) {
                this.topics.delete(name);
            }
            return isUnused;
        };
        const leave = () => {
            clients.delete(client);
            if (clients.size === 0) {
                subscribed
                    .then(unsubscribe => removeTopic() && unsubscribe(), removeTopic)
                    .catch(err => logError(`Could not unsubscribe from "${name}"`, err));
            }
        };
        return { subscribed, leave };
    }

    private createTopic(name: string): Topic {
        const clients = new Set<Client>();
        const { subscriptionRelayStrategy } = this.configService.apiOptions;
        const topic = {
            clients,
            subscribed: subscriptionRelayStrategy.subscribe(name, payload =>
                clients.forEach(client => client(payload)),
            ),
        };
        this.topics.set(name, topic);
        return topic;
    }
}

function getTopic(name: string, channelId: ID, key?: ID) {
    return key == null ? `${name}:${channelId}` : `${name}:${channelId}:${key}`;
}

function logError(message: string, err: unknown) {
    const error = err instanceof Error ? err : undefined;
    const reason = error?.message ?? (typeof err === 'string' ? err : inspect(err));
    Logger.error(`${message}: ${reason}`, loggerCtx, error?.stack);
}

/**
 * Yields the results of a topic to one client, in order, if the filter accepts them. It joins the
 * topic when it is first read, so that an iterator which is never read holds nothing, and leaves it
 * when it returns.
 */
class ResultIterator implements AsyncIterableIterator<Json> {
    private readonly results: Json[] = [];
    private readonly pulls: Array<{
        resolve: (result: IteratorResult<Json>) => void;
        reject: (error: unknown) => void;
    }> = [];
    private leave?: () => void;
    private filtered = Promise.resolve();
    private done = false;
    private failure?: { error: unknown };

    constructor(
        private join: (client: Client) => Membership,
        private toResult: (payload: Json) => Json,
        private accepts?: (payload: Json) => Promise<boolean>,
    ) {}

    next(): Promise<IteratorResult<Json>> {
        if (!this.leave && !this.done) {
            const { subscribed, leave } = this.join(payload => this.receive(payload));
            this.leave = leave;
            subscribed.catch(error => this.fail(error));
        }
        const queued = this.results.shift();
        if (queued !== undefined) {
            return Promise.resolve({ value: queued, done: false });
        }
        if (this.failure) {
            return Promise.reject(this.failure.error);
        }
        if (this.done) {
            return Promise.resolve({ value: undefined, done: true });
        }
        return new Promise((resolve, reject) => this.pulls.push({ resolve, reject }));
    }

    return(): Promise<IteratorResult<Json>> {
        if (!this.done) {
            this.done = true;
            this.results.length = 0;
            this.pulls.splice(0).forEach(pull => pull.resolve({ value: undefined, done: true }));
            this.leave?.();
        }
        return Promise.resolve({ value: undefined, done: true });
    }

    throw(error: unknown): Promise<IteratorResult<Json>> {
        void this.return();
        return Promise.reject(error);
    }

    [Symbol.asyncIterator]() {
        return this;
    }

    private receive(payload: Json) {
        const { accepts } = this;
        if (!accepts) {
            this.push(this.toResult(payload));
            return;
        }
        // The results keep their order while they are filtered, and are not filtered once the
        // client has left
        this.filtered = this.filtered.then(async () => {
            if (!this.done && (await accepts(payload))) {
                this.push(this.toResult(payload));
            }
        });
    }

    private push(result: Json) {
        // A result which was being filtered when the client left is dropped
        if (this.done) {
            return;
        }
        const pull = this.pulls.shift();
        if (pull) {
            pull.resolve({ value: result, done: false });
        } else {
            this.results.push(result);
        }
    }

    private fail(error: unknown) {
        this.failure = { error };
        this.pulls.splice(0).forEach(pull => pull.reject(error));
        void this.return();
    }
}
