import { Subject, Subscription } from 'rxjs';

import { RelayedSubscriptionMessage, SubscriptionRelayStrategy } from './subscription-relay-strategy';

/**
 * @description
 * The default {@link SubscriptionRelayStrategy}, which delivers messages within a single process.
 * Events published by another server instance or by the worker are not delivered - configure the
 * {@link RedisSubscriptionRelayStrategy} for that.
 *
 * @docsCategory subscriptions
 */
export class InMemorySubscriptionRelayStrategy implements SubscriptionRelayStrategy {
    private subjects = new Map<string, Subject<RelayedSubscriptionMessage>>();

    destroy() {
        for (const subject of this.subjects.values()) {
            subject.complete();
        }
        this.subjects.clear();
    }

    publish(message: RelayedSubscriptionMessage): Promise<void> {
        const subject = this.subjects.get(message.key);
        if (!subject) {
            return Promise.resolve();
        }
        // Serialized like a distributed strategy would, so that a payload which does not survive
        // the trip fails in development rather than only in production.
        subject.next(JSON.parse(JSON.stringify(message)) as RelayedSubscriptionMessage);
        return Promise.resolve();
    }

    subscribe(
        key: string,
        handler: (message: RelayedSubscriptionMessage) => void,
    ): Promise<() => Promise<void>> {
        let subject = this.subjects.get(key);
        if (!subject) {
            subject = new Subject<RelayedSubscriptionMessage>();
            this.subjects.set(key, subject);
        }
        const subscription: Subscription = subject.subscribe(message => handler(message));
        return Promise.resolve(() => {
            subscription.unsubscribe();
            const existing = this.subjects.get(key);
            if (existing && !existing.observed) {
                existing.complete();
                this.subjects.delete(key);
            }
            return Promise.resolve();
        });
    }
}
