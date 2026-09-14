import { Subject } from 'rxjs';

import {
    RelayedSubscriptionMessage,
    SubscriptionRelayListener,
    SubscriptionRelayStrategy,
} from './subscription-relay-strategy';

/**
 * @description
 * The default {@link SubscriptionRelayStrategy}, which delivers messages within a single process.
 * Events published by another server instance or by the worker are not delivered - configure the
 * {@link RedisSubscriptionRelayStrategy} for that.
 *
 * @docsCategory subscriptions
 * @since 3.8.0
 */
export class InMemorySubscriptionRelayStrategy implements SubscriptionRelayStrategy {
    /** Carries the serialized message: each listener gets its own copy, as with a real relay. */
    private subjects = new Map<string, Subject<string>>();

    destroy() {
        for (const subject of this.subjects.values()) {
            subject.complete();
        }
        this.subjects.clear();
    }

    publish(message: RelayedSubscriptionMessage): Promise<void> {
        // Serialized like a distributed strategy would, so that a payload which does not survive
        // the trip fails in development rather than only in production.
        this.subjects.get(message.key)?.next(JSON.stringify(message));
        return Promise.resolve();
    }

    subscribe(key: string, listener: SubscriptionRelayListener): Promise<() => Promise<void>> {
        let subject = this.subjects.get(key);
        if (!subject) {
            subject = new Subject<string>();
            this.subjects.set(key, subject);
        }
        const subscription = subject.subscribe(serialized => listener.next(JSON.parse(serialized)));
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
