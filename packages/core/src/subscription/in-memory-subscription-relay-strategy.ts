import { Json } from '@vendure/common/lib/shared-types';
import { EventEmitter } from 'events';

import { Injector } from '../common/injector';
import { ConfigService } from '../config/config.service';
import { Logger } from '../config/logger/vendure-logger';
import { ProcessContext } from '../process-context/process-context';

import { SubscriptionRelayStrategy } from './subscription-relay-strategy';

const loggerCtx = 'InMemorySubscriptionRelayStrategy';

/**
 * @description
 * The default {@link SubscriptionRelayStrategy}, which relays the results within the process in
 * which an event is raised. So it only suits a Vendure server which runs in a single process: the
 * results of the events raised in a worker which runs in its own process, or on another server
 * instance, do not reach the clients.
 *
 * @docsCategory subscriptions
 * @docsPage SubscriptionRelayStrategy
 * @since 3.8.0
 */
export class InMemorySubscriptionRelayStrategy implements SubscriptionRelayStrategy {
    private readonly emitter = new EventEmitter();

    init(injector: Injector) {
        if (injector.get(ProcessContext).isWorker && injector.get(ConfigService).apiOptions.subscriptions) {
            Logger.warn(
                'The results of the subscriptions which are published in the worker do not reach the ' +
                    'server. Use the RedisSubscriptionRelayStrategy to relay them.',
                loggerCtx,
            );
        }
    }

    async publish(topic: string, message: Json) {
        // Relayed as JSON, as between processes, so that the results are the same with every strategy
        this.emitter.emit(topic, JSON.parse(JSON.stringify(message)));
    }

    async subscribe(topic: string, onMessage: (message: Json) => void) {
        this.emitter.on(topic, onMessage);
        return () => {
            this.emitter.off(topic, onMessage);
        };
    }
}
