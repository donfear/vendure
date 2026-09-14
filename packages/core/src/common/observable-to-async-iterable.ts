import { GraphQLError } from 'graphql';
import { Observable } from 'rxjs';

/**
 * The number of values which may be buffered for a consumer which is not keeping up. Exceeding it
 * fails the iterator rather than growing the buffer without limit.
 */
export const MAX_BUFFERED_VALUES = 1000;

/**
 * Adapts an Observable to the `AsyncIterableIterator` consumed by the GraphQL subscription
 * machinery: values which arrive before the consumer asks for them are buffered, errors are
 * delivered to the consumer, and ending the iteration unsubscribes from the source.
 */
export function observableToAsyncIterable<T>(source: Observable<T>): AsyncIterableIterator<T> {
    const queue: T[] = [];
    const waiting: Array<{
        resolve: (result: IteratorResult<any>) => void;
        reject: (error: any) => void;
    }> = [];
    let done = false;
    let error: Error | undefined;

    const subscription = source.subscribe({
        next: value => {
            const consumer = waiting.shift();
            if (consumer) {
                consumer.resolve({ value, done: false });
            } else if (queue.length >= MAX_BUFFERED_VALUES) {
                fail(
                    new GraphQLError(
                        'Events were published faster than this subscription could deliver them. ' +
                            'Refetch your data and subscribe again.',
                        { extensions: { code: 'SUBSCRIPTION_BUFFER_OVERFLOW' } },
                    ),
                );
            } else {
                queue.push(value);
            }
        },
        error: err => fail(err instanceof Error ? err : new Error(String(err))),
        complete: () => stop(),
    });

    function fail(err: Error) {
        error = err;
        queue.length = 0;
        const consumer = waiting.shift();
        stop();
        if (consumer) {
            error = undefined;
            consumer.reject(err);
        }
    }

    function stop(): IteratorResult<any> {
        if (!done) {
            done = true;
            subscription.unsubscribe();
            waiting.splice(0, waiting.length).forEach(c => c.resolve({ value: undefined, done: true }));
        }
        return { value: undefined, done: true };
    }

    return {
        next() {
            const value = queue.shift();
            if (value !== undefined) {
                return Promise.resolve({ value, done: false });
            }
            if (error) {
                const thrown = error;
                error = undefined;
                return Promise.reject(thrown);
            }
            if (done) {
                return Promise.resolve({ value: undefined, done: true });
            }
            return new Promise((resolve, reject) => waiting.push({ resolve, reject }));
        },
        return: () => Promise.resolve(stop()),
        throw: (thrown: unknown) => {
            stop();
            return Promise.reject(thrown instanceof Error ? thrown : new Error(String(thrown)));
        },
        [Symbol.asyncIterator]() {
            return this;
        },
    };
}
