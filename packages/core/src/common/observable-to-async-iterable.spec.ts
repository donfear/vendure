import { Subject } from 'rxjs';
import { describe, expect, it } from 'vitest';

import { MAX_BUFFERED_VALUES, observableToAsyncIterable } from './observable-to-async-iterable';

describe('observableToAsyncIterable()', () => {
    it('delivers a value to a consumer which is already waiting', async () => {
        const source = new Subject<number>();
        const iterator = observableToAsyncIterable(source);
        const next = iterator.next();

        source.next(1);

        expect(await next).toEqual({ value: 1, done: false });
    });

    it('buffers values which arrive before they are consumed, in order', async () => {
        const source = new Subject<number>();
        const iterator = observableToAsyncIterable(source);

        source.next(1);
        source.next(2);

        expect((await iterator.next()).value).toBe(1);
        expect((await iterator.next()).value).toBe(2);
    });

    it('completes when the source completes', async () => {
        const source = new Subject<number>();
        const iterator = observableToAsyncIterable(source);

        source.complete();

        expect(await iterator.next()).toEqual({ value: undefined, done: true });
    });

    it('delivers the buffered values before completing', async () => {
        const source = new Subject<number>();
        const iterator = observableToAsyncIterable(source);

        source.next(1);
        source.complete();

        expect((await iterator.next()).value).toBe(1);
        expect(await iterator.next()).toEqual({ value: undefined, done: true });
    });

    it('rejects a waiting consumer when the source errors', async () => {
        const source = new Subject<number>();
        const iterator = observableToAsyncIterable(source);
        const next = iterator.next();

        source.error(new Error('boom'));

        await expect(next).rejects.toThrow('boom');
    });

    it('rejects the next call when the source errored before it', async () => {
        const source = new Subject<number>();
        const iterator = observableToAsyncIterable(source);

        source.error(new Error('boom'));

        await expect(iterator.next()).rejects.toThrow('boom');
        // the error is delivered once, then the iterator is done
        expect(await iterator.next()).toEqual({ value: undefined, done: true });
    });

    it('converts a non-Error source error into an Error', async () => {
        const source = new Subject<number>();
        const iterator = observableToAsyncIterable(source);

        source.error('just a string');

        await expect(iterator.next()).rejects.toThrow('just a string');
    });

    it('fails rather than buffering without limit', async () => {
        const source = new Subject<number>();
        const iterator = observableToAsyncIterable(source);

        for (let i = 0; i <= MAX_BUFFERED_VALUES; i++) {
            source.next(i);
        }

        await expect(iterator.next()).rejects.toThrow(/faster than this subscription/);
    });

    it('unsubscribes from the source when the consumer stops iterating', async () => {
        const source = new Subject<number>();
        const iterator = observableToAsyncIterable(source);
        expect(source.observed).toBe(true);

        await iterator.return?.();

        expect(source.observed).toBe(false);
        expect(await iterator.next()).toEqual({ value: undefined, done: true });
    });

    it('unsubscribes from the source when the consumer throws into it', async () => {
        const source = new Subject<number>();
        const iterator = observableToAsyncIterable(source);

        await expect(iterator.throw?.(new Error('from the consumer'))).rejects.toThrow('from the consumer');
        expect(source.observed).toBe(false);
    });
});
