import { pollUntil } from './poll-until';

/**
 * Consumes a subscription opened with `SimpleGraphQLClient.subscribe()` in the background,
 * recording every result and any error, so that a test can trigger events and then inspect
 * what was delivered.
 */
export function collectSubscription(subscription: AsyncIterableIterator<any> & { close: () => void }) {
    const results: any[] = [];
    const state: { error?: any; ended: boolean } = { ended: false };
    const done = (async () => {
        try {
            for await (const result of subscription) {
                results.push(result);
            }
        } catch (e) {
            state.error = e;
        } finally {
            state.ended = true;
        }
    })();
    return {
        results,
        done,
        close: () => subscription.close(),
        /** An operation may fail via a connection error or a result carrying GraphQL errors. */
        get errorText() {
            const fromResults = results.flatMap(r => r.errors ?? []);
            return JSON.stringify([state.error, ...fromResults].filter(Boolean), (key, value) =>
                value instanceof Error ? value.message : value,
            );
        },
        get hasError() {
            return !!state.error || results.some(r => r.errors?.length);
        },
        get hasEnded() {
            return state.ended;
        },
    };
}

/**
 * The graphql-transport-ws protocol gives a client no signal for "subscription established", so
 * an event triggered right after subscribing may precede it. A harmless trigger is therefore
 * repeated until the subscription has demonstrably received it.
 */
export async function triggerUntilReceived(
    trigger: () => Promise<unknown>,
    received: () => boolean,
    attempts = 20,
): Promise<void> {
    for (let i = 0; i < attempts; i++) {
        await trigger();
        const ok = await pollUntil(received, { timeout: 250 }).then(
            () => true,
            () => false,
        );
        if (ok) {
            return;
        }
    }
    throw new Error(`The subscription received nothing after ${attempts} triggers`);
}

export function delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
