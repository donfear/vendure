/**
 * Consumes a subscription opened with `SimpleGraphQLClient.subscribe()` in the background,
 * recording every result and any error, so that a test can trigger events and then inspect
 * what was delivered.
 */
export function collectSubscription(subscription: AsyncIterableIterator<any> & { close: () => void }) {
    const results: any[] = [];
    const state: { error?: any } = {};
    const done = (async () => {
        try {
            for await (const result of subscription) {
                results.push(result);
            }
        } catch (e) {
            state.error = e;
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
    };
}

export function delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
