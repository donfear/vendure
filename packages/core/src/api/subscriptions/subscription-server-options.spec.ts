import { Permission } from '@vendure/common/lib/generated-types';
import express from 'express';
import { buildSchema, parse } from 'graphql';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { ConfigService } from '../../config/config.service';
import { CachedSession } from '../../config/session-cache/session-cache-strategy';
import { Channel } from '../../entity/channel/channel.entity';
import { I18nService } from '../../i18n/i18n.service';
import { SessionService } from '../../service/services/session.service';
import { IdCodecService } from '../common/id-codec.service';
import { internal_setRequestContext, RequestContext } from '../common/request-context';

import {
    assertApiPathsDoNotOverlap,
    createSubscriptionServerOptions,
    SESSION_REVALIDATION_INTERVAL_MS,
} from './subscription-server-options';

describe('assertApiPathsDoNotOverlap()', () => {
    it('accepts the default paths', () => {
        expect(() =>
            assertApiPathsDoNotOverlap({ adminApiPath: 'admin-api', shopApiPath: 'shop-api' }),
        ).not.toThrow();
    });

    it('refuses one path being a prefix of the other, which would make both servers claim a socket', () => {
        expect(() => assertApiPathsDoNotOverlap({ adminApiPath: 'api', shopApiPath: 'api/shop' })).toThrow(
            /prefix/,
        );
        expect(() =>
            assertApiPathsDoNotOverlap({ adminApiPath: 'shop-api-v2', shopApiPath: 'shop-api' }),
        ).toThrow(/prefix/);
    });
});

/**
 * Drives the graphql-ws hooks the way graphql-ws does, to prove the session guard which the
 * AuthGuard cannot provide: it runs once, when the client subscribes.
 */
describe('subscription server options: session guard', () => {
    const session = {
        id: 1,
        token: 'token',
        expires: new Date(Date.now() + 100_000),
        cacheExpiry: Date.now() / 1000 + 1000,
        user: {
            id: 1,
            identifier: 'admin',
            verified: true,
            channelPermissions: [{ id: 1, permissions: [Permission.ReadOrder] }],
        },
    } as unknown as CachedSession;
    const sessionService = { getSessionFromToken: vi.fn(async () => session) } as unknown as SessionService;
    const options = createSubscriptionServerOptions({
        schema: buildSchema('type Query { ok: Boolean } type Subscription { orderUpdated: Boolean }'),
        apiType: 'admin',
        validationRules: [],
        encodeIds: false,
        configService: {
            apiOptions: { channelTokenKey: 'vendure-token', cors: true, subscriptions: {} },
            authOptions: { tokenMethod: 'bearer', apiKeyHeaderKey: 'vendure-api-key', disableAuth: false },
            assetOptions: { assetStorageStrategy: {} },
        } as unknown as ConfigService,
        i18nService: {
            handle: () => (req: any, res: any, next: () => void) => next(),
            translateError: (req: any, error: any) => error,
        } as unknown as I18nService,
        idCodecService: {} as IdCodecService,
        sessionService,
        expressApp: express(),
    });
    const upgradeRequest = { url: '/admin-api', headers: {}, socket: {} } as any;
    const args = { document: parse('subscription { orderUpdated }') } as any;

    /** Subscribes at `now`, as the AuthGuard would, returning what onNext needs. */
    function subscribe(now: number) {
        vi.spyOn(Date, 'now').mockReturnValue(now);
        const contextValue = (options.context as any)(
            { extra: { request: upgradeRequest }, connectionParams: {} },
            'id',
            {},
        );
        const ctx = new RequestContext({
            apiType: 'admin',
            channel: new Channel({ id: 1 }),
            session,
            isAuthorized: true,
            authorizedAsOwnerOnly: false,
        });
        internal_setRequestContext(contextValue.req, ctx);
        return { ...args, contextValue };
    }
    const deliver = (execArgs: any) =>
        (options.onNext as any)({}, 'id', {}, execArgs, { data: { orderUpdated: true } });

    afterEach(() => vi.restoreAllMocks());

    it('re-reads the session before the first event, once the interval since subscribing has passed', async () => {
        const execArgs = subscribe(1_000);
        (sessionService as any).getSessionFromToken = vi.fn(async () => undefined);
        vi.spyOn(Date, 'now').mockReturnValue(1_000 + SESSION_REVALIDATION_INTERVAL_MS);

        await expect(deliver(execArgs)).rejects.toMatchObject({
            extensions: { code: 'SUBSCRIPTION_SESSION_INVALID' },
        });
    });

    it('ends the subscription once the permissions in the Channel have changed', async () => {
        const execArgs = subscribe(1_000);
        (sessionService as any).getSessionFromToken = vi.fn(async () => ({
            ...session,
            user: { ...session.user, channelPermissions: [{ id: 1, permissions: [] }] },
        }));
        vi.spyOn(Date, 'now').mockReturnValue(1_000 + SESSION_REVALIDATION_INTERVAL_MS);

        await expect(deliver(execArgs)).rejects.toMatchObject({
            extensions: { code: 'SUBSCRIPTION_SESSION_INVALID' },
        });
    });

    it('does not re-read the session within the interval, so a burst costs one lookup', async () => {
        const execArgs = subscribe(1_000);
        (sessionService as any).getSessionFromToken = vi.fn(async () => session);
        vi.spyOn(Date, 'now').mockReturnValue(1_000 + SESSION_REVALIDATION_INTERVAL_MS - 1);

        await deliver(execArgs);

        expect(sessionService.getSessionFromToken).not.toHaveBeenCalled();
    });

    it('treats a failed re-read as still valid, without leaking the failure to the client', async () => {
        const execArgs = subscribe(1_000);
        (sessionService as any).getSessionFromToken = vi.fn(async () => {
            throw new Error('ECONNREFUSED');
        });
        vi.spyOn(Date, 'now').mockReturnValue(1_000 + SESSION_REVALIDATION_INTERVAL_MS);

        await expect(deliver(execArgs)).resolves.toMatchObject({ data: { orderUpdated: true } });
    });

    it('leaves an errors-only result alone, which may precede any RequestContext', async () => {
        const contextValue = (options.context as any)(
            { extra: { request: upgradeRequest }, connectionParams: {} },
            'id',
            {},
        );

        await expect(
            (options.onNext as any)(
                {},
                'id',
                {},
                { ...args, contextValue },
                { errors: [new Error('refused')] },
            ),
        ).resolves.toMatchObject({ errors: [expect.anything()] });
    });
});
