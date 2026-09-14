import express, { Request } from 'express';
import { describe, expect, it } from 'vitest';

import { isOriginAllowed } from './origin-check';

function request(headers: Record<string, string>): Request {
    const req = Object.create(express.request);
    req.app = express();
    req.headers = headers;
    return req;
}

describe('isOriginAllowed()', () => {
    const origin = 'https://storefront.example.com';

    it('allows a request without an Origin, which did not come from a browser', async () => {
        expect(await isOriginAllowed(request({}), { origin: ['https://other.example.com'] })).toBe(true);
    });

    it('accepts only the exact origin when cors names one (cors sets the header unconditionally)', async () => {
        expect(await isOriginAllowed(request({ origin }), { origin })).toBe(true);
        expect(await isOriginAllowed(request({ origin: 'https://evil.example.com' }), { origin })).toBe(
            false,
        );
    });

    it('accepts a wildcard only for a connection which carries no cookies', async () => {
        expect(await isOriginAllowed(request({ origin }), { origin: '*' })).toBe(true);
        expect(await isOriginAllowed(request({ origin, cookie: 'session=x' }), { origin: '*' })).toBe(false);
    });

    it('accepts a cookie-carrying connection only when cors allows credentials', async () => {
        const cookie = 'session=x';
        expect(await isOriginAllowed(request({ origin, cookie }), { origin: true })).toBe(false);
        expect(await isOriginAllowed(request({ origin, cookie }), { origin: true, credentials: true })).toBe(
            true,
        );
        expect(await isOriginAllowed(request({ origin }), { origin: true })).toBe(true);
    });

    it('requires the same origin when cors is disabled', async () => {
        const host = 'storefront.example.com';
        expect(await isOriginAllowed(request({ origin, host }), false)).toBe(true);
        expect(await isOriginAllowed(request({ origin, host: 'api.example.com' }), false)).toBe(false);
    });
});
