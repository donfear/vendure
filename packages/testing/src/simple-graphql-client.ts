import { TypedDocumentNode } from '@graphql-typed-document-node/core';
import { SUPER_ADMIN_USER_IDENTIFIER, SUPER_ADMIN_USER_PASSWORD } from '@vendure/common/lib/shared-constants';
import { VendureConfig } from '@vendure/core';
import fs from 'fs';
import { DocumentNode, FormattedExecutionResult } from 'graphql';
import gql from 'graphql-tag';
import { createClient } from 'graphql-ws';
import { print } from 'graphql/language/printer';
import mime from 'mime-types';
import { stringify } from 'querystring';
import WebSocket from 'ws';

import { QueryParams } from './types';
import { createUploadPostData } from './utils/create-upload-post-data';

const LOGIN = gql`
    mutation ($username: String!, $password: String!) {
        login(username: $username, password: $password) {
            ... on CurrentUser {
                id
                identifier
                channels {
                    token
                }
            }
            ... on ErrorResult {
                errorCode
                message
            }
        }
    }
`;

/* eslint-disable no-console */
/**
 * @description
 * A minimalistic GraphQL client for populating and querying test data.
 *
 * @docsCategory testing
 */
export class SimpleGraphQLClient {
    private authToken: string;
    private channelToken: string | null = null;
    private cookies = new Map<string, string>();
    private headers: { [key: string]: any } = {
        'Apollo-Require-Preflight': 'true',
    };

    constructor(
        private vendureConfig: Required<VendureConfig>,
        private apiUrl: string = '',
    ) {}

    /**
     * @description
     * Sets the authToken to be used in each GraphQL request.
     */
    setAuthToken(token: string) {
        this.authToken = token;
        this.headers.Authorization = `Bearer ${this.authToken}`;
    }

    /**
     * @description
     * Sets the authToken to be used in each GraphQL request.
     */
    setChannelToken(token: string | null) {
        this.channelToken = token;
        if (this.vendureConfig.apiOptions.channelTokenKey) {
            this.headers[this.vendureConfig.apiOptions.channelTokenKey] = this.channelToken;
        }
    }

    /**
     * @description
     * Returns the authToken currently being used.
     */
    getAuthToken(): string {
        return this.authToken;
    }

    /**
     * @description
     * Returns the cookies which the server has set on this client, in the format of a
     * `Cookie` request header. Useful when testing the `cookie` tokenMethod.
     */
    getCookieHeader(): string {
        return [...this.cookies.entries()].map(([name, value]) => `${name}=${value}`).join('; ');
    }

    /**
     * @description
     * Performs both query and mutation operations.
     */
    async query<T = any, V extends Record<string, any> = Record<string, any>>(
        query: DocumentNode | TypedDocumentNode<T, V>,
        variables?: V,
        queryParams?: QueryParams,
    ): Promise<T> {
        const response = await this.makeGraphQlRequest(query, variables, queryParams);
        const result = await this.getResult(response);

        if (response.ok && !result.errors && result.data) {
            return result.data;
        } else {
            const errorResult = typeof result === 'string' ? { error: result } : result;
            throw new ClientError(
                { ...errorResult, status: response.status },
                { query: print(query), variables },
            );
        }
    }

    /**
     * @description
     * Performs a raw HTTP request to the given URL, but also includes the authToken & channelToken
     * headers if they have been set. Useful for testing non-GraphQL endpoints, e.g. for plugins
     * which make use of REST controllers.
     */
    async fetch(url: string, options: RequestInit = {}): Promise<Response> {
        const cookieHeader = this.getCookieHeader();
        const headers = {
            'Content-Type': 'application/json',
            ...this.headers,
            // sending back the server's cookies is what makes the `cookie` tokenMethod work
            ...(cookieHeader ? { Cookie: cookieHeader } : {}),
            ...options.headers,
        };

        const response = await fetch(url, {
            ...options,
            headers,
        });
        const authToken = response.headers.get(this.vendureConfig.authOptions.authTokenHeaderKey || '');
        if (authToken != null) {
            this.setAuthToken(authToken);
        }
        this.storeCookies(response);
        return response;
    }

    /**
     * @description
     * Opens a GraphQL subscription over a WebSocket connection to the API, and returns an
     * async iterator of the results. The iterator also exposes a `close()` method which
     * terminates the connection.
     *
     * The auth token & channel token of this client are passed in the `connectionParams` of the
     * connection. Requires `apiOptions.subscriptions.enabled` in the server config.
     *
     * @example
     * ```ts
     * const subscription = client.subscribe(gql`
     *     subscription { orderUpdated { type orderId } }
     * `);
     * const received: any[] = [];
     * void (async () => {
     *     for await (const result of subscription) {
     *         received.push(result.data);
     *     }
     * })();
     * // ... trigger an event, then:
     * subscription.close();
     * ```
     */
    subscribe<T = any, V extends Record<string, any> = Record<string, any>>(
        document: DocumentNode | TypedDocumentNode<T, V>,
        variables?: V,
        options: {
            /** Overrides the connectionParams which are sent with the ConnectionInit message. */
            connectionParams?: Record<string, unknown> | null;
            /** Additional headers to send with the WebSocket upgrade request. */
            headers?: Record<string, string>;
            /** Sends the cookies previously set by the server with the upgrade request. */
            withCookies?: boolean;
        } = {},
    ): AsyncIterableIterator<FormattedExecutionResult<T>> & { close: () => void } {
        const channelTokenKey = this.vendureConfig.apiOptions.channelTokenKey ?? 'vendure-token';
        const connectionParams =
            options.connectionParams === undefined
                ? {
                      ...(this.authToken ? { Authorization: `Bearer ${this.authToken}` } : {}),
                      ...(this.channelToken ? { [channelTokenKey]: this.channelToken } : {}),
                  }
                : (options.connectionParams ?? undefined);
        const headers = {
            ...options.headers,
            ...(options.withCookies ? { Cookie: this.getCookieHeader() } : {}),
        };
        // `graphql-ws` constructs the socket itself, so a subclass is the only way to pass
        // headers (needed for cookie-based auth) to the upgrade request.
        class WebSocketWithHeaders extends WebSocket {
            constructor(url: string, protocols?: string | string[]) {
                super(url, protocols, { headers });
            }
        }
        const client = createClient({
            url: this.apiUrl.replace(/^http/, 'ws'),
            webSocketImpl: WebSocketWithHeaders,
            connectionParams,
            retryAttempts: 0,
        });
        const iterator = client.iterate<T, V>({
            query: print(document),
            variables,
        });
        return Object.assign(iterator, {
            close: () => {
                void iterator.return?.();
                void client.dispose();
            },
        });
    }

    private storeCookies(response: Response) {
        const setCookies: string[] =
            typeof (response.headers as any).getSetCookie === 'function'
                ? (response.headers as any).getSetCookie()
                : [];
        for (const cookie of setCookies) {
            const [pair] = cookie.split(';');
            const separatorIndex = pair.indexOf('=');
            if (separatorIndex === -1) {
                continue;
            }
            this.cookies.set(pair.slice(0, separatorIndex).trim(), pair.slice(separatorIndex + 1).trim());
        }
    }

    /**
     * @description
     * Performs a query or mutation and returns the resulting status code.
     */
    async queryStatus<T = any, V extends Record<string, any> = Record<string, any>>(
        query: DocumentNode,
        variables?: V,
    ): Promise<number> {
        const response = await this.makeGraphQlRequest(query, variables);
        return response.status;
    }

    /**
     * @description
     * Attempts to log in with the specified credentials.
     */
    async asUserWithCredentials(username: string, password: string) {
        // first log out as the current user
        if (this.authToken) {
            await this.query(gql`
                mutation {
                    logout {
                        success
                    }
                }
            `);
        }
        const result = await this.query(LOGIN, { username, password });
        if (result.login.channels?.length === 1) {
            this.setChannelToken(result.login.channels[0].token);
        }
        return result.login;
    }

    /**
     * @description
     * Logs in as the SuperAdmin user.
     */
    async asSuperAdmin() {
        const { superadminCredentials } = this.vendureConfig.authOptions;
        await this.asUserWithCredentials(
            superadminCredentials?.identifier ?? SUPER_ADMIN_USER_IDENTIFIER,
            superadminCredentials?.password ?? SUPER_ADMIN_USER_PASSWORD,
        );
    }

    /**
     * @description
     * Logs out so that the client is then treated as an anonymous user.
     */
    async asAnonymousUser() {
        await this.query(gql`
            mutation {
                logout {
                    success
                }
            }
        `);
    }

    private async makeGraphQlRequest(
        query: DocumentNode,
        variables?: { [key: string]: any },
        queryParams?: QueryParams,
    ): Promise<Response> {
        const queryString = print(query);
        const body = JSON.stringify({
            query: queryString,
            variables: variables ? variables : undefined,
        });

        const url = queryParams ? this.apiUrl + `?${stringify(queryParams)}` : this.apiUrl;

        return this.fetch(url, {
            method: 'POST',
            body,
        });
    }

    private async getResult(response: Response): Promise<any> {
        const contentType = response.headers.get('Content-Type');
        if (contentType && contentType.startsWith('application/json')) {
            return response.json();
        } else {
            return response.text();
        }
    }

    /**
     * @description
     * Perform a file upload mutation.
     *
     * Upload spec: https://github.com/jaydenseric/graphql-multipart-request-spec
     *
     * Discussion of issue: https://github.com/jaydenseric/apollo-upload-client/issues/32
     *
     * @param mutation - GraphQL document for a mutation that has input files
     * with the Upload type.
     * @param filePaths - Array of paths to files, in the same order that the
     * corresponding Upload fields appear in the variables for the mutation.
     * @param mapVariables - Function that must return the variables for the
     * mutation, with `null` as the value for each `Upload` field.
     * @param contentTypeOverrides - Optional overrides for the `Content-Type` of individual file
     * parts, keyed by their index in `filePaths`. Used to simulate a client spoofing the
     * Content-Type header independently of the actual file contents (e.g. via a proxy tool).
     *
     * @example
     * ```ts
     * // Testing a custom mutation:
     * const result = await client.fileUploadMutation({
     *   mutation: gql`
     *     mutation AddSellerImages($input: AddSellerImagesInput!) {
     *       addSellerImages(input: $input) {
     *         id
     *         name
     *       }
     *     }
     *   `,
     *   filePaths: ['./images/profile-picture.jpg', './images/logo.png'],
     *   mapVariables: () => ({
     *     name: "George's Pans",
     *     profilePicture: null,  // corresponds to filePaths[0]
     *     branding: {
     *       logo: null  // corresponds to filePaths[1]
     *     }
     *   })
     * });
     * ```
     */
    async fileUploadMutation(options: {
        mutation: DocumentNode;
        filePaths: string[];
        mapVariables: (filePaths: string[]) => any;
        contentTypeOverrides?: { [index: number]: string };
    }): Promise<any> {
        const { mutation, filePaths, mapVariables, contentTypeOverrides } = options;

        const postData = createUploadPostData(mutation, filePaths, mapVariables);
        const body = new FormData();
        body.append('operations', JSON.stringify(postData.operations));
        body.append(
            'map',
            '{' +
                Object.entries(postData.map)
                    .map(([i, mapPath]) => `"${i}":["${mapPath}"]`)
                    .join(',') +
                '}',
        );
        postData.filePaths.forEach((filePath, index) => {
            const file = fs.readFileSync(filePath.file);
            // Native FormData inherits its part Content-Type from the Blob's
            // `type` field. `form-data` previously did this lookup automatically
            // via the `mime-types` package, so we reproduce it explicitly. An
            // explicit `contentTypeOverrides` entry takes precedence, allowing a
            // test to spoof the part's Content-Type independently of the file.
            const type = contentTypeOverrides?.[index] ?? (mime.lookup(filePath.file) || undefined);
            const blob = type ? new Blob([file], { type }) : new Blob([file]);
            body.append(filePath.name, blob, filePath.file);
        });

        const result = await fetch(this.apiUrl, {
            method: 'POST',
            body,
            headers: {
                ...this.headers,
            },
        });
        const response = (await result.json()) as any;
        if (response.errors && response.errors.length) {
            const error = response.errors[0];
            throw new Error(error.message);
        }
        return response.data;
    }
}

export class ClientError extends Error {
    constructor(
        public response: any,
        public request: any,
    ) {
        super(ClientError.extractMessage(response));
    }
    private static extractMessage(response: any): string {
        if (response.errors) {
            return response.errors[0].message;
        } else {
            return `GraphQL Error (Code: ${response.status as number})`;
        }
    }
}
