import { ASTVisitor, GraphQLError, OperationTypeNode, ValidationContext } from 'graphql';

/**
 * Rejects anything but a subscription over the WebSocket transport. Queries and mutations sent
 * this way would bypass CSRF prevention and every Apollo Server plugin which acts on the http
 * response (id encoding, absolute asset urls, error translation, query complexity limits).
 */
export function subscriptionOnlyRule(context: ValidationContext): ASTVisitor {
    return {
        OperationDefinition(node) {
            if (node.operation !== OperationTypeNode.SUBSCRIPTION) {
                context.reportError(
                    new GraphQLError(
                        'Only subscription operations are permitted over WebSocket; ' +
                            'use HTTP for queries and mutations',
                        { nodes: node },
                    ),
                );
            }
        },
    };
}

/**
 * Rejects a subscription sent over http, which Apollo Server would otherwise attempt to execute,
 * resulting in a confusing null-valued error.
 */
export function createNoSubscriptionOverHttpRule(options: {
    subscriptionsEnabled: boolean;
    apiPath: string;
}): (context: ValidationContext) => ASTVisitor {
    const message = options.subscriptionsEnabled
        ? 'Subscription operations are not supported over HTTP. Connect to the WebSocket endpoint ' +
          `at "/${options.apiPath}" using the "graphql-transport-ws" protocol instead.`
        : 'GraphQL subscriptions are not enabled on this server. They can be enabled by setting ' +
          '`apiOptions.subscriptions.enabled` to `true` in the VendureConfig.';
    return (context: ValidationContext): ASTVisitor => ({
        OperationDefinition(node) {
            if (node.operation === OperationTypeNode.SUBSCRIPTION) {
                context.reportError(new GraphQLError(message, { nodes: node }));
            }
        },
    });
}
