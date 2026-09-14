import { ApolloServerPlugin, GraphQLRequestListener, GraphQLServerContext } from '@apollo/server';

import { GraphqlValueTransformer } from '../common/graphql-value-transformer';
import { IdCodecService } from '../common/id-codec.service';
import { encodeIdsInResult } from '../common/result-transformers';

/**
 * Encodes the ids of outgoing responses according to the configured EntityIdStrategy.
 *
 * This is done here and not via a Nest Interceptor because it's not possible
 * according to https://github.com/nestjs/graphql/issues/320
 */
export class IdCodecPlugin implements ApolloServerPlugin {
    private graphqlValueTransformer: GraphqlValueTransformer;
    constructor(private idCodecService: IdCodecService) {}

    async serverWillStart(service: GraphQLServerContext): Promise<void> {
        this.graphqlValueTransformer = new GraphqlValueTransformer(service.schema);
    }

    async requestDidStart(): Promise<GraphQLRequestListener<any>> {
        return {
            willSendResponse: async requestContext => {
                const { document } = requestContext;
                if (document) {
                    const { body } = requestContext.response;
                    if (body.kind === 'single') {
                        encodeIdsInResult(
                            this.graphqlValueTransformer,
                            this.idCodecService,
                            document,
                            body.singleResult.data,
                        );
                    }
                }
            },
        };
    }
}
