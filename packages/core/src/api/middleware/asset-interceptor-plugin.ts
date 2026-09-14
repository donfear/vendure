import { ApolloServerPlugin, GraphQLRequestListener, GraphQLServerContext } from '@apollo/server';

import { Instrument } from '../../common/instrument-decorator';
import { AssetStorageStrategy } from '../../config/asset-storage-strategy/asset-storage-strategy';
import { ConfigService } from '../../config/config.service';
import { GraphqlValueTransformer } from '../common/graphql-value-transformer';
import { prefixAssetUrlsInResult } from '../common/result-transformers';

/**
 * Transforms outputs so that any Asset instances are run through the {@link AssetStorageStrategy.toAbsoluteUrl}
 * method before being returned in the response.
 */
@Instrument()
export class AssetInterceptorPlugin implements ApolloServerPlugin {
    private graphqlValueTransformer: GraphqlValueTransformer;
    private readonly toAbsoluteUrl: AssetStorageStrategy['toAbsoluteUrl'] | undefined;

    constructor(private configService: ConfigService) {
        const { assetOptions } = this.configService;
        if (assetOptions.assetStorageStrategy.toAbsoluteUrl) {
            this.toAbsoluteUrl = assetOptions.assetStorageStrategy.toAbsoluteUrl.bind(
                assetOptions.assetStorageStrategy,
            );
        }
    }

    async serverWillStart(service: GraphQLServerContext): Promise<void> {
        this.graphqlValueTransformer = new GraphqlValueTransformer(service.schema);
    }

    async requestDidStart(): Promise<GraphQLRequestListener<any>> {
        return {
            willSendResponse: async requestContext => {
                const { document } = requestContext;
                if (document) {
                    const { body } = requestContext.response;
                    const req = requestContext.contextValue.req;
                    if (body.kind === 'single') {
                        prefixAssetUrlsInResult(
                            this.graphqlValueTransformer,
                            this.toAbsoluteUrl,
                            req,
                            document,
                            body.singleResult.data,
                        );
                    }
                }
            },
        };
    }
}
