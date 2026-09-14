import { isObject } from '@vendure/common/lib/shared-utils';
import { DocumentNode, GraphQLNamedType, isUnionType } from 'graphql';

import { AssetStorageStrategy } from '../../config/asset-storage-strategy/asset-storage-strategy';

import { GraphqlValueTransformer } from './graphql-value-transformer';
import { IdCodecService } from './id-codec.service';

/**
 * These keys are JSON fields which are known to contain entity ids, and which
 * therefore need to be encoded/decoded.
 */
const JSON_ID_KEYS = [
    'paymentId',
    'fulfillmentId',
    'orderItemIds',
    'orderLineId',
    'promotionId',
    'refundId',
    'groupId',
    'modificationId',
    'previousCustomerId',
    'newCustomerId',
];

const ASSET_TYPE_NAMES = ['Asset', 'SearchResultAsset'];

/**
 * Encodes the ids in the given result data according to the configured EntityIdStrategy.
 * The `data` object is mutated in place.
 *
 * This function is shared by the {@link IdCodecPlugin} (http) and the subscriptions
 * transport (websocket), so that both use exactly the same logic.
 */
export function encodeIdsInResult(
    graphqlValueTransformer: GraphqlValueTransformer,
    idCodecService: IdCodecService,
    document: DocumentNode,
    data?: Record<string, unknown> | null,
): void {
    if (!data) {
        return;
    }
    const typeTree = graphqlValueTransformer.getOutputTypeTree(document);
    graphqlValueTransformer.transformValues(typeTree, data, (value, type) => {
        const isIdType = type && type.name === 'ID';
        if (type && type.name === 'JSON' && isObject(value)) {
            return idCodecService.encode(value, JSON_ID_KEYS);
        }
        return isIdType ? idCodecService.encode(value) : value;
    });
}

/**
 * Transforms the result data so that any Asset instances are run through the
 * {@link AssetStorageStrategy.toAbsoluteUrl} method. The `data` object is mutated in place.
 *
 * This function is shared by the {@link AssetInterceptorPlugin} (http) and the subscriptions
 * transport (websocket), so that both use exactly the same logic.
 */
export function prefixAssetUrlsInResult(
    graphqlValueTransformer: GraphqlValueTransformer,
    toAbsoluteUrl: AssetStorageStrategy['toAbsoluteUrl'] | undefined,
    request: any,
    document: DocumentNode,
    data?: Record<string, unknown> | null,
): void {
    if (!toAbsoluteUrl || !data) {
        return;
    }
    const typeTree = graphqlValueTransformer.getOutputTypeTree(document);
    graphqlValueTransformer.transformValues(typeTree, data, (value, type) => {
        if (!type) {
            return value;
        }
        const isAssetType = isAssetTypeName(type);
        const isUnionWithAssetType = isUnionType(type) && type.getTypes().find(t => isAssetTypeName(t));
        if (isAssetType || isUnionWithAssetType) {
            if (value && !Array.isArray(value)) {
                if (value.preview) {
                    value.preview = toAbsoluteUrl(request, value.preview);
                }
                if (value.source) {
                    value.source = toAbsoluteUrl(request, value.source);
                }
            }
        }
        return value;
    });
}

function isAssetTypeName(type: GraphQLNamedType): boolean {
    return ASSET_TYPE_NAMES.includes(type.name);
}
