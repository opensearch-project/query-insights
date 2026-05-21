/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.util.BytesRef;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WithFieldName;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;

/**
 * Class to generate query shape
 */
public class QueryShapeGenerator implements ClusterStateListener {
    static final String EMPTY_STRING = "";
    static final String ONE_SPACE_INDENT = " ";
    private final ClusterService clusterService;
    private final IndicesFieldTypeCache indicesFieldTypeCache;
    private long cacheHitCount;
    private long cacheMissCount;

    private final String NO_FIELD_TYPE_VALUE = "";
    public static final String HIT_COUNT = "hit_count";
    public static final String MISS_COUNT = "miss_count";
    public static final String EVICTIONS = "evictions";
    public static final String ENTRY_COUNT = "entry_count";
    public static final String SIZE_IN_BYTES = "size_in_bytes";

    public QueryShapeGenerator(ClusterService clusterService) {
        this.clusterService = clusterService;
        clusterService.addListener(this);
        this.indicesFieldTypeCache = new IndicesFieldTypeCache(clusterService.getSettings());
        this.cacheHitCount = 0;
        this.cacheMissCount = 0;
    }

    public void clusterChanged(ClusterChangedEvent event) {
        final List<Index> indicesDeleted = event.indicesDeleted();
        for (Index index : indicesDeleted) {
            // remove the deleted index mapping from field type cache
            indicesFieldTypeCache.invalidate(index);
        }

        if (event.metadataChanged()) {
            final Metadata previousMetadata = event.previousState().metadata();
            final Metadata currentMetadata = event.state().metadata();
            for (Index index : indicesFieldTypeCache.keySet()) {
                if (previousMetadata.index(index) != currentMetadata.index(index)) {
                    // remove the updated index mapping from field type cache
                    indicesFieldTypeCache.invalidate(index);
                }
            }
        }
    }

    /**
     * Gets the hash code of the query shape given a search source.
     *
     * @param source                search request source
     * @param showFieldName         whether to include field names in the query shape
     * @param showFieldType         whether to include field types in the query shape
     * @param successfulSearchShardIndices the set of indices that were successfully searched
     * @return Hash code of the query shape as a MurmurHash3.Hash128 object (128-bit)
     */
    public MurmurHash3.Hash128 getShapeHashCode(
        SearchSourceBuilder source,
        Boolean showFieldName,
        Boolean showFieldType,
        Set<Index> successfulSearchShardIndices
    ) {
        final String shape = buildShape(source, showFieldName, showFieldType, successfulSearchShardIndices);
        final BytesRef shapeBytes = new BytesRef(shape);
        return MurmurHash3.hash128(shapeBytes.bytes, 0, shapeBytes.length, 0, new MurmurHash3.Hash128());
    }

    /**
     * Gets the hash code of the query shape as a string.
     *
     * @param source                search request source
     * @param showFieldName         whether to include field names in the query shape
     * @param showFieldType         whether to include field types in the query shape
     * @param successfulSearchShardIndices the set of indices that were successfully searched
     * @return Hash code of the query shape as a string
     */
    public String getShapeHashCodeAsString(
        SearchSourceBuilder source,
        Boolean showFieldName,
        Boolean showFieldType,
        Set<Index> successfulSearchShardIndices
    ) {
        MurmurHash3.Hash128 hashcode = getShapeHashCode(source, showFieldName, showFieldType, successfulSearchShardIndices);
        String hashAsString = Long.toHexString(hashcode.h1) + Long.toHexString(hashcode.h2);
        return hashAsString;
    }

    /**
     * Gets the hash code of the query shape as a string.
     *
     * @param queryShape            query shape as input
     * @return Hash code of the query shape as a string
     */
    public String getShapeHashCodeAsString(String queryShape) {
        final BytesRef shapeBytes = new BytesRef(queryShape);
        MurmurHash3.Hash128 hashcode = MurmurHash3.hash128(shapeBytes.bytes, 0, shapeBytes.length, 0, new MurmurHash3.Hash128());
        return Long.toHexString(hashcode.h1) + Long.toHexString(hashcode.h2);
    }

    /**
     * Builds the search query shape given a source.
     *
     * @param source                search request source
     * @param showFieldName         whether to append field names
     * @param showFieldType         whether to append field types
     * @param successfulSearchShardIndices the set of indices that were successfully searched
     * @return Search query shape as a String
     */
    public String buildShape(
        SearchSourceBuilder source,
        Boolean showFieldName,
        Boolean showFieldType,
        Set<Index> successfulSearchShardIndices
    ) {
        Index firstIndex = null;
        Map<String, Object> propertiesAsMap = null;
        if (successfulSearchShardIndices != null) {
            firstIndex = successfulSearchShardIndices.iterator().next();
            propertiesAsMap = getPropertiesMapForIndex(firstIndex);
        }

        StringBuilder shape = new StringBuilder();
        shape.append(buildQueryShape(source.query(), showFieldName, showFieldType, propertiesAsMap, firstIndex));
        shape.append(buildAggregationShape(source.aggregations(), showFieldName, showFieldType, propertiesAsMap, firstIndex));
        shape.append(buildSortShape(source.sorts(), showFieldName, showFieldType, propertiesAsMap, firstIndex));
        return shape.toString();
    }

    private Map<String, Object> getPropertiesMapForIndex(Index index) {
        IndexMetadata indexMetadata = clusterService.state().metadata().index(index);
        if (indexMetadata == null || indexMetadata.mapping() == null) {
            return Collections.emptyMap();
        }

        Map<String, Object> sourceAsMap = indexMetadata.mapping().getSourceAsMap();
        if (sourceAsMap == null) {
            return Collections.emptyMap();
        }

        Map<String, Object> propertiesMap = (Map<String, Object>) sourceAsMap.get("properties");
        if (propertiesMap == null) {
            return Collections.emptyMap();
        }
        return propertiesMap;
    }

    /**
     * Builds the query-section shape.
     *
     * @param queryBuilder          search request query builder
     * @param showFieldName         whether to append field names
     * @param showFieldType         whether to append field types
     * @param propertiesAsMap       properties
     * @param index                 index
     * @return Query-section shape as a String
     */
    String buildQueryShape(
        QueryBuilder queryBuilder,
        Boolean showFieldName,
        Boolean showFieldType,
        Map<String, Object> propertiesAsMap,
        Index index
    ) {
        if (queryBuilder == null) {
            return EMPTY_STRING;
        }
        QueryShapeVisitor shapeVisitor = new QueryShapeVisitor(this, propertiesAsMap, index, showFieldName, showFieldType);
        queryBuilder.visit(shapeVisitor);
        return shapeVisitor.prettyPrintTree(EMPTY_STRING, showFieldName, showFieldType);
    }

    /**
     * Builds the aggregation shape.
     *
     * @param aggregationsBuilder    search request aggregation builder
     * @param showFieldName          whether to append field names
     * @param showFieldType          whether to append field types
     * @param propertiesAsMap       properties
     * @param index                 index
     * @return Aggregation shape as a String
     */
    String buildAggregationShape(
        AggregatorFactories.Builder aggregationsBuilder,
        Boolean showFieldName,
        Boolean showFieldType,
        Map<String, Object> propertiesAsMap,
        Index index
    ) {
        if (aggregationsBuilder == null) {
            return EMPTY_STRING;
        }
        StringBuilder aggregationShape = recursiveAggregationShapeBuilder(
            aggregationsBuilder.getAggregatorFactories(),
            aggregationsBuilder.getPipelineAggregatorFactories(),
            new StringBuilder(),
            new StringBuilder(),
            showFieldName,
            showFieldType,
            propertiesAsMap,
            index
        );
        return aggregationShape.toString();
    }

    StringBuilder recursiveAggregationShapeBuilder(
        Collection<AggregationBuilder> aggregationBuilders,
        Collection<PipelineAggregationBuilder> pipelineAggregations,
        StringBuilder outputBuilder,
        StringBuilder baseIndent,
        Boolean showFieldName,
        Boolean showFieldType,
        Map<String, Object> propertiesAsMap,
        Index index
    ) {
        //// Normal Aggregations ////
        if (aggregationBuilders.isEmpty() == false) {
            outputBuilder.append(baseIndent).append("aggregation:").append("\n");
        }
        List<String> aggShapeStrings = new ArrayList<>();
        for (AggregationBuilder aggBuilder : aggregationBuilders) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(baseIndent).append(ONE_SPACE_INDENT.repeat(2)).append(aggBuilder.getType());
            if (showFieldName || showFieldType) {
                stringBuilder.append(buildFieldDataString(aggBuilder, propertiesAsMap, index, showFieldName, showFieldType));
            }
            stringBuilder.append("\n");

            if (aggBuilder.getSubAggregations().isEmpty() == false) {
                // Recursive call on sub-aggregations
                recursiveAggregationShapeBuilder(
                    aggBuilder.getSubAggregations(),
                    aggBuilder.getPipelineAggregations(),
                    stringBuilder,
                    baseIndent.append(ONE_SPACE_INDENT.repeat(4)),
                    showFieldName,
                    showFieldType,
                    propertiesAsMap,
                    index
                );
                baseIndent.delete(0, 4);
            }
            aggShapeStrings.add(stringBuilder.toString());
        }

        // Sort alphanumerically and append aggregations list
        Collections.sort(aggShapeStrings);
        for (String shapeString : aggShapeStrings) {
            outputBuilder.append(shapeString);
        }

        //// Pipeline Aggregation (cannot have sub-aggregations) ////
        if (pipelineAggregations.isEmpty() == false) {
            outputBuilder.append(baseIndent).append(ONE_SPACE_INDENT.repeat(2)).append("pipeline aggregation:").append("\n");

            List<String> pipelineAggShapeStrings = new ArrayList<>();
            for (PipelineAggregationBuilder pipelineAgg : pipelineAggregations) {
                pipelineAggShapeStrings.add(
                    new StringBuilder().append(baseIndent)
                        .append(ONE_SPACE_INDENT.repeat(4))
                        .append(pipelineAgg.getType())
                        .append("\n")
                        .toString()
                );
            }

            // Sort alphanumerically and append pipeline aggregations list
            Collections.sort(pipelineAggShapeStrings);
            for (String shapeString : pipelineAggShapeStrings) {
                outputBuilder.append(shapeString);
            }
        }
        return outputBuilder;
    }

    /**
     * Builds the sort shape.
     *
     * @param sortBuilderList        search request sort builders list
     * @param showFieldName          whether to append field names
     * @param showFieldType          whether to append field types
     * @param propertiesAsMap       properties
     * @param index                 index
     * @return Sort shape as a String
     */
    String buildSortShape(
        List<SortBuilder<?>> sortBuilderList,
        Boolean showFieldName,
        Boolean showFieldType,
        Map<String, Object> propertiesAsMap,
        Index index
    ) {
        if (sortBuilderList == null || sortBuilderList.isEmpty()) {
            return EMPTY_STRING;
        }
        StringBuilder sortShape = new StringBuilder();
        sortShape.append("sort:\n");

        List<String> shapeStrings = new ArrayList<>();
        for (SortBuilder<?> sortBuilder : sortBuilderList) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ONE_SPACE_INDENT.repeat(2)).append(sortBuilder.order());
            if (showFieldName || showFieldType) {
                stringBuilder.append(buildFieldDataString(sortBuilder, propertiesAsMap, index, showFieldName, showFieldType));
            }
            shapeStrings.add(stringBuilder.toString());
        }

        for (String line : shapeStrings) {
            sortShape.append(line).append("\n");
        }
        return sortShape.toString();
    }

    /**
     * Builds a field data string from a builder.
     *
     * @param builder                  aggregation or sort builder
     * @param propertiesAsMap       properties
     * @param index                 index
     * @param showFieldName            whether to include field names
     * @param showFieldType            whether to include field types
     * @return Field data string
     * Ex: " [my_field, width:5]"
     */
    String buildFieldDataString(
        NamedWriteable builder,
        Map<String, Object> propertiesAsMap,
        Index index,
        Boolean showFieldName,
        Boolean showFieldType
    ) {
        List<String> fieldDataList = new ArrayList<>();
        if (builder instanceof WithFieldName) {
            String fieldName = ((WithFieldName) builder).fieldName();
            if (showFieldName) {
                fieldDataList.add(fieldName);
            }
            if (showFieldType) {
                String fieldType = getFieldType(fieldName, propertiesAsMap, index);
                if (fieldType != null && !fieldType.isEmpty()) {
                    fieldDataList.add(fieldType);
                }
            }
        }
        return " [" + String.join(", ", fieldDataList) + "]";
    }

    public String getFieldType(String fieldName, Map<String, Object> propertiesAsMap, Index index) {
        if (propertiesAsMap == null || index == null) {
            return null;
        }
        // Attempt to get field type from cache
        String fieldType = getFieldTypeFromCache(fieldName, index);

        if (fieldType != null) {
            cacheHitCount += 1;
            return fieldType;
        } else {
            cacheMissCount += 1;
        }

        // Retrieve field type from mapping and cache it if found
        fieldType = getFieldTypeFromProperties(fieldName, propertiesAsMap);

        // Cache field type or NO_FIELD_TYPE_VALUE if not found
        fieldType = fieldType != null ? fieldType : NO_FIELD_TYPE_VALUE;
        if (indicesFieldTypeCache.getOrInitialize(index).putIfAbsent(fieldName, fieldType)) {
            indicesFieldTypeCache.incrementCountAndWeight(fieldName, fieldType);
        }

        return fieldType;
    }

    String getFieldTypeFromProperties(String fieldName, Map<String, Object> propertiesAsMap) {
        if (propertiesAsMap == null) {
            return null;
        }

        String[] fieldParts = fieldName.split("\\.");
        Map<String, ?> currentProperties = propertiesAsMap;

        for (int depth = 0; depth < fieldParts.length; depth++) {
            Object currentMapping = currentProperties.get(fieldParts[depth]);

            if (currentMapping instanceof Map) {
                Map<String, ?> currentMap = (Map<String, ?>) currentMapping;

                // Navigate into nested properties if available
                if (currentMap.containsKey("properties")) {
                    currentProperties = (Map<String, ?>) currentMap.get("properties");
                }
                // Handle multifields (e.g., title.raw)
                else if (currentMap.containsKey("fields") && depth + 1 < fieldParts.length) {
                    currentProperties = (Map<String, ?>) currentMap.get("fields");
                }
                // Return type if found
                else if (currentMap.containsKey("type")) {
                    return (String) currentMap.get("type");
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }

        return null;
    }

    String getFieldTypeFromCache(String fieldName, Index index) {
        return indicesFieldTypeCache.getOrInitialize(index).get(fieldName);
    }

    /**
     * Get field type cache stats
     *
     * @return Map containing cache hit count, miss count, and byte stats
     */
    public Map<String, Long> getFieldTypeCacheStats() {
        return Map.of(
            SIZE_IN_BYTES,
            indicesFieldTypeCache.getWeight(),
            ENTRY_COUNT,
            indicesFieldTypeCache.getEntryCount(),
            EVICTIONS,
            indicesFieldTypeCache.getEvictionCount(),
            HIT_COUNT,
            cacheHitCount,
            MISS_COUNT,
            cacheMissCount
        );
    }
}
