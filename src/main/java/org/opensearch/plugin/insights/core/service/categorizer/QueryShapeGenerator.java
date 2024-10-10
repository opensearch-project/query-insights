/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.util.BytesRef;
import org.opensearch.cluster.metadata.MappingMetadata;
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
public class QueryShapeGenerator {
    static final String EMPTY_STRING = "";
    static final String ONE_SPACE_INDENT = " ";
    private final ClusterService clusterService;
    private final String NO_FIELD_TYPE_VALUE = "";
    private final ConcurrentHashMap<Index, ConcurrentHashMap<String, String>> fieldTypeMap;

    public QueryShapeGenerator(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.fieldTypeMap = new ConcurrentHashMap<>();
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
        StringBuilder shape = new StringBuilder();
        shape.append(buildQueryShape(source.query(), showFieldName, showFieldType, successfulSearchShardIndices));
        shape.append(buildAggregationShape(source.aggregations(), showFieldName, showFieldType, successfulSearchShardIndices));
        shape.append(buildSortShape(source.sorts(), showFieldName, showFieldType, successfulSearchShardIndices));
        return shape.toString();
    }

    /**
     * Builds the query-section shape.
     *
     * @param queryBuilder          search request query builder
     * @param showFieldName         whether to append field names
     * @param showFieldType         whether to append field types
     * @param successfulSearchShardIndices the set of indices that were successfully searched
     * @return Query-section shape as a String
     */
    String buildQueryShape(
        QueryBuilder queryBuilder,
        Boolean showFieldName,
        Boolean showFieldType,
        Set<Index> successfulSearchShardIndices
    ) {
        if (queryBuilder == null) {
            return EMPTY_STRING;
        }
        QueryShapeVisitor shapeVisitor = new QueryShapeVisitor(this, successfulSearchShardIndices, showFieldName, showFieldType);
        queryBuilder.visit(shapeVisitor);
        return shapeVisitor.prettyPrintTree(EMPTY_STRING, showFieldName, showFieldType);
    }

    /**
     * Builds the aggregation shape.
     *
     * @param aggregationsBuilder    search request aggregation builder
     * @param showFieldName          whether to append field names
     * @param showFieldType          whether to append field types
     * @param successfulSearchShardIndices the set of indices that were successfully searched
     * @return Aggregation shape as a String
     */
    String buildAggregationShape(
        AggregatorFactories.Builder aggregationsBuilder,
        Boolean showFieldName,
        Boolean showFieldType,
        Set<Index> successfulSearchShardIndices
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
            successfulSearchShardIndices
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
        Set<Index> successfulSearchShardIndices
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
                stringBuilder.append(buildFieldDataString(aggBuilder, successfulSearchShardIndices, showFieldName, showFieldType));
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
                    successfulSearchShardIndices
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
     * @param successfulSearchShardIndices the set of indices that were successfully searched
     * @return Sort shape as a String
     */
    String buildSortShape(
        List<SortBuilder<?>> sortBuilderList,
        Boolean showFieldName,
        Boolean showFieldType,
        Set<Index> successfulSearchShardIndices
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
                stringBuilder.append(buildFieldDataString(sortBuilder, successfulSearchShardIndices, showFieldName, showFieldType));
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
     * @param successfulSearchShardIndices the set of indices that were successfully searched
     * @param showFieldName            whether to include field names
     * @param showFieldType            whether to include field types
     * @return Field data string
     * Ex: " [my_field, width:5]"
     */
    String buildFieldDataString(
        NamedWriteable builder,
        Set<Index> successfulSearchShardIndices,
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
                String fieldType = getFieldType(fieldName, successfulSearchShardIndices);
                if (fieldType != null && !fieldType.isEmpty()) {
                    fieldDataList.add(fieldType);
                }
            }
        }
        return " [" + String.join(", ", fieldDataList) + "]";
    }

    String getFieldType(String fieldName, Set<Index> successfulSearchShardIndices) {
        if (successfulSearchShardIndices == null) {
            return null;
        }
        String fieldType = getFieldTypeFromCache(fieldName, successfulSearchShardIndices);
        if (fieldType != null) {
            if (fieldType.equals(NO_FIELD_TYPE_VALUE)) {
                return null;
            } else {
                return fieldType;
            }
        }

        for (Index index : successfulSearchShardIndices) {
            Map<String, MappingMetadata> indexMapping = null;
            try {
                indexMapping = clusterService.state().metadata().findMappings(new String[] { index.getName() }, input -> str -> true);
            } catch (IOException e) {
                // Skip the current index
                continue;
            }
            MappingMetadata mappingMetadata = indexMapping.get(index.getName());
            fieldType = getFieldTypeFromMapping(index, fieldName, mappingMetadata);
            if (fieldType != null) {
                String finalFieldType = fieldType;
                fieldTypeMap.computeIfAbsent(index, k -> new ConcurrentHashMap<>()).computeIfAbsent(fieldName, k -> finalFieldType);
                return fieldType;
            }
        }
        return null;
    }

    String getFieldTypeFromMapping(Index index, String fieldName, MappingMetadata mappingMetadata) {
        // Get properties directly from the mapping metadata
        Map<String, ?> propertiesAsMap = null;
        if (mappingMetadata != null) {
            propertiesAsMap = (Map<String, ?>) mappingMetadata.getSourceAsMap().get("properties");
        }

        // Recursively find the field type from properties
        if (propertiesAsMap != null) {
            String fieldType = findFieldTypeInProperties(propertiesAsMap, fieldName.split("\\."), 0);

            // Cache the NO_FIELD_TYPE_VALUE result if no field type is found in this index
            if (fieldType == null) {
                fieldTypeMap.computeIfAbsent(index, k -> new ConcurrentHashMap<>()).computeIfAbsent(fieldName, k -> fieldType);
            }
            return fieldType;
        }
        return null;
    }

    private String findFieldTypeInProperties(Map<String, ?> properties, String[] fieldParts, int index) {
        if (index >= fieldParts.length) {
            return null;
        }

        String currentField = fieldParts[index];
        Object currentMapping = properties.get(currentField);

        // Check if current mapping is a map and contains further nested properties or a type
        if (currentMapping instanceof Map) {
            Map<String, ?> currentMappingMap = (Map<String, ?>) currentMapping;

            // If it has a 'properties' key, go into the nested object
            if (currentMappingMap.containsKey("properties")) {
                Map<String, ?> nestedProperties = (Map<String, ?>) currentMappingMap.get("properties");
                return findFieldTypeInProperties(nestedProperties, fieldParts, index + 1);
            }

            // If it has a 'fields' key, handle multifields (e.g., title.raw) and is not the parent field (parent field case handled below)
            if (currentMappingMap.containsKey("fields") && index + 1 < fieldParts.length) {
                Map<String, ?> fieldsMap = (Map<String, ?>) currentMappingMap.get("fields");
                return findFieldTypeInProperties(fieldsMap, fieldParts, index + 1); // Recursively check subfield
            }

            // If it has a 'type' key, return the type. Also handles parent field case in multifields
            if (currentMappingMap.containsKey("type")) {
                return currentMappingMap.get("type").toString();
            }
        }

        return null;
    }

    String getFieldTypeFromCache(String fieldName, Set<Index> successfulSearchShardIndices) {
        for (Index index : successfulSearchShardIndices) {
            Map<String, String> indexMap = fieldTypeMap.get(index);
            if (indexMap != null) {
                String fieldType = indexMap.get(fieldName);
                if (fieldType != null) {
                    return fieldType;
                }
            }
        }
        return null;
    }
}
