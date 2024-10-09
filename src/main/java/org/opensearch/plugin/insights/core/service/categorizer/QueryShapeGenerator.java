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
    private final ConcurrentHashMap<Index, ConcurrentHashMap<String, String>> fieldTypeMap;

    public QueryShapeGenerator(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.fieldTypeMap = new ConcurrentHashMap<>();
    }

    /**
     * Method to get query shape hash code given a source
     * @param source search request source
     * @param showFields whether to include field data in query shape
     * @return Hash code of query shape as a MurmurHash3.Hash128 object (128-bit)
     */
    public MurmurHash3.Hash128 getShapeHashCode(SearchSourceBuilder source, Boolean showFields, Set<Index> successfulSearchShardIndices) {
        final String shape = buildShape(source, showFields, successfulSearchShardIndices);
        final BytesRef shapeBytes = new BytesRef(shape);
        return MurmurHash3.hash128(shapeBytes.bytes, 0, shapeBytes.length, 0, new MurmurHash3.Hash128());
    }

    public String getShapeHashCodeAsString(SearchSourceBuilder source, Boolean showFields, Set<Index> successfulSearchShardIndices) {
        MurmurHash3.Hash128 hashcode = getShapeHashCode(source, showFields, successfulSearchShardIndices);
        String hashAsString = Long.toHexString(hashcode.h1) + Long.toHexString(hashcode.h2);
        return hashAsString;
    }

    /**
     * Method to build search query shape given a source
     * @param source search request source
     * @param showFields whether to append field data
     * @return Search query shape as String
     */
    public String buildShape(SearchSourceBuilder source, Boolean showFields, Set<Index> successfulSearchShardIndices) {
        StringBuilder shape = new StringBuilder();
        shape.append(buildQueryShape(source.query(), showFields, successfulSearchShardIndices));
        shape.append(buildAggregationShape(source.aggregations(), showFields, successfulSearchShardIndices));
        shape.append(buildSortShape(source.sorts(), showFields, successfulSearchShardIndices));
        return shape.toString();
    }

    /**
     * Method to build query-section shape
     * @param queryBuilder search request query builder
     * @param showFields whether to append field data
     * @return Query-section shape as String
     */
    String buildQueryShape(QueryBuilder queryBuilder, Boolean showFields, Set<Index> successfulSearchShardIndices) {
        if (queryBuilder == null) {
            return EMPTY_STRING;
        }
        QueryShapeVisitor shapeVisitor = new QueryShapeVisitor(this, successfulSearchShardIndices);
        queryBuilder.visit(shapeVisitor);
        return shapeVisitor.prettyPrintTree(EMPTY_STRING, showFields);
    }

    /**
     * Method to build aggregation shape
     * @param aggregationsBuilder search request aggregation builder
     * @param showFields whether to append field data
     * @return Aggregation shape as String
     */
    String buildAggregationShape(
        AggregatorFactories.Builder aggregationsBuilder,
        Boolean showFields,
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
            showFields,
            successfulSearchShardIndices
        );
        return aggregationShape.toString();
    }

    StringBuilder recursiveAggregationShapeBuilder(
        Collection<AggregationBuilder> aggregationBuilders,
        Collection<PipelineAggregationBuilder> pipelineAggregations,
        StringBuilder outputBuilder,
        StringBuilder baseIndent,
        Boolean showFields,
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
            if (showFields) {
                stringBuilder.append(buildFieldDataString(aggBuilder, successfulSearchShardIndices));
            }
            stringBuilder.append("\n");

            if (aggBuilder.getSubAggregations().isEmpty() == false) {
                // Recursive call on sub-aggregations
                recursiveAggregationShapeBuilder(
                    aggBuilder.getSubAggregations(),
                    aggBuilder.getPipelineAggregations(),
                    stringBuilder,
                    baseIndent.append(ONE_SPACE_INDENT.repeat(4)),
                    showFields,
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
     * Method to build sort shape
     * @param sortBuilderList search request sort builders list
     * @param showFields whether to append field data
     * @return Sort shape as String
     */
    String buildSortShape(List<SortBuilder<?>> sortBuilderList, Boolean showFields, Set<Index> successfulSearchShardIndices) {
        if (sortBuilderList == null || sortBuilderList.isEmpty()) {
            return EMPTY_STRING;
        }
        StringBuilder sortShape = new StringBuilder();
        sortShape.append("sort:\n");

        List<String> shapeStrings = new ArrayList<>();
        for (SortBuilder<?> sortBuilder : sortBuilderList) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ONE_SPACE_INDENT.repeat(2)).append(sortBuilder.order());
            if (showFields) {
                stringBuilder.append(buildFieldDataString(sortBuilder, successfulSearchShardIndices));
            }
            shapeStrings.add(stringBuilder.toString());
        }

        for (String line : shapeStrings) {
            sortShape.append(line).append("\n");
        }
        return sortShape.toString();
    }

    /**
     * Method to build field data
     * @return String: comma separated list with leading space in square brackets
     * Ex: " [my_field, width:5]"
     */
    String buildFieldDataString(NamedWriteable builder, Set<Index> successfulSearchShardIndices) {
        List<String> fieldDataList = new ArrayList<>();
        if (builder instanceof WithFieldName) {
            String fieldName = ((WithFieldName) builder).fieldName();
            fieldDataList.add(fieldName);
            fieldDataList.add(getFieldType(fieldName, successfulSearchShardIndices));
        }
        return " [" + String.join(", ", fieldDataList) + "]";
    }

    String getFieldType(String fieldName, Set<Index> successfulSearchShardIndices) {
        for (Index index : successfulSearchShardIndices) {
            Map<String, String> indexMap = fieldTypeMap.get(index);
            if (indexMap != null) {
                String fieldType = indexMap.get(fieldName);
                if (fieldType != null) {
                    return fieldType;
                }
            }
        }
        Map<String, MappingMetadata> allIndicesMap;
        try {
            allIndicesMap = clusterService.state()
                .metadata()
                .findMappings(successfulSearchShardIndices.stream().map(Index::getName).toArray(String[]::new), input -> (str -> true));
        } catch (IOException e) {
            return null;
        }
        for (Index index : successfulSearchShardIndices) {
            MappingMetadata mappingMetadata = allIndicesMap.get(index.getName());
            if (mappingMetadata != null) {
                @SuppressWarnings("unchecked")
                Map<String, ?> propertiesAsMap = (Map<String, ?>) mappingMetadata.getSourceAsMap().get("properties");
                String fieldType = MappingVisitor.visitMapping(propertiesAsMap, fieldName.split("\\."), 0);
                if (fieldType != null) {
                    // add item to cache
                    fieldTypeMap.computeIfAbsent(index, k -> new ConcurrentHashMap<>()).put(fieldName, fieldType);
                    return fieldType;
                }
            }
        }
        return null;
    }
}
