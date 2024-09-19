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
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.core.common.io.stream.NamedWriteable;
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

    /**
     * Method to get query shape hash code given a source
     * @param source search request source
     * @param showFields whether to include field data in query shape
     * @return Hash code of query shape as a MurmurHash3.Hash128 object (128-bit)
     */
    public static MurmurHash3.Hash128 getShapeHashCode(SearchSourceBuilder source, Boolean showFields) {
        final String shape = buildShape(source, showFields);
        final BytesRef shapeBytes = new BytesRef(shape);
        return MurmurHash3.hash128(shapeBytes.bytes, 0, shapeBytes.length, 0, new MurmurHash3.Hash128());
    }

    public static String getShapeHashCodeAsString(SearchSourceBuilder source, Boolean showFields) {
        MurmurHash3.Hash128 hashcode = getShapeHashCode(source, showFields);
        String hashAsString = Long.toHexString(hashcode.h1) + Long.toHexString(hashcode.h2);
        return hashAsString;
    }

    /**
     * Method to build search query shape given a source
     * @param source search request source
     * @param showFields whether to append field data
     * @return Search query shape as String
     */
    public static String buildShape(SearchSourceBuilder source, Boolean showFields) {
        StringBuilder shape = new StringBuilder();
        shape.append(buildQueryShape(source.query(), showFields));
        shape.append(buildAggregationShape(source.aggregations(), showFields));
        shape.append(buildSortShape(source.sorts(), showFields));
        return shape.toString();
    }

    /**
     * Method to build query-section shape
     * @param queryBuilder search request query builder
     * @param showFields whether to append field data
     * @return Query-section shape as String
     */
    static String buildQueryShape(QueryBuilder queryBuilder, Boolean showFields) {
        if (queryBuilder == null) {
            return EMPTY_STRING;
        }
        QueryShapeVisitor shapeVisitor = new QueryShapeVisitor();
        queryBuilder.visit(shapeVisitor);
        return shapeVisitor.prettyPrintTree(EMPTY_STRING, showFields);
    }

    /**
     * Method to build aggregation shape
     * @param aggregationsBuilder search request aggregation builder
     * @param showFields whether to append field data
     * @return Aggregation shape as String
     */
    static String buildAggregationShape(AggregatorFactories.Builder aggregationsBuilder, Boolean showFields) {
        if (aggregationsBuilder == null) {
            return EMPTY_STRING;
        }
        StringBuilder aggregationShape = recursiveAggregationShapeBuilder(
            aggregationsBuilder.getAggregatorFactories(),
            aggregationsBuilder.getPipelineAggregatorFactories(),
            new StringBuilder(),
            new StringBuilder(),
            showFields
        );
        return aggregationShape.toString();
    }

    static StringBuilder recursiveAggregationShapeBuilder(
        Collection<AggregationBuilder> aggregationBuilders,
        Collection<PipelineAggregationBuilder> pipelineAggregations,
        StringBuilder outputBuilder,
        StringBuilder baseIndent,
        Boolean showFields
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
                stringBuilder.append(buildFieldDataString(aggBuilder));
            }
            stringBuilder.append("\n");

            if (aggBuilder.getSubAggregations().isEmpty() == false) {
                // Recursive call on sub-aggregations
                recursiveAggregationShapeBuilder(
                    aggBuilder.getSubAggregations(),
                    aggBuilder.getPipelineAggregations(),
                    stringBuilder,
                    baseIndent.append(ONE_SPACE_INDENT.repeat(4)),
                    showFields
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
    static String buildSortShape(List<SortBuilder<?>> sortBuilderList, Boolean showFields) {
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
                stringBuilder.append(buildFieldDataString(sortBuilder));
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
    static String buildFieldDataString(NamedWriteable builder) {
        List<String> fieldDataList = new ArrayList<>();
        if (builder instanceof WithFieldName) {
            fieldDataList.add(((WithFieldName) builder).fieldName());
        }
        return " [" + String.join(", ", fieldDataList) + "]";
    }
}
