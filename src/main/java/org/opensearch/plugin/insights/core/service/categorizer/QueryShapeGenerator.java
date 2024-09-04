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
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.index.query.AbstractGeometryQueryBuilder;
import org.opensearch.index.query.CommonTermsQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.FieldMaskingSpanQueryBuilder;
import org.opensearch.index.query.FuzzyQueryBuilder;
import org.opensearch.index.query.GeoBoundingBoxQueryBuilder;
import org.opensearch.index.query.GeoDistanceQueryBuilder;
import org.opensearch.index.query.GeoPolygonQueryBuilder;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiTermQueryBuilder;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.SpanNearQueryBuilder;
import org.opensearch.index.query.SpanTermQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.VariableWidthHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.AbstractRangeBuilder;
import org.opensearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.RareTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;

/**
 * Class to generate query shape
 */
public class QueryShapeGenerator {
    static final String EMPTY_STRING = "";
    static final String ONE_SPACE_INDENT = " ";
    static final Map<Class<?>, List<Function<Object, String>>> QUERY_FIELD_DATA_MAP = FieldDataMapHelper.getQueryFieldDataMap();
    static final Map<Class<?>, List<Function<Object, String>>> AGG_FIELD_DATA_MAP = FieldDataMapHelper.getAggFieldDataMap();
    static final Map<Class<?>, List<Function<Object, String>>> SORT_FIELD_DATA_MAP = FieldDataMapHelper.getSortFieldDataMap();

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
                stringBuilder.append(buildFieldDataString(AGG_FIELD_DATA_MAP.get(aggBuilder.getClass()), aggBuilder));
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
                stringBuilder.append(buildFieldDataString(SORT_FIELD_DATA_MAP.get(sortBuilder.getClass()), sortBuilder));
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
    static String buildFieldDataString(List<Function<Object, String>> methods, NamedWriteable builder) {
        List<String> fieldDataList = new ArrayList<>();
        if (methods != null) {
            for (Function<Object, String> lambda : methods) {
                fieldDataList.add(lambda.apply(builder));
            }
        }
        return " [" + String.join(", ", fieldDataList) + "]";
    }

    /**
     * Helper class to create static field data maps
     */
    private static class FieldDataMapHelper {

        // Helper method to create map entries
        private static <T> Map.Entry<Class<?>, List<Function<Object, String>>> createEntry(Class<T> clazz, Function<T, String> extractor) {
            return Map.entry(clazz, List.of(obj -> extractor.apply(clazz.cast(obj))));
        }

        /**
         * Returns a map where the keys are query builders, and the values are lists of
         * functions that extract field values from instances of these classes.
         *
         * @return a map with class types as keys and lists of field extraction functions as values.
         */
        private static Map<Class<?>, List<Function<Object, String>>> getQueryFieldDataMap() {
            return Map.ofEntries(
                createEntry(AbstractGeometryQueryBuilder.class, AbstractGeometryQueryBuilder::fieldName),
                createEntry(CommonTermsQueryBuilder.class, CommonTermsQueryBuilder::fieldName),
                createEntry(ExistsQueryBuilder.class, ExistsQueryBuilder::fieldName),
                createEntry(FieldMaskingSpanQueryBuilder.class, FieldMaskingSpanQueryBuilder::fieldName),
                createEntry(FuzzyQueryBuilder.class, FuzzyQueryBuilder::fieldName),
                createEntry(GeoBoundingBoxQueryBuilder.class, GeoBoundingBoxQueryBuilder::fieldName),
                createEntry(GeoDistanceQueryBuilder.class, GeoDistanceQueryBuilder::fieldName),
                createEntry(GeoPolygonQueryBuilder.class, GeoPolygonQueryBuilder::fieldName),
                createEntry(MatchBoolPrefixQueryBuilder.class, MatchBoolPrefixQueryBuilder::fieldName),
                createEntry(MatchQueryBuilder.class, MatchQueryBuilder::fieldName),
                createEntry(MatchPhraseQueryBuilder.class, MatchPhraseQueryBuilder::fieldName),
                createEntry(MatchPhrasePrefixQueryBuilder.class, MatchPhrasePrefixQueryBuilder::fieldName),
                createEntry(MultiTermQueryBuilder.class, MultiTermQueryBuilder::fieldName),
                createEntry(PrefixQueryBuilder.class, PrefixQueryBuilder::fieldName),
                createEntry(RangeQueryBuilder.class, RangeQueryBuilder::fieldName),
                createEntry(RegexpQueryBuilder.class, RegexpQueryBuilder::fieldName),
                createEntry(SpanNearQueryBuilder.SpanGapQueryBuilder.class, SpanNearQueryBuilder.SpanGapQueryBuilder::fieldName),
                createEntry(SpanTermQueryBuilder.class, SpanTermQueryBuilder::fieldName),
                createEntry(TermQueryBuilder.class, TermQueryBuilder::fieldName),
                createEntry(TermsQueryBuilder.class, TermsQueryBuilder::fieldName),
                createEntry(WildcardQueryBuilder.class, WildcardQueryBuilder::fieldName)
            );
        }

        /**
         * Returns a map where the keys are aggregation builders, and the values are lists of
         * functions that extract field values from instances of these classes.
         *
         * @return a map with class types as keys and lists of field extraction functions as values.
         */
        private static Map<Class<?>, List<Function<Object, String>>> getAggFieldDataMap() {
            return Map.ofEntries(
                createEntry(IpRangeAggregationBuilder.class, IpRangeAggregationBuilder::field),
                createEntry(AutoDateHistogramAggregationBuilder.class, AutoDateHistogramAggregationBuilder::field),
                createEntry(DateHistogramAggregationBuilder.class, DateHistogramAggregationBuilder::field),
                createEntry(HistogramAggregationBuilder.class, HistogramAggregationBuilder::field),
                createEntry(VariableWidthHistogramAggregationBuilder.class, VariableWidthHistogramAggregationBuilder::field),
                createEntry(MissingAggregationBuilder.class, MissingAggregationBuilder::field),
                createEntry(AbstractRangeBuilder.class, AbstractRangeBuilder::field),
                createEntry(GeoDistanceAggregationBuilder.class, GeoDistanceAggregationBuilder::field),
                createEntry(DiversifiedAggregationBuilder.class, DiversifiedAggregationBuilder::field),
                createEntry(RareTermsAggregationBuilder.class, RareTermsAggregationBuilder::field),
                createEntry(SignificantTermsAggregationBuilder.class, SignificantTermsAggregationBuilder::field),
                createEntry(TermsAggregationBuilder.class, TermsAggregationBuilder::field),
                createEntry(AvgAggregationBuilder.class, AvgAggregationBuilder::field),
                createEntry(CardinalityAggregationBuilder.class, CardinalityAggregationBuilder::field),
                createEntry(ExtendedStatsAggregationBuilder.class, ExtendedStatsAggregationBuilder::field),
                createEntry(GeoCentroidAggregationBuilder.class, GeoCentroidAggregationBuilder::field),
                createEntry(MaxAggregationBuilder.class, MaxAggregationBuilder::field),
                createEntry(MinAggregationBuilder.class, MinAggregationBuilder::field),
                createEntry(StatsAggregationBuilder.class, StatsAggregationBuilder::field),
                createEntry(SumAggregationBuilder.class, SumAggregationBuilder::field),
                createEntry(ValueCountAggregationBuilder.class, ValueCountAggregationBuilder::field),
                createEntry(ValuesSourceAggregationBuilder.class, ValuesSourceAggregationBuilder::field)
            );
        }

        /**
         * Returns a map where the keys are sort builders, and the values are lists of
         * functions that extract field values from instances of these classes.
         *
         * @return a map with class types as keys and lists of field extraction functions as values.
         */
        private static Map<Class<?>, List<Function<Object, String>>> getSortFieldDataMap() {
            return Map.ofEntries(createEntry(FieldSortBuilder.class, FieldSortBuilder::getFieldName));
        }
    }
}
