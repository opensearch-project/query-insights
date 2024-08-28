/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizor;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator;
import org.opensearch.search.aggregations.bucket.terms.SignificantTextAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

public final class QueryShapeGeneratorTests extends OpenSearchTestCase {
    public void testComplexSearch() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        // build query
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("field1", "value2");
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("field2", "php");
        RegexpQueryBuilder regexpQueryBuilder = new RegexpQueryBuilder("field3", "text");
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("field4");
        sourceBuilder.query(
            new BoolQueryBuilder().must(termQueryBuilder).filter(matchQueryBuilder).should(regexpQueryBuilder).filter(rangeQueryBuilder)
        );
        // build agg
        sourceBuilder.aggregation(
            new TermsAggregationBuilder("agg1").userValueTypeHint(ValueType.STRING)
                .field("type")
                .subAggregation(new DerivativePipelineAggregationBuilder("pipeline-agg1", "bucket1"))
                .subAggregation(new TermsAggregationBuilder("child-agg3").userValueTypeHint(ValueType.STRING).field("key.sub3"))
        );
        sourceBuilder.aggregation(new TermsAggregationBuilder("agg2").userValueTypeHint(ValueType.STRING).field("model"));
        sourceBuilder.aggregation(
            new TermsAggregationBuilder("agg3").userValueTypeHint(ValueType.STRING)
                .field("key")
                .subAggregation(new MaxBucketPipelineAggregationBuilder("pipeline-agg2", "bucket2"))
                .subAggregation(new TermsAggregationBuilder("child-agg1").userValueTypeHint(ValueType.STRING).field("key.sub1"))
                .subAggregation(new TermsAggregationBuilder("child-agg2").userValueTypeHint(ValueType.STRING).field("key.sub2"))
        );
        sourceBuilder.aggregation(new TopHitsAggregationBuilder("top_hits").storedField("_none_"));
        sourceBuilder.aggregation(new SignificantTextAggregationBuilder("sig_text", "agg4").filterDuplicateText(true));
        sourceBuilder.aggregation(new MaxBucketPipelineAggregationBuilder("pipeline-agg4", "bucket4"));
        sourceBuilder.aggregation(new DerivativePipelineAggregationBuilder("pipeline-agg3", "bucket3"));
        sourceBuilder.aggregation(new AvgBucketPipelineAggregationBuilder("pipeline-agg5", "bucket5"));
        // build sort
        sourceBuilder.sort("color", SortOrder.DESC);
        sourceBuilder.sort("vendor", SortOrder.DESC);
        sourceBuilder.sort("price", SortOrder.ASC);
        sourceBuilder.sort("album", SortOrder.ASC);

        String shapeShowFieldsTrue = QueryShapeGenerator.buildShape(sourceBuilder, true);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [field1]\n"
            + "  filter:\n"
            + "    match [field2]\n"
            + "    range [field4]\n"
            + "  should:\n"
            + "    regexp [field3]\n"
            + "aggregation:\n"
            + "  significant_text []\n"
            + "  terms [key]\n"
            + "    aggregation:\n"
            + "      terms [key.sub1]\n"
            + "      terms [key.sub2]\n"
            + "      pipeline aggregation:\n"
            + "        max_bucket\n"
            + "  terms [model]\n"
            + "  terms [type]\n"
            + "    aggregation:\n"
            + "      terms [key.sub3]\n"
            + "      pipeline aggregation:\n"
            + "        derivative\n"
            + "  top_hits []\n"
            + "  pipeline aggregation:\n"
            + "    avg_bucket\n"
            + "    derivative\n"
            + "    max_bucket\n"
            + "sort:\n"
            + "  asc [album]\n"
            + "  asc [price]\n"
            + "  desc [color]\n"
            + "  desc [vendor]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = QueryShapeGenerator.buildShape(sourceBuilder, false);
        String expectedShowFieldsFalse = "bool\n"
            + "  must:\n"
            + "    term\n"
            + "  filter:\n"
            + "    match\n"
            + "    range\n"
            + "  should:\n"
            + "    regexp\n"
            + "aggregation:\n"
            + "  significant_text\n"
            + "  terms\n"
            + "  terms\n"
            + "    aggregation:\n"
            + "      terms\n"
            + "      pipeline aggregation:\n"
            + "        derivative\n"
            + "  terms\n"
            + "    aggregation:\n"
            + "      terms\n"
            + "      terms\n"
            + "      pipeline aggregation:\n"
            + "        max_bucket\n"
            + "  top_hits\n"
            + "  pipeline aggregation:\n"
            + "    avg_bucket\n"
            + "    derivative\n"
            + "    max_bucket\n"
            + "sort:\n"
            + "  asc\n"
            + "  asc\n"
            + "  desc\n"
            + "  desc\n";
        assertEquals(expectedShowFieldsFalse, shapeShowFieldsFalse);
    }

    public void testQueryShape() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("field1", "value2");
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("field2", "php");
        RegexpQueryBuilder regexpQueryBuilder = new RegexpQueryBuilder("field3", "text");
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("field4");
        sourceBuilder.query(
            new BoolQueryBuilder().must(termQueryBuilder).filter(matchQueryBuilder).should(regexpQueryBuilder).filter(rangeQueryBuilder)
        );

        String shapeShowFieldsTrue = QueryShapeGenerator.buildShape(sourceBuilder, true);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [field1]\n"
            + "  filter:\n"
            + "    match [field2]\n"
            + "    range [field4]\n"
            + "  should:\n"
            + "    regexp [field3]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = QueryShapeGenerator.buildShape(sourceBuilder, false);
        String expectedShowFieldsFalse = "bool\n"
            + "  must:\n"
            + "    term\n"
            + "  filter:\n"
            + "    match\n"
            + "    range\n"
            + "  should:\n"
            + "    regexp\n";
        assertEquals(expectedShowFieldsFalse, shapeShowFieldsFalse);
    }

    public void testAggregationShape() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(
            new TermsAggregationBuilder("agg1").userValueTypeHint(ValueType.STRING)
                .field("type")
                .subAggregation(new DerivativePipelineAggregationBuilder("pipeline-agg1", "bucket1"))
                .subAggregation(new TermsAggregationBuilder("child-agg3").userValueTypeHint(ValueType.STRING).field("key.sub3"))
        );
        sourceBuilder.aggregation(new TermsAggregationBuilder("agg2").userValueTypeHint(ValueType.STRING).field("model"));
        sourceBuilder.aggregation(
            new TermsAggregationBuilder("agg3").userValueTypeHint(ValueType.STRING)
                .field("key")
                .subAggregation(new MaxBucketPipelineAggregationBuilder("pipeline-agg2", "bucket2"))
                .subAggregation(new TermsAggregationBuilder("child-agg1").userValueTypeHint(ValueType.STRING).field("key.sub1"))
                .subAggregation(new TermsAggregationBuilder("child-agg2").userValueTypeHint(ValueType.STRING).field("key.sub2"))
        );
        sourceBuilder.aggregation(new TopHitsAggregationBuilder("top_hits").storedField("_none_"));
        sourceBuilder.aggregation(new SignificantTextAggregationBuilder("sig_text", "agg4").filterDuplicateText(true));
        sourceBuilder.aggregation(new MaxBucketPipelineAggregationBuilder("pipeline-agg4", "bucket4"));
        sourceBuilder.aggregation(new DerivativePipelineAggregationBuilder("pipeline-agg3", "bucket3"));
        sourceBuilder.aggregation(new AvgBucketPipelineAggregationBuilder("pipeline-agg5", "bucket5"));

        String shapeShowFieldsTrue = QueryShapeGenerator.buildShape(sourceBuilder, true);
        String expectedShowFieldsTrue = "aggregation:\n"
            + "  significant_text []\n"
            + "  terms [key]\n"
            + "    aggregation:\n"
            + "      terms [key.sub1]\n"
            + "      terms [key.sub2]\n"
            + "      pipeline aggregation:\n"
            + "        max_bucket\n"
            + "  terms [model]\n"
            + "  terms [type]\n"
            + "    aggregation:\n"
            + "      terms [key.sub3]\n"
            + "      pipeline aggregation:\n"
            + "        derivative\n"
            + "  top_hits []\n"
            + "  pipeline aggregation:\n"
            + "    avg_bucket\n"
            + "    derivative\n"
            + "    max_bucket\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = QueryShapeGenerator.buildShape(sourceBuilder, false);
        String expectedShowFieldsFalse = "aggregation:\n"
            + "  significant_text\n"
            + "  terms\n"
            + "  terms\n"
            + "    aggregation:\n"
            + "      terms\n"
            + "      pipeline aggregation:\n"
            + "        derivative\n"
            + "  terms\n"
            + "    aggregation:\n"
            + "      terms\n"
            + "      terms\n"
            + "      pipeline aggregation:\n"
            + "        max_bucket\n"
            + "  top_hits\n"
            + "  pipeline aggregation:\n"
            + "    avg_bucket\n"
            + "    derivative\n"
            + "    max_bucket\n";
        assertEquals(expectedShowFieldsFalse, shapeShowFieldsFalse);
    }

    public void testSortShape() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.sort("color", SortOrder.DESC);
        sourceBuilder.sort("vendor", SortOrder.DESC);
        sourceBuilder.sort("price", SortOrder.ASC);
        sourceBuilder.sort("album", SortOrder.ASC);

        String shapeShowFieldsTrue = QueryShapeGenerator.buildShape(sourceBuilder, true);
        String expectedShowFieldsTrue = "sort:\n" + "  asc [album]\n" + "  asc [price]\n" + "  desc [color]\n" + "  desc [vendor]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = QueryShapeGenerator.buildShape(sourceBuilder, false);
        String expectedShowFieldsFalse = "sort:\n" + "  asc\n" + "  asc\n" + "  desc\n" + "  desc\n";
        assertEquals(expectedShowFieldsFalse, shapeShowFieldsFalse);
    }
}
