/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.bucket.terms.SignificantTextAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;

public class SearchSourceBuilderUtils {

    public static SearchSourceBuilder createDefaultSearchSourceBuilder() {
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
        // build aggregation
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

        return sourceBuilder;
    }

    public static SearchSourceBuilder createQuerySearchSourceBuilder() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("field1", "value2");
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("field2", "php");
        RegexpQueryBuilder regexpQueryBuilder = new RegexpQueryBuilder("field3", "text");
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("field4");
        sourceBuilder.query(
            new BoolQueryBuilder().must(termQueryBuilder).filter(matchQueryBuilder).should(regexpQueryBuilder).filter(rangeQueryBuilder)
        );
        return sourceBuilder;
    }

    public static SearchSourceBuilder createAggregationSearchSourceBuilder() {
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

        return sourceBuilder;
    }

    public static SearchSourceBuilder createSortSearchSourceBuilder() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.sort("color", SortOrder.DESC);
        sourceBuilder.sort("vendor", SortOrder.DESC);
        sourceBuilder.sort("price", SortOrder.ASC);
        sourceBuilder.sort("album", SortOrder.ASC);
        return sourceBuilder;
    }
}
