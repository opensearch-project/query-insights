/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import java.util.Collection;
import java.util.Map;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * Increments the counters related to Aggregation Search Queries.
 */
public class SearchQueryAggregationCategorizer {

    static final String AGGREGATION_TYPE_TAG = "agg_type";
    private final SearchQueryCounters searchQueryCounters;

    /**
     * Constructor for SearchQueryAggregationCategorizer
     * @param searchQueryCounters Object containing all query counters
     */
    public SearchQueryAggregationCategorizer(SearchQueryCounters searchQueryCounters) {
        this.searchQueryCounters = searchQueryCounters;
    }

    /**
     * Increment aggregation related counters
     *
     * @param aggregatorFactories input aggregations
     * @param measurements latency, cpu, memory measurements
     * @param isStreaming whether the query is a streaming request
     */
    public void incrementSearchQueryAggregationCounters(
        Collection<AggregationBuilder> aggregatorFactories,
        Map<MetricType, Measurement> measurements,
        boolean isStreaming
    ) {
        for (AggregationBuilder aggregationBuilder : aggregatorFactories) {
            incrementCountersRecursively(aggregationBuilder, measurements, isStreaming);
        }
    }

    private void incrementCountersRecursively(
        AggregationBuilder aggregationBuilder,
        Map<MetricType, Measurement> measurements,
        boolean isStreaming
    ) {
        // Increment counters for the current aggregation
        String aggregationType = aggregationBuilder.getType();
        searchQueryCounters.incrementAggCounter(1, Tags.create().addTag(AGGREGATION_TYPE_TAG, aggregationType), measurements, isStreaming);

        // Recursively process sub-aggregations if any
        Collection<AggregationBuilder> subAggregations = aggregationBuilder.getSubAggregations();
        if (subAggregations != null && !subAggregations.isEmpty()) {
            for (AggregationBuilder subAggregation : subAggregations) {
                incrementCountersRecursively(subAggregation, measurements, isStreaming);
            }
        }

        // Process pipeline aggregations
        Collection<PipelineAggregationBuilder> pipelineAggregations = aggregationBuilder.getPipelineAggregations();
        for (PipelineAggregationBuilder pipelineAggregation : pipelineAggregations) {
            String pipelineAggregationType = pipelineAggregation.getType();
            searchQueryCounters.incrementAggCounter(
                1,
                Tags.create().addTag(AGGREGATION_TYPE_TAG, pipelineAggregationType),
                measurements,
                isStreaming
            );
        }
    }
}
