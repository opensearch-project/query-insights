/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * Class contains all the Counters related to search query types.
 */
public final class SearchQueryCounters {
    private static final String LEVEL_TAG = "level";
    private static final String TYPE_TAG = "type";
    private static final String UNIT = "1";
    private final MetricsRegistry metricsRegistry;
    /**
     * Aggregation counter
     */
    private final Counter aggCounter;
    /**
     * Counter for all other query types (catch all)
     */
    private final Counter otherQueryCounter;
    /**
     * Counter for sort
     */
    private final Counter sortCounter;

    /**
     * Histogram for latency per query type
     */
    private final Histogram queryTypeLatencyHistogram;
    /**
     * Histogram for cpu per query type
     */
    private final Histogram queryTypeCpuHistogram;
    /**
     * Histogram for memory per query type
     */
    private final Histogram queryTypeMemoryHistogram;

    private final Map<Class<? extends QueryBuilder>, Counter> queryHandlers;
    /**
     * Counter name to Counter object map
     */
    private final ConcurrentHashMap<String, Counter> nameToQueryTypeCounters;

    /**
     * Constructor
     * @param metricsRegistry opentelemetry metrics registry
     */
    public SearchQueryCounters(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        this.nameToQueryTypeCounters = new ConcurrentHashMap<>();
        this.aggCounter = metricsRegistry.createCounter(
            "search.query.type.agg.count",
            "Counter for the number of top level agg search queries",
            UNIT
        );
        this.otherQueryCounter = metricsRegistry.createCounter(
            "search.query.type.other.count",
            "Counter for the number of top level and nested search queries that do not match any other categories",
            UNIT
        );
        this.sortCounter = metricsRegistry.createCounter(
            "search.query.type.sort.count",
            "Counter for the number of top level sort search queries",
            UNIT
        );
        this.queryTypeLatencyHistogram = metricsRegistry.createHistogram(
            "search.query.type.latency.histogram",
            "Histogram for the latency per query type",
            UNIT
        );
        this.queryTypeCpuHistogram = metricsRegistry.createHistogram(
            "search.query.type.cpu.histogram",
            "Histogram for the cpu per query type",
            UNIT
        );
        this.queryTypeMemoryHistogram = metricsRegistry.createHistogram(
            "search.query.type.memory.histogram",
            "Histogram for the memory per query type",
            UNIT
        );
        this.queryHandlers = new HashMap<>();
    }

    /**
     * Increment counter
     * @param queryBuilder query builder
     * @param level level of query builder, 0 being highest level
     * @param measurements metrics measurements
     */
    public void incrementCounter(QueryBuilder queryBuilder, int level, Map<MetricType, Number> measurements) {
        String uniqueQueryCounterName = queryBuilder.getName();

        Counter counter = nameToQueryTypeCounters.computeIfAbsent(uniqueQueryCounterName, k -> createQueryCounter(k));
        counter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        incrementAllHistograms(Tags.create().addTag(LEVEL_TAG, level).addTag(TYPE_TAG, uniqueQueryCounterName), measurements);
    }

    /**
     * Increment aggregate counter
     * @param value value to increment
     * @param tags tags
     * @param measurements metrics measurements
     */
    public void incrementAggCounter(double value, Tags tags, Map<MetricType, Number> measurements) {
        aggCounter.add(value, tags);
        incrementAllHistograms(tags, measurements);
    }

    /**
     * Increment sort counter
     * @param value value to increment
     * @param tags tags
     * @param measurements metrics measurements
     */
    public void incrementSortCounter(double value, Tags tags, Map<MetricType, Number> measurements) {
        sortCounter.add(value, tags);
        incrementAllHistograms(tags, measurements);
    }

    private void incrementAllHistograms(Tags tags, Map<MetricType, Number> measurements) {
        queryTypeLatencyHistogram.record(measurements.get(MetricType.LATENCY).doubleValue(), tags);
        queryTypeCpuHistogram.record(measurements.get(MetricType.CPU).doubleValue(), tags);
        queryTypeMemoryHistogram.record(measurements.get(MetricType.MEMORY).doubleValue(), tags);
    }

    /**
     * Get aggregation counter
     * @return aggregation counter
     */
    public Counter getAggCounter() {
        return aggCounter;
    }

    /**
     * Get sort counter
     * @return sort counter
     */
    public Counter getSortCounter() {
        return sortCounter;
    }

    /**
     * Get counter based on the query builder name
     * @param queryBuilderName query builder name
     * @return counter
     */
    public Counter getCounterByQueryBuilderName(String queryBuilderName) {
        return nameToQueryTypeCounters.get(queryBuilderName);
    }

    private Counter createQueryCounter(String counterName) {
        Counter counter = metricsRegistry.createCounter(
            "search.query.type." + counterName + ".count",
            "Counter for the number of top level and nested " + counterName + " search queries",
            UNIT
        );
        return counter;
    }
}
