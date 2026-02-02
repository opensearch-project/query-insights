/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.QueryTypeMetrics;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * Class contains all the Counters related to search query types.
 */
public final class SearchQueryCounters {
    private static final String LEVEL_TAG = "level";
    private static final String QUERY_TYPE_TAG = "type";
    private static final String UNIT = "1";
    private static final String UNIT_MILLIS = "ms";
    private static final String UNIT_CPU_CYCLES = "ns";
    private static final String UNIT_BYTES = "bytes";

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
     * Aggregation type to QueryTypeMetrics map for storing queryable metrics
     */
    private final ConcurrentHashMap<String, QueryTypeMetrics> aggregationTypeMetrics;

    /**
     * Constructor
     * @param metricsRegistry opentelemetry metrics registry
     */
    public SearchQueryCounters(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        this.nameToQueryTypeCounters = new ConcurrentHashMap<>();
        this.aggregationTypeMetrics = new ConcurrentHashMap<>();
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
            UNIT_MILLIS
        );
        this.queryTypeCpuHistogram = metricsRegistry.createHistogram(
            "search.query.type.cpu.histogram",
            "Histogram for the cpu per query type",
            UNIT_CPU_CYCLES
        );
        this.queryTypeMemoryHistogram = metricsRegistry.createHistogram(
            "search.query.type.memory.histogram",
            "Histogram for the memory per query type",
            UNIT_BYTES
        );
        this.queryHandlers = new HashMap<>();
    }

    /**
     * Increment counter
     * @param queryBuilder query builder
     * @param level level of query builder, 0 being highest level
     * @param measurements metrics measurements
     */
    public void incrementCounter(QueryBuilder queryBuilder, int level, Map<MetricType, Measurement> measurements) {
        String uniqueQueryCounterName = queryBuilder.getName();

        Counter counter = nameToQueryTypeCounters.computeIfAbsent(uniqueQueryCounterName, k -> createQueryCounter(k));
        counter.add(1, Tags.create().addTag(LEVEL_TAG, level));
        incrementAllHistograms(Tags.create().addTag(LEVEL_TAG, level).addTag(QUERY_TYPE_TAG, uniqueQueryCounterName), measurements);
    }

    /**
     * Increment aggregate counter
     * @param value value to increment
     * @param tags tags
     * @param measurements metrics measurements
     */
    public void incrementAggCounter(double value, Tags tags, Map<MetricType, Measurement> measurements) {
        aggCounter.add(value, tags);
        incrementAllHistograms(tags, measurements);
    }

    /**
     * Increment aggregate counter and store metrics for the aggregation type
     * @param value value to increment
     * @param aggregationType the aggregation type (e.g., "streaming_terms", "terms")
     * @param tags tags for OpenTelemetry
     * @param measurements metrics measurements
     */
    public void incrementAggCounterWithType(double value, String aggregationType, Tags tags, Map<MetricType, Measurement> measurements) {
        aggCounter.add(value, tags);
        incrementAllHistograms(tags, measurements);

        // Store metrics for querying via API
        QueryTypeMetrics metrics = aggregationTypeMetrics.computeIfAbsent(
            aggregationType,
            k -> new QueryTypeMetrics(aggregationType)
        );

        long latency = measurements.containsKey(MetricType.LATENCY)
            ? measurements.get(MetricType.LATENCY).getMeasurement().longValue()
            : 0;
        long cpu = measurements.containsKey(MetricType.CPU)
            ? measurements.get(MetricType.CPU).getMeasurement().longValue()
            : 0;
        long memory = measurements.containsKey(MetricType.MEMORY)
            ? measurements.get(MetricType.MEMORY).getMeasurement().longValue()
            : 0;

        metrics.recordMeasurement(latency, cpu, memory);
    }

    /**
     * Get all aggregation type metrics
     * @return unmodifiable map of aggregation type to QueryTypeMetrics
     */
    public Map<String, QueryTypeMetrics> getAggregationTypeMetrics() {
        return Collections.unmodifiableMap(aggregationTypeMetrics);
    }

    /**
     * Get metrics for a specific aggregation type
     * @param aggregationType the aggregation type
     * @return QueryTypeMetrics or null if not found
     */
    public QueryTypeMetrics getMetricsForAggregationType(String aggregationType) {
        return aggregationTypeMetrics.get(aggregationType);
    }

    /**
     * Increment sort counter
     * @param value value to increment
     * @param tags tags
     * @param measurements metrics measurements
     */
    public void incrementSortCounter(double value, Tags tags, Map<MetricType, Measurement> measurements) {
        sortCounter.add(value, tags);
        incrementAllHistograms(tags, measurements);
    }

    private void incrementAllHistograms(Tags tags, Map<MetricType, Measurement> measurements) {
        if (measurements.containsKey(MetricType.LATENCY)) {
            queryTypeLatencyHistogram.record(measurements.get(MetricType.LATENCY).getMeasurement().doubleValue(), tags);
        }
        if (measurements.containsKey(MetricType.CPU)) {
            queryTypeCpuHistogram.record(measurements.get(MetricType.CPU).getMeasurement().doubleValue(), tags);
        }
        if (measurements.containsKey(MetricType.MEMORY)) {
            queryTypeMemoryHistogram.record(measurements.get(MetricType.MEMORY).getMeasurement().doubleValue(), tags);
        }
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
