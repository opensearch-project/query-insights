/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.metrics;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * Class contains all the Counters related to search query types.
 */
public final class OperationalMetricsCounter {
    private static final String PREFIX = "search.insights.";
    private static final String CLUSTER_NAME_TAG = "cluster_name";
    private static final String UNIT = "1";

    private final String clusterName;
    private final MetricsRegistry metricsRegistry;
    private final ConcurrentHashMap<OperationalMetric, Counter> metricCounterMap;

    private static OperationalMetricsCounter instance;

    /**
     * Constructor of OperationalMetricsCounter
     * @param metricsRegistry the OTel metrics registry
     */
    private OperationalMetricsCounter(String clusterName, MetricsRegistry metricsRegistry) {
        this.clusterName = clusterName;
        this.metricsRegistry = metricsRegistry;
        this.metricCounterMap = new ConcurrentHashMap<>();
        Stream.of(OperationalMetric.values()).forEach(name -> metricCounterMap.computeIfAbsent(name, this::createMetricCounter));
    }

    /**
     * Initializes the singleton instance of OperationalMetricsCounter.
     * This method must be called once before accessing the instance.
     *
     * @param clusterName the name of the cluster
     * @param metricsRegistry the OTel metrics registry
     */
    public static synchronized void initialize(String clusterName, MetricsRegistry metricsRegistry) {
        instance = new OperationalMetricsCounter(clusterName, metricsRegistry);
    }

    /**
     * Get the singleton instance of OperationalMetricsCounter.
     *
     * @return the singleton instance
     * @throws IllegalStateException if the instance is not yet initialized
     */
    public static synchronized OperationalMetricsCounter getInstance() {
        if (instance == null) {
            throw new IllegalStateException("OperationalMetricsCounter is not initialized. Call initialize() first.");
        }
        return instance;
    }

    /**
     * Increment the operational metrics counter, attaching custom tags
     *
     * @param metricName name of the metric
     * @param customTags custom tags of this metric
     */
    public void incrementCounter(OperationalMetric metricName, Tags customTags) {
        Counter counter = metricCounterMap.computeIfAbsent(metricName, this::createMetricCounter);
        Tags metricsTags = (customTags == null ? Tags.create() : customTags).addTag(CLUSTER_NAME_TAG, clusterName);
        counter.add(1, metricsTags);
    }

    /**
     * Increment the operational metrics counter
     *
     * @param metricName name of the metric
     */
    public void incrementCounter(OperationalMetric metricName) {
        this.incrementCounter(metricName, null);
    }

    private Counter createMetricCounter(OperationalMetric metricName) {
        return metricsRegistry.createCounter(
            PREFIX + metricName.toString().toLowerCase(Locale.ROOT) + ".count",
            metricName.getDescription(),
            UNIT
        );
    }
}
