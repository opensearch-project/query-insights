/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getExporterSettings;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_GROUPING_TYPE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterFactory;
import org.opensearch.plugin.insights.core.service.categorizer.SearchQueryCategorizer;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

/**
 * Service responsible for gathering, analyzing, storing and exporting
 * information related to search queries
 */
public class QueryInsightsService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(QueryInsightsService.class);

    /**
     * The internal OpenSearch thread pool that execute async processing and exporting tasks
     */
    private final ThreadPool threadPool;

    /**
     * Services to capture top n queries for different metric types
     */
    private final Map<MetricType, TopQueriesService> topQueriesServices;

    /**
     * Flags for enabling insight data collection for different metric types
     */
    private final Map<MetricType, Boolean> enableCollect;

    /**
     * The internal thread-safe queue to ingest the search query data and subsequently forward to processors
     */
    private final LinkedBlockingQueue<SearchQueryRecord> queryRecordsQueue;

    /**
     * Holds a reference to delayed operation {@link Scheduler.Cancellable} so it can be cancelled when
     * the service closed concurrently.
     */
    protected volatile Scheduler.Cancellable scheduledFuture;

    /**
     * Query Insights exporter factory
     */
    final QueryInsightsExporterFactory queryInsightsExporterFactory;

    /**
     * Flags for enabling insight data grouping for different metric types
     */
    private GroupingType groupingType;

    private volatile boolean searchQueryMetricsEnabled;

    private SearchQueryCategorizer searchQueryCategorizer;

    /**
     * Constructor of the QueryInsightsService
     *
     * @param clusterSettings OpenSearch cluster level settings
     * @param threadPool The OpenSearch thread pool to run async tasks
     * @param client OS client
     * @param metricsRegistry Opentelemetry Metrics registry
     */
    @Inject
    public QueryInsightsService(
        final ClusterSettings clusterSettings,
        final ThreadPool threadPool,
        final Client client,
        final MetricsRegistry metricsRegistry
    ) {
        enableCollect = new HashMap<>();
        queryRecordsQueue = new LinkedBlockingQueue<>(QueryInsightsSettings.QUERY_RECORD_QUEUE_CAPACITY);
        this.threadPool = threadPool;
        this.queryInsightsExporterFactory = new QueryInsightsExporterFactory(client);
        // initialize top n queries services and configurations consumers
        topQueriesServices = new HashMap<>();
        for (MetricType metricType : MetricType.allMetricTypes()) {
            enableCollect.put(metricType, false);
            topQueriesServices.put(metricType, new TopQueriesService(metricType, threadPool, queryInsightsExporterFactory));
        }
        for (MetricType type : MetricType.allMetricTypes()) {
            clusterSettings.addSettingsUpdateConsumer(
                getExporterSettings(type),
                (settings -> setExporter(type, settings)),
                (settings -> validateExporterConfig(type, settings))
            );
        }

        this.searchQueryCategorizer = SearchQueryCategorizer.getInstance(metricsRegistry);
        this.enableSearchQueryMetricsFeature(false);
        this.groupingType = DEFAULT_GROUPING_TYPE;
    }

    /**
     * Ingest the query data into in-memory stores
     *
     * @param record the record to ingest
     * @return true/false
     */
    public boolean addRecord(final SearchQueryRecord record) {
        boolean shouldAdd = isSearchQueryMetricsFeatureEnabled() || isGroupingEnabled();
        if (!shouldAdd) {
            for (Map.Entry<MetricType, TopQueriesService> entry : topQueriesServices.entrySet()) {
                if (!enableCollect.get(entry.getKey())) {
                    continue;
                }
                List<SearchQueryRecord> currentSnapshot = entry.getValue().getTopQueriesCurrentSnapshot();
                // skip add to top N queries store if the incoming record is smaller than the Nth record
                if (currentSnapshot.size() < entry.getValue().getTopNSize()
                    || SearchQueryRecord.compare(record, currentSnapshot.get(0), entry.getKey()) > 0) {
                    shouldAdd = true;
                    break;
                }
            }
        }
        if (shouldAdd) {
            return queryRecordsQueue.offer(record);
        }
        return false;
    }

    /**
     * Drain the queryRecordsQueue into internal stores and services
     */
    public void drainRecords() {
        final List<SearchQueryRecord> records = new ArrayList<>();
        queryRecordsQueue.drainTo(records);
        records.sort(Comparator.comparingLong(SearchQueryRecord::getTimestamp));
        for (MetricType metricType : MetricType.allMetricTypes()) {
            if (enableCollect.get(metricType)) {
                // ingest the records into topQueriesService
                topQueriesServices.get(metricType).consumeRecords(records);
            }
        }

        if (searchQueryMetricsEnabled) {
            try {
                searchQueryCategorizer.consumeRecords(records);
            } catch (Exception e) {
                logger.error("Error while trying to categorize the queries.", e);
            }
        }
    }

    /**
     * Get the top queries service based on metricType
     * @param metricType {@link MetricType}
     * @return {@link TopQueriesService}
     */
    public TopQueriesService getTopQueriesService(final MetricType metricType) {
        return topQueriesServices.get(metricType);
    }

    /**
     * Set flag to enable or disable Query Insights data collection
     *
     * @param metricType {@link MetricType}
     * @param enable Flag to enable or disable Query Insights data collection
     */
    public void enableCollection(final MetricType metricType, final boolean enable) {
        this.enableCollect.put(metricType, enable);
        this.topQueriesServices.get(metricType).setEnabled(enable);
    }

    /**
     * Validate grouping
     * @param groupingTypeSetting grouping
     */
    public void validateGrouping(final String groupingTypeSetting) {
        GroupingType.getGroupingTypeFromSettingAndValidate(groupingTypeSetting);
    }

    /**
     * Set grouping
     * @param groupingTypeSetting grouping
     */
    public void setGrouping(final String groupingTypeSetting) {
        GroupingType newGroupingType = GroupingType.getGroupingTypeFromSettingAndValidate(groupingTypeSetting);
        GroupingType oldGroupingType = groupingType;

        if (oldGroupingType != newGroupingType) {
            groupingType = newGroupingType;

            for (MetricType metricType : MetricType.allMetricTypes()) {
                this.topQueriesServices.get(metricType).setGrouping(newGroupingType);
            }
        }
    }

    /**
     * Get the grouping type based on the metricType
     * @return GroupingType
     */

    public GroupingType getGrouping() {
        return groupingType;
    }

    /**
     * Get if the Query Insights data collection is enabled for a MetricType
     *
     * @param metricType {@link MetricType}
     * @return if the Query Insights data collection is enabled
     */
    public boolean isCollectionEnabled(final MetricType metricType) {
        return this.enableCollect.get(metricType);
    }

    /**
     * Check if any feature of Query Insights service is enabled, right now includes Top N and Categorization.
     *
     * @return if query insights service is enabled
     */
    public boolean isAnyFeatureEnabled() {
        return isTopNFeatureEnabled() || isSearchQueryMetricsFeatureEnabled();
    }

    /**
     * Check if top N enabled for any metric type
     *
     * @return if top N feature is enabled
     */
    public boolean isTopNFeatureEnabled() {
        for (MetricType t : MetricType.allMetricTypes()) {
            if (isCollectionEnabled(t)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Is search query metrics feature enabled.
     * @return boolean flag
     */
    public boolean isSearchQueryMetricsFeatureEnabled() {
        return this.searchQueryMetricsEnabled;
    }

    /**
     * Is grouping feature enabled and grouping not NONE
     * @return boolean
     */
    public boolean isGroupingEnabled() {
        return this.groupingType != GroupingType.NONE;
    }

    /**
     * Enable/Disable search query metrics feature.
     * @param enable enable/disable search query metrics feature
     * Stops query insights service if no features enabled
     */
    public void enableSearchQueryMetricsFeature(boolean enable) {
        searchQueryMetricsEnabled = enable;
    }

    /**
     * Validate the window size config for a metricType
     *
     * @param type {@link MetricType}
     * @param windowSize {@link TimeValue}
     */
    public void validateWindowSize(final MetricType type, final TimeValue windowSize) {
        if (topQueriesServices.containsKey(type)) {
            topQueriesServices.get(type).validateWindowSize(windowSize);
        }
    }

    /**
     * Set window size for a metricType
     *
     * @param type {@link MetricType}
     * @param windowSize {@link TimeValue}
     */
    public void setWindowSize(final MetricType type, final TimeValue windowSize) {
        if (topQueriesServices.containsKey(type)) {
            topQueriesServices.get(type).setWindowSize(windowSize);
        }
    }

    /**
     * Validate the top n size config for a metricType
     *
     * @param type {@link MetricType}
     * @param topNSize top n size
     */
    public void validateTopNSize(final MetricType type, final int topNSize) {
        if (topQueriesServices.containsKey(type)) {
            topQueriesServices.get(type).validateTopNSize(topNSize);
        }
    }

    /**
     * Set the top n size config for a metricType
     *
     * @param type {@link MetricType}
     * @param topNSize top n size
     */
    public void setTopNSize(final MetricType type, final int topNSize) {
        if (topQueriesServices.containsKey(type)) {
            topQueriesServices.get(type).setTopNSize(topNSize);
        }
    }

    /**
     * Set the exporter config for a metricType
     *
     * @param type {@link MetricType}
     * @param settings exporter settings
     */
    public void setExporter(final MetricType type, final Settings settings) {
        if (topQueriesServices.containsKey(type)) {
            topQueriesServices.get(type).setExporter(settings);
        }
    }

    /**
     * Get search query categorizer object
     * @return SearchQueryCategorizer object
     */
    public SearchQueryCategorizer getSearchQueryCategorizer() {
        return this.searchQueryCategorizer;
    }

    /**
     * Validate the exporter config for a metricType
     *
     * @param type {@link MetricType}
     * @param settings exporter settings
     */
    public void validateExporterConfig(final MetricType type, final Settings settings) {
        if (topQueriesServices.containsKey(type)) {
            topQueriesServices.get(type).validateExporterConfig(settings);
        }
    }

    @Override
    protected void doStart() {
        if (isAnyFeatureEnabled()) {
            scheduledFuture = threadPool.scheduleWithFixedDelay(
                this::drainRecords,
                QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL,
                QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR
            );
        }
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {
        // close all top n queries service
        for (TopQueriesService topQueriesService : topQueriesServices.values()) {
            topQueriesService.close();
        }
        // close any unclosed resources
        queryInsightsExporterFactory.closeAllExporters();
    }
}
