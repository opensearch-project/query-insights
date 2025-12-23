/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import static org.opensearch.plugin.insights.core.service.TopQueriesService.TOP_QUERIES_EXPORTER_ID;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.TOP_QUERIES_READER_ID;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_GROUPING_TYPE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_TOP_N_QUERIES_INDEX_PATTERN;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_EXPORTER_DELETE_AFTER;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_EXPORTER_TYPE;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporter;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterFactory;
import org.opensearch.plugin.insights.core.exporter.SinkType;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReader;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReaderFactory;
import org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator;
import org.opensearch.plugin.insights.core.service.categorizer.SearchQueryCategorizer;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.healthStats.QueryInsightsHealthStats;
import org.opensearch.plugin.insights.rules.model.healthStats.TopQueriesHealthStats;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * Service responsible for gathering, analyzing, storing and exporting
 * information related to search queries
 */
public class QueryInsightsService extends AbstractLifecycleComponent {
    public static final String QUERY_INSIGHTS_INDEX_TAG_NAME = "query_insights_feature_space";

    private static final Logger logger = LogManager.getLogger(QueryInsightsService.class);

    private final ClusterService clusterService;

    /**
     * The internal OpenSearch thread pool that execute async processing and exporting tasks
     */
    private final ThreadPool threadPool;

    /**
     * Map of {@link MetricType} to associated {@link TopQueriesService}
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
     * List of references to delayed operations {@link Scheduler.Cancellable} so they can be cancelled when
     * the service closed concurrently.
     */
    protected volatile List<Scheduler.Cancellable> scheduledFutures;

    /**
     * Reference to the Query Insights local index deletion {@link ScheduledFuture}
     */
    protected volatile ScheduledFuture deleteIndicesScheduledFuture;

    /**
     * Factory for validating and creating exporters
     */
    final QueryInsightsExporterFactory queryInsightsExporterFactory;

    /**
     * Factory for validating and creating readers
     */
    final QueryInsightsReaderFactory queryInsightsReaderFactory;

    /**
     * Flags for enabling insight data grouping for different metric types
     */
    private GroupingType groupingType;

    private volatile boolean searchQueryMetricsEnabled;

    private final SearchQueryCategorizer searchQueryCategorizer;

    private final NamedXContentRegistry namedXContentRegistry;

    /**
     * Query shape generator instance
     */
    private QueryShapeGenerator queryShapeGenerator;

    private LocalIndexLifecycleManager localIndexLifecycleManager;

    SinkType sinkType;

    /**
     * Constructor of the QueryInsightsService
     *
     * @param clusterService OpenSearch cluster service
     * @param threadPool The OpenSearch thread pool to run async tasks
     * @param client OS client
     * @param metricsRegistry Opentelemetry Metrics registry
     * @param namedXContentRegistry NamedXContentRegistry for parsing purposes
     */
    @Inject
    public QueryInsightsService(
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final Client client,
        final MetricsRegistry metricsRegistry,
        final NamedXContentRegistry namedXContentRegistry,
        final QueryInsightsExporterFactory queryInsightsExporterFactory,
        final QueryInsightsReaderFactory queryInsightsReaderFactory
    ) {
        this.clusterService = clusterService;
        enableCollect = new HashMap<>();
        queryRecordsQueue = new LinkedBlockingQueue<>(QueryInsightsSettings.QUERY_RECORD_QUEUE_CAPACITY);
        this.threadPool = threadPool;
        this.queryInsightsExporterFactory = queryInsightsExporterFactory;
        this.queryInsightsReaderFactory = queryInsightsReaderFactory;
        this.namedXContentRegistry = namedXContentRegistry;
        // initialize top n queries services and configurations consumers
        this.topQueriesServices = new HashMap<>();
        for (MetricType metricType : MetricType.allMetricTypes()) {
            enableCollect.put(metricType, false);
            topQueriesServices.put(
                metricType,
                new TopQueriesService(client, metricType, threadPool, this.queryInsightsExporterFactory, this.queryInsightsReaderFactory)
            );
        }
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_EXPORTER_TYPE,
                (v -> setExporterAndReaderType(SinkType.parse(v))),
                (this::validateExporterType)
            );
        this.localIndexLifecycleManager = new LocalIndexLifecycleManager(
            threadPool,
            client,
            clusterService.getClusterSettings().get(TOP_N_EXPORTER_DELETE_AFTER)
        );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_EXPORTER_DELETE_AFTER,
                (localIndexLifecycleManager::setDeleteAfterAndDelete),
                (localIndexLifecycleManager::validateDeleteAfter)
            );

        this.setExporterAndReaderType(SinkType.parse(clusterService.getClusterSettings().get(TOP_N_EXPORTER_TYPE)));
        this.searchQueryCategorizer = SearchQueryCategorizer.getInstance(metricsRegistry, namedXContentRegistry);
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
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.QUERY_CATEGORIZE_EXCEPTIONS);
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

        // Clear the queue when all features are disabled to prevent stale records from being processed
        // when features are re-enabled
        if (!enable && !isAnyFeatureEnabled()) {
            queryRecordsQueue.clear();
        }
    }

    /**
     * Validate grouping given grouping type setting
     * @param groupingTypeSetting grouping setting
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
     * Set max number of groups
     * @param maxGroups maximum number of groups that should be tracked when calculating Top N groups
     */
    public void setMaximumGroups(final int maxGroups) {
        for (MetricType metricType : MetricType.allMetricTypes()) {
            this.topQueriesServices.get(metricType).setMaxGroups(maxGroups);
        }
    }

    /**
     * Validate max number of groups. Should be between 1 and MAX_GROUPS_LIMIT
     * @param maxGroups maximum number of groups that should be tracked when calculating Top N groups
     */
    public void validateMaximumGroups(final int maxGroups) {
        if (maxGroups < 0 || maxGroups > QueryInsightsSettings.MAX_GROUPS_EXCLUDING_TOPN_LIMIT) {
            throw new IllegalArgumentException(
                "Max groups setting"
                    + " should be between 0 and "
                    + QueryInsightsSettings.MAX_GROUPS_EXCLUDING_TOPN_LIMIT
                    + ", was ("
                    + maxGroups
                    + ")"
            );
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
     * Is grouping feature enabled and TopN feature enabled
     * @return boolean
     */
    public boolean isGroupingEnabled() {
        return this.groupingType != GroupingType.NONE && isTopNFeatureEnabled();
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
     * Set the exporter and reader type config for a metricType
     *
     * @param sinkType {@link SinkType}
     */
    public void setExporterAndReaderType(final SinkType sinkType) {
        // Configure the exporter for TopQueriesService in QueryInsightsService
        final QueryInsightsExporter currentExporter = queryInsightsExporterFactory.getExporter(TOP_QUERIES_EXPORTER_ID);
        final QueryInsightsReader currentReader = queryInsightsReaderFactory.getReader(TOP_QUERIES_READER_ID);

        // Close the current exporter and reader
        if (currentExporter != null) {
            try {
                queryInsightsExporterFactory.closeExporter(currentExporter);
            } catch (IOException e) {
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.EXPORTER_FAIL_TO_CLOSE_EXCEPTION);
                logger.error("Fail to close the current exporter when updating exporter and reader, error: ", e);
            }
        }
        if (currentReader != null) {
            try {
                queryInsightsReaderFactory.closeReader(currentReader);
            } catch (IOException e) {
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.READER_FAIL_TO_CLOSE_EXCEPTION);
                logger.error("Fail to close the current reader when updating exporter and reader, error: ", e);
            }
        }
        // Set sink type to local index for TopQueriesServices
        if (sinkType == SinkType.LOCAL_INDEX) {
            queryInsightsExporterFactory.createLocalIndexExporter(
                TOP_QUERIES_EXPORTER_ID,
                DEFAULT_TOP_N_QUERIES_INDEX_PATTERN,
                "mappings/top-queries-record.json"
            );
            // Set up reader for TopQueriesService
            queryInsightsReaderFactory.createLocalIndexReader(
                TOP_QUERIES_READER_ID,
                DEFAULT_TOP_N_QUERIES_INDEX_PATTERN,
                namedXContentRegistry
            );
        }
        // Set sink type to debug exporter
        else if (sinkType == SinkType.DEBUG) {
            queryInsightsExporterFactory.createDebugExporter(TOP_QUERIES_EXPORTER_ID);
        }

        this.sinkType = sinkType;
    }

    /**
     * Get search query categorizer object
     * @return SearchQueryCategorizer object
     */
    public SearchQueryCategorizer getSearchQueryCategorizer() {
        return this.searchQueryCategorizer;
    }

    /**
     * Validate the exporter type config
     *
     * @param exporterType exporter type
     */
    public void validateExporterType(final String exporterType) {
        queryInsightsExporterFactory.validateExporterType(exporterType);
    }

    /**
     * Calculates the duration until the next UTC midnight (00:00:00).
     * <p>
     * This method computes the time difference between the given {@code Instant} and the start of the next UTC day
     * at midnight (00:00:00). It returns the duration as a long in milliseconds.
     *
     * @param now The current time as an {@link Instant}.
     * @return A long representing the time delay (in milliseconds) until the next UTC midnight.
     */
    static long getInitialDelay(final Instant now) {
        // Calculate the start of the next UTC day (00:00:00)
        final Instant startOfNextDay = now.truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.DAYS);
        return Duration.between(now, startOfNextDay).toMillis();
    }

    @Override
    protected void doStart() {
        if (isAnyFeatureEnabled()) {
            scheduledFutures = new ArrayList<>();
            scheduledFutures.add(
                threadPool.scheduleWithFixedDelay(
                    this::drainRecords,
                    QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL,
                    QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR
                )
            );
        }
        if (threadPool.scheduler() != null) {
            deleteIndicesScheduledFuture = threadPool.scheduler().scheduleWithFixedDelay(() -> {
                try {
                    if (clusterService.isStateInitialised()
                        && clusterService.state().getNodes() != null
                        && clusterService.state().getNodes().isLocalNodeElectedClusterManager()) {
                        localIndexLifecycleManager.deleteExpiredTopNIndices();
                    }
                } catch (Exception e) {
                    logger.error("Error occurred while deleting expired indices:", e);
                }
            }, getInitialDelay(Instant.now()) + Duration.ofMinutes(5).toMillis(), Duration.ofDays(1).toMillis(), TimeUnit.MILLISECONDS);
        } else {
            logger.error("Unable to schedule Query Insights delete job. threadPool.scheduler() is null");
        }
    }

    @Override
    protected void doStop() {
        if (scheduledFutures != null) {
            for (Scheduler.Cancellable cancellable : scheduledFutures) {
                if (cancellable != null) {
                    cancellable.cancel();
                }
            }
        }
        FutureUtils.cancel(deleteIndicesScheduledFuture);
    }

    @Override
    protected void doClose() throws IOException {
        // close all top n queries service
        for (TopQueriesService topQueriesService : topQueriesServices.values()) {
            topQueriesService.close();
        }
        // close any unclosed resources
        queryInsightsExporterFactory.closeAllExporters();
        queryInsightsReaderFactory.closeAllReaders();
    }

    /**
     * Get health stats for query insights services
     *
     * @return QueryInsightsHealthStats
     */
    public QueryInsightsHealthStats getHealthStats() {
        Map<MetricType, TopQueriesHealthStats> topQueriesHealthStatsMap = topQueriesServices.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getHealthStats()));
        Map<String, Long> fieldTypeCacheStats = Optional.ofNullable(queryShapeGenerator)
            .map(QueryShapeGenerator::getFieldTypeCacheStats)
            .orElse(Collections.emptyMap());
        return new QueryInsightsHealthStats(
            threadPool.info(QUERY_INSIGHTS_EXECUTOR),
            this.queryRecordsQueue.size(),
            topQueriesHealthStatsMap,
            fieldTypeCacheStats
        );
    }

    /**
     * Set query shape generator
     */
    public void setQueryShapeGenerator(final QueryShapeGenerator queryShapeGenerator) {
        this.queryShapeGenerator = queryShapeGenerator;
    }

    /**
     * Sets the {@link LocalIndexLifecycleManager} instance.
     * <p>
     * <strong>Note:</strong> This method is intended for testing purposes only.
     * Production code should rely on the instance created and managed internally
     * by the constructor.
     *
     * @param localIndexLifecycleManager the mock or test instance to use
     */
    void setLocalIndexLifecycleManager(final LocalIndexLifecycleManager localIndexLifecycleManager) {
        this.localIndexLifecycleManager = localIndexLifecycleManager;
    }

    /**
     * Gets the {@link LocalIndexLifecycleManager} instance.
     */
    LocalIndexLifecycleManager getLocalIndexLifecycleManager() {
        return localIndexLifecycleManager;
    }
}
