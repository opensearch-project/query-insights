/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_TOP_N_QUERIES_INDEX_PATTERN;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_TOP_QUERIES_EXPORTER_TYPE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.EXPORTER_TYPE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.MAX_DELETE_AFTER_VALUE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.MIN_DELETE_AFTER_VALUE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.core.exporter.LocalIndexExporter;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporter;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterFactory;
import org.opensearch.plugin.insights.core.exporter.SinkType;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReader;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReaderFactory;
import org.opensearch.plugin.insights.core.service.grouper.MinMaxHeapQueryGrouper;
import org.opensearch.plugin.insights.core.service.grouper.QueryGrouper;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.healthStats.TopQueriesHealthStats;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.threadpool.ThreadPool;

/**
 * Service responsible for gathering and storing top N queries
 * with high latency or resource usage
 */
public class TopQueriesService {
    private static final String METRIC_TYPE_TAG = "metric_type";
    private static final String GROUPBY_TAG = "groupby";

    /**
     * Logger of the local index exporter
     */
    private final Logger logger = LogManager.getLogger();
    private boolean enabled;
    private final Client client;
    /**
     * The metric type to measure top n queries
     */
    private final MetricType metricType;
    private int topNSize;
    /**
     * The window size to keep the top n queries
     */
    private TimeValue windowSize;
    /**
     * The current window start timestamp
     */
    private long windowStart;
    /**
     * The internal thread-safe store that holds the top n queries insight data
     */
    private final PriorityBlockingQueue<SearchQueryRecord> topQueriesStore;

    /**
     * The AtomicReference of a snapshot of the current window top queries for getters to consume
     */
    private final AtomicReference<List<SearchQueryRecord>> topQueriesCurrentSnapshot;

    /**
     * The AtomicReference of a snapshot of the last window top queries for getters to consume
     */
    private final AtomicReference<List<SearchQueryRecord>> topQueriesHistorySnapshot;

    /**
     * Factory for validating and creating exporters
     */
    private final QueryInsightsExporterFactory queryInsightsExporterFactory;

    /**
     * Factory for validating and creating readers
     */
    private final QueryInsightsReaderFactory queryInsightsReaderFactory;

    /**
     * The internal OpenSearch thread pool that execute async processing and exporting tasks
     */
    private final ThreadPool threadPool;

    /**
     * Exporter for exporting top queries data
     */
    private QueryInsightsExporter exporter;
    private QueryInsightsReader reader;

    private final QueryGrouper queryGrouper;

    TopQueriesService(
        final Client client,
        final MetricType metricType,
        final ThreadPool threadPool,
        final QueryInsightsExporterFactory queryInsightsExporterFactory,
        QueryInsightsReaderFactory queryInsightsReaderFactory
    ) {
        this.enabled = false;
        this.client = client;
        this.metricType = metricType;
        this.threadPool = threadPool;
        this.queryInsightsExporterFactory = queryInsightsExporterFactory;
        this.queryInsightsReaderFactory = queryInsightsReaderFactory;
        this.topNSize = QueryInsightsSettings.DEFAULT_TOP_N_SIZE;
        this.windowSize = QueryInsightsSettings.DEFAULT_WINDOW_SIZE;
        this.windowStart = -1L;
        this.exporter = null;
        this.reader = null;
        topQueriesStore = new PriorityBlockingQueue<>(topNSize, (a, b) -> SearchQueryRecord.compare(a, b, metricType));
        topQueriesCurrentSnapshot = new AtomicReference<>(new ArrayList<>());
        topQueriesHistorySnapshot = new AtomicReference<>(new ArrayList<>());
        queryGrouper = new MinMaxHeapQueryGrouper(
            metricType,
            QueryInsightsSettings.DEFAULT_GROUPING_TYPE,
            AggregationType.AVERAGE,
            topQueriesStore,
            topNSize
        );
    }

    /**
     * Set the top N size for TopQueriesService service.
     *
     * @param topNSize the top N size to set
     */
    public void setTopNSize(final int topNSize) {
        this.topNSize = topNSize;
        this.queryGrouper.updateTopNSize(topNSize);
    }

    /**
     * Get the current configured top n size
     *
     * @return top n size
     */
    public int getTopNSize() {
        return topNSize;
    }

    /**
     * Validate the top N size based on the internal constrains
     *
     * @param size the wanted top N size
     */
    public void validateTopNSize(final int size) {
        if (size < 1 || size > QueryInsightsSettings.MAX_N_SIZE) {
            throw new IllegalArgumentException(
                "Top N size setting for ["
                    + metricType
                    + "]"
                    + " should be between 1 and "
                    + QueryInsightsSettings.MAX_N_SIZE
                    + ", was ("
                    + size
                    + ")"
            );
        }
    }

    /**
     * Set enable flag for the service
     *
     * @param enabled boolean
     */
    public void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Set the window size for top N queries service
     *
     * @param windowSize window size to set
     */
    public void setWindowSize(final TimeValue windowSize) {
        this.windowSize = windowSize;
        // reset the window start time since the window size has changed
        this.windowStart = -1L;
    }

    public void setGrouping(final GroupingType groupingType) {
        boolean changed = queryGrouper.setGroupingType(groupingType);
        if (changed) {
            drain();
        }
    }

    public void setMaxGroups(final int maxGroups) {
        boolean changed = queryGrouper.setMaxGroups(maxGroups);
        if (changed) {
            drain();
        }
    }

    /**
     * Validate if the window size is valid, based on internal constrains.
     *
     * @param windowSize the window size to validate
     */
    public void validateWindowSize(final TimeValue windowSize) {
        if (windowSize.compareTo(QueryInsightsSettings.MAX_WINDOW_SIZE) > 0
            || windowSize.compareTo(QueryInsightsSettings.MIN_WINDOW_SIZE) < 0) {
            throw new IllegalArgumentException(
                "Window size setting for ["
                    + metricType
                    + "]"
                    + " should be between ["
                    + QueryInsightsSettings.MIN_WINDOW_SIZE
                    + ","
                    + QueryInsightsSettings.MAX_WINDOW_SIZE
                    + "]"
                    + "was ("
                    + windowSize
                    + ")"
            );
        }
        if (!(QueryInsightsSettings.VALID_WINDOW_SIZES_IN_MINUTES.contains(windowSize) || windowSize.getMinutes() % 60 == 0)) {
            throw new IllegalArgumentException(
                "Window size setting for ["
                    + metricType
                    + "]"
                    + " should be multiple of 1 hour, or one of "
                    + QueryInsightsSettings.VALID_WINDOW_SIZES_IN_MINUTES
                    + ", was ("
                    + windowSize
                    + ")"
            );
        }
    }

    /**
     * Set up the top queries exporter based on provided settings
     *
     * @param settings exporter config {@link Settings}
     */
    public void setExporter(final Settings settings, final Map<String, IndexMetadata> indexMetadataMap) {
        // This method is invoked when sink type is changed
        // Clear local indices if exporter is of type LocalIndexExporter
        if (exporter != null && exporter.getClass() == LocalIndexExporter.class) {
            deleteAllTopNIndices(indexMetadataMap);
        }

        if (settings.get(EXPORTER_TYPE) != null) {
            SinkType expectedType = SinkType.parse(settings.get(EXPORTER_TYPE, DEFAULT_TOP_QUERIES_EXPORTER_TYPE));
            if (exporter != null && expectedType == SinkType.getSinkTypeFromExporter(exporter)) {
                queryInsightsExporterFactory.updateExporter(exporter, DEFAULT_TOP_N_QUERIES_INDEX_PATTERN);
            } else {
                try {
                    queryInsightsExporterFactory.closeExporter(this.exporter);
                } catch (IOException e) {
                    OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.EXPORTER_FAIL_TO_CLOSE_EXCEPTION);
                    logger.error("Fail to close the current exporter when updating exporter, error: ", e);
                }
                this.exporter = queryInsightsExporterFactory.createExporter(
                    SinkType.parse(settings.get(EXPORTER_TYPE, DEFAULT_TOP_QUERIES_EXPORTER_TYPE)),
                    DEFAULT_TOP_N_QUERIES_INDEX_PATTERN
                );
            }
        } else {
            // Disable exporter if exporter type is set to null
            try {
                queryInsightsExporterFactory.closeExporter(this.exporter);
                this.exporter = null;
            } catch (IOException e) {
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.EXPORTER_FAIL_TO_CLOSE_EXCEPTION);
                logger.error("Fail to close the current exporter when disabling exporter, error: ", e);
            }
        }
    }

    /**
     * Set up the top queries reader based on provided settings
     *
     * @param settings reader config {@link Settings}
     * @param namedXContentRegistry NamedXContentRegistry for parsing purposes
     */
    public void setReader(final Settings settings, final NamedXContentRegistry namedXContentRegistry) {
        this.reader = queryInsightsReaderFactory.createReader(DEFAULT_TOP_N_QUERIES_INDEX_PATTERN, namedXContentRegistry);
        queryInsightsReaderFactory.updateReader(reader, DEFAULT_TOP_N_QUERIES_INDEX_PATTERN);
    }

    /**
     * Validate provided settings for top queries exporter and reader
     *
     * @param settings settings exporter/reader config {@link Settings}
     */
    public void validateExporterAndReaderConfig(Settings settings) {
        queryInsightsExporterFactory.validateExporterConfig(settings);
    }

    /**
     * Lambda function to mark if a record is internal
     */
    private final Predicate<SearchQueryRecord> checkIfInternal = (record) -> {
        Map<Attribute, Object> attributes = record.getAttributes();
        Object indicesObject = attributes.get(Attribute.INDICES);
        if (indicesObject instanceof Object[]) {
            Object[] indices = (Object[]) indicesObject;
            return Arrays.stream(indices).noneMatch(index -> {
                if (index instanceof String) {
                    String indexString = (String) index;
                    return indexString.contains("top_queries");
                }
                return false;
            });
        }
        return true;
    };

    /**
     * Get all top queries records that are in the current top n queries store
     * Optionally include top N records from the last window.
     * <p>
     * By default, return the records in sorted order.
     *
     * @param includeLastWindow if the top N queries from the last window should be included
     * @param from start timestamp
     * @param to end timestamp
     * @return List of the records that are in the query insight store
     * @throws IllegalArgumentException if query insights is disabled in the cluster
     */
    public List<SearchQueryRecord> getTopQueriesRecords(final boolean includeLastWindow, final String from, final String to)
        throws IllegalArgumentException {
        OperationalMetricsCounter.getInstance()
            .incrementCounter(
                OperationalMetric.TOP_N_QUERIES_USAGE_COUNT,
                Tags.create()
                    .addTag(METRIC_TYPE_TAG, this.metricType.name())
                    .addTag(GROUPBY_TAG, this.queryGrouper.getGroupingType().name())
            );
        if (!enabled) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Cannot get top n queries for [%s] when it is not enabled.", metricType.toString())
            );
        }
        // read from window snapshots
        final List<SearchQueryRecord> queries = new ArrayList<>(topQueriesCurrentSnapshot.get());
        if (includeLastWindow) {
            queries.addAll(topQueriesHistorySnapshot.get());
        }
        List<SearchQueryRecord> filterQueries = queries;
        if (from != null && to != null) {
            final ZonedDateTime start = ZonedDateTime.parse(from);
            final ZonedDateTime end = ZonedDateTime.parse(to);
            Predicate<SearchQueryRecord> timeFilter = element -> start.toInstant().toEpochMilli() <= element.getTimestamp()
                && element.getTimestamp() <= end.toInstant().toEpochMilli();
            filterQueries = queries.stream().filter(checkIfInternal.and(timeFilter)).collect(Collectors.toList());
        }
        return Stream.of(filterQueries)
            .flatMap(Collection::stream)
            .sorted((a, b) -> SearchQueryRecord.compare(a, b, metricType) * -1)
            .collect(Collectors.toList());
    }

    /**
     * Get all historical top queries records that are in local index
     * <p>
     * By default, return the records in sorted order.
     *
     * @param from start timestamp
     * @param to end timestamp
     * @return List of the records that are in local index (if enabled) with timestamps between from and to
     * @throws IllegalArgumentException if query insights is disabled in the cluster
     */
    public List<SearchQueryRecord> getTopQueriesRecordsFromIndex(final String from, final String to) throws IllegalArgumentException {
        if (!enabled) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Cannot get top n queries for [%s] when it is not enabled.", metricType.toString())
            );
        }

        final List<SearchQueryRecord> queries = new ArrayList<>();
        if (reader != null) {
            try {
                final ZonedDateTime start = ZonedDateTime.parse(from);
                final ZonedDateTime end = ZonedDateTime.parse(to);
                List<SearchQueryRecord> records = reader.read(from, to);
                Predicate<SearchQueryRecord> timeFilter = element -> start.toInstant().toEpochMilli() <= element.getTimestamp()
                    && element.getTimestamp() <= end.toInstant().toEpochMilli();
                List<SearchQueryRecord> filteredRecords = records.stream()
                    .filter(checkIfInternal.and(timeFilter))
                    .collect(Collectors.toList());
                queries.addAll(filteredRecords);
            } catch (Exception e) {
                logger.error("Failed to read from index: ", e);
            }
        }
        return Stream.of(queries)
            .flatMap(Collection::stream)
            .sorted((a, b) -> SearchQueryRecord.compare(a, b, metricType) * -1)
            .collect(Collectors.toList());
    }

    /**
     * Consume records to top queries stores
     *
     * @param records a list of {@link SearchQueryRecord}
     */
    void consumeRecords(final List<SearchQueryRecord> records) {
        final long currentWindowStart = calculateWindowStart(System.currentTimeMillis());
        List<SearchQueryRecord> recordsInLastWindow = new ArrayList<>();
        List<SearchQueryRecord> recordsInThisWindow = new ArrayList<>();
        for (SearchQueryRecord record : records) {
            // skip the records that does not have the corresponding measurement
            if (!record.getMeasurements().containsKey(metricType)) {
                continue;
            }
            if (record.getTimestamp() < currentWindowStart) {
                recordsInLastWindow.add(record);
            } else {
                recordsInThisWindow.add(record);
            }
        }
        // add records in last window, if there are any, to the top n store
        addToTopNStore(recordsInLastWindow);
        // rotate window and reset window start if necessary
        rotateWindowIfNecessary(currentWindowStart);
        // add records in current window, if there are any, to the top n store
        addToTopNStore(recordsInThisWindow);
        // update the current window snapshot for getters to consume
        final List<SearchQueryRecord> newSnapShot = new ArrayList<>(topQueriesStore);
        newSnapShot.sort((a, b) -> SearchQueryRecord.compare(a, b, metricType));
        topQueriesCurrentSnapshot.set(newSnapShot);
    }

    private void addToTopNStore(final List<SearchQueryRecord> records) {
        if (queryGrouper.getGroupingType() != GroupingType.NONE) {
            for (SearchQueryRecord record : records) {
                queryGrouper.add(record);
            }
        } else {
            topQueriesStore.addAll(records);
            // remove top elements for fix sizing priority queue
            while (topQueriesStore.size() > topNSize) {
                topQueriesStore.poll();
            }
        }
    }

    /**
     * Reset the current window and rotate the data to history snapshot for top n queries,
     * This function would be invoked zero time or only once in each consumeRecords call
     *
     * @param newWindowStart the new windowStart to set to
     */
    private void rotateWindowIfNecessary(final long newWindowStart) {
        // reset window if the current window is outdated
        if (windowStart < newWindowStart) {
            final List<SearchQueryRecord> history = new ArrayList<>();
            // rotate the current window to history store only if the data belongs to the last window
            if (windowStart == newWindowStart - windowSize.getMillis()) {
                history.addAll(topQueriesStore);
            }
            topQueriesHistorySnapshot.set(history);
            topQueriesStore.clear();
            if (queryGrouper.getGroupingType() != GroupingType.NONE) {
                queryGrouper.drain();
            }
            topQueriesCurrentSnapshot.set(new ArrayList<>());
            windowStart = newWindowStart;
            // export to the configured sink
            if (exporter != null) {
                threadPool.executor(QUERY_INSIGHTS_EXECUTOR).execute(() -> exporter.export(history));
            }
        }
    }

    /**
     * Calculate the window start for the given timestamp
     *
     * @param timestamp the given timestamp to calculate window start
     */
    private long calculateWindowStart(final long timestamp) {
        final LocalDateTime currentTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("UTC"));
        LocalDateTime windowStartTime = currentTime.truncatedTo(ChronoUnit.HOURS);
        while (!windowStartTime.plusMinutes(windowSize.getMinutes()).isAfter(currentTime)) {
            windowStartTime = windowStartTime.plusMinutes(windowSize.getMinutes());
        }
        return windowStartTime.toInstant(ZoneOffset.UTC).getEpochSecond() * 1000;
    }

    /**
     * Get the current top queries snapshot from the AtomicReference.
     *
     * @return a list of {@link SearchQueryRecord}
     */
    public List<SearchQueryRecord> getTopQueriesCurrentSnapshot() {
        return topQueriesCurrentSnapshot.get();
    }

    /**
     * Close the top n queries service
     * @throws IOException exception
     */
    public void close() throws IOException {
        queryInsightsExporterFactory.closeExporter(this.exporter);
        queryInsightsReaderFactory.closeReader(this.reader);
    }

    /**
     * Drain internal stores.
     */
    private void drain() {
        topQueriesStore.clear();
        topQueriesHistorySnapshot.set(new ArrayList<>());
        topQueriesCurrentSnapshot.set(new ArrayList<>());
    }

    /**
     * Get top queries service health stats
     *
     * @return TopQueriesHealthStats
     */
    public TopQueriesHealthStats getHealthStats() {
        return new TopQueriesHealthStats(this.topQueriesStore.size(), this.queryGrouper.getHealthStats());
    }

    /**
     * Validate the exporter delete after value
     *
     * @param deleteAfter exporter and reader settings
     */
    static void validateExporterDeleteAfter(final int deleteAfter) {
        if (deleteAfter < MIN_DELETE_AFTER_VALUE || deleteAfter > MAX_DELETE_AFTER_VALUE) {
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.INVALID_EXPORTER_TYPE_FAILURES);
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Invalid exporter delete_after_days setting [%d], value should be an integer between %d and %d.",
                    deleteAfter,
                    MIN_DELETE_AFTER_VALUE,
                    MAX_DELETE_AFTER_VALUE
                )
            );
        }
    }

    /**
     * Set exporter delete after if exporter is a {@link LocalIndexExporter}
     *
     * @param deleteAfter the number of days after which Top N local indices should be deleted
     */
    void setExporterDeleteAfter(final int deleteAfter) {
        if (exporter != null && exporter.getClass() == LocalIndexExporter.class) {
            ((LocalIndexExporter) exporter).setDeleteAfter(deleteAfter);
        }
    }

    /**
     * Delete Top N local indices older than the configured data retention period
     */
    void deleteExpiredTopNIndices(final Map<String, IndexMetadata> indexMetadataMap) {
        if (exporter != null && exporter.getClass() == LocalIndexExporter.class) {
            threadPool.executor(QUERY_INSIGHTS_EXECUTOR)
                .execute(() -> ((LocalIndexExporter) exporter).deleteExpiredTopNIndices(indexMetadataMap));
        }
    }

    /**
     * Deletes all Top N local indices
     *
     * @param indexMetadataMap Map of index name {@link String} to {@link IndexMetadata}
     */
    void deleteAllTopNIndices(final Map<String, IndexMetadata> indexMetadataMap) {
        indexMetadataMap.entrySet()
            .stream()
            .filter(entry -> isTopQueriesIndex(entry.getKey()))
            .forEach(entry -> deleteSingleIndex(entry.getKey(), client));
    }

    /**
     * Deletes the specified index and logs any failure that occurs during the operation.
     *
     * @param indexName The name of the index to delete.
     * @param client The OpenSearch client used to perform the deletion.
     */
    public static void deleteSingleIndex(String indexName, Client client) {
        Logger logger = LogManager.getLogger();
        client.admin().indices().delete(new DeleteIndexRequest(indexName), new ActionListener<>() {
            @Override
            // CS-SUPPRESS-SINGLE: RegexpSingleline It is not possible to use phrase "cluster manager" instead of master here
            public void onResponse(org.opensearch.action.support.master.AcknowledgedResponse acknowledgedResponse) {}

            @Override
            public void onFailure(Exception e) {
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_DELETE_FAILURES);
                logger.error("Failed to delete index '{}': ", indexName, e);
            }
        });
    }

    /**
     * Validates if the input string is a Query Insights local index name
     * in the format "top_queries-YYYY.MM.dd-XXXXX".
     *
     * @param indexName the string to validate.
     * @return {@code true} if the string is valid, {@code false} otherwise.
     */
    public static boolean isTopQueriesIndex(String indexName) {
        // Split the input string by '-'
        String[] parts = indexName.split("-");

        // Check if the string has exactly 3 parts
        if (parts.length != 3) {
            return false;
        }

        // Validate the first part is "top_queries"
        if (!"top_queries".equals(parts[0])) {
            return false;
        }

        // Validate the second part is a valid date in "YYYY.MM.dd" format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT);
        try {
            LocalDate.parse(parts[1], formatter);
        } catch (DateTimeParseException e) {
            return false;
        }

        // Validate the third part is exactly 5 digits
        return parts[2].matches("\\d{5}");
    }
}
