/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import static org.opensearch.plugin.insights.core.service.QueryInsightsService.QUERY_INSIGHTS_INDEX_TAG_NAME;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.INDEX_DATE_FORMAT_PATTERN;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_QUERIES_INDEX_PREFIX;

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
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.core.auth.PrincipalExtractor;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporter;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterFactory;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReader;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReaderFactory;
import org.opensearch.plugin.insights.core.service.grouper.MinMaxHeapQueryGrouper;
import org.opensearch.plugin.insights.core.service.grouper.QueryGrouper;
import org.opensearch.plugin.insights.core.utils.ExporterReaderUtils;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.SourceString;
import org.opensearch.plugin.insights.rules.model.healthStats.TopQueriesHealthStats;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import reactor.util.annotation.NonNull;

/**
 * Service responsible for gathering and storing top N queries
 * with high latency or resource usage
 */
public class TopQueriesService {
    /**
     * Top queries services utilize a shared exporter and reader instance.
     * These shared components are uniquely identified by TOP_QUERIES_EXPORTER_ID and TOP_QUERIES_READER_ID
     */
    public static final String TOP_QUERIES_EXPORTER_ID = "top_queries_exporter";
    public static final String TOP_QUERIES_READER_ID = "top_queries_reader";
    /**
     * Tag value used to identify local index mappings that are specifically created
     * by TopQueriesService. This tag serves as a unique identifier for tracking and
     * managing indices associated with top queries operations.
     */
    public static final String TOP_QUERIES_INDEX_TAG_VALUE = "top_n_queries";
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

    private final QueryGrouper queryGrouper;

    private int maxSourceLength;

    TopQueriesService(
        final Client client,
        final MetricType metricType,
        final ThreadPool threadPool,
        final QueryInsightsExporterFactory queryInsightsExporterFactory,
        final QueryInsightsReaderFactory queryInsightsReaderFactory
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
        this.maxSourceLength = QueryInsightsSettings.DEFAULT_MAX_SOURCE_LENGTH;
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
     * Set the maximum source length before truncation
     *
     * @param maxSourceLength maximum length for source strings
     */
    public void setMaxSourceLength(final int maxSourceLength) {
        this.maxSourceLength = maxSourceLength;
        this.queryGrouper.setMaxSourceLength(maxSourceLength);
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
        // Clear all snapshots when disabling to prevent stale queries from appearing when re-enabled
        if (!enabled) {
            drain();
        }
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
     * Get all top queries records that are in the current top n queries store
     * Optionally include top N records from the last window.
     * <p>
     * By default, return the records in sorted order.
     *
     * @param includeLastWindow if the top N queries from the last window should be included
     * @param from start timestamp
     * @param to end timestamp
     * @param id unique identifier for query/query group
     * @param verbose whether to return full output
     * @return List of the records that are in the query insight store
     * @throws IllegalArgumentException if query insights is disabled in the cluster
     */
    public List<SearchQueryRecord> getTopQueriesRecords(
        @NonNull final boolean includeLastWindow,
        @Nullable final String from,
        @Nullable final String to,
        @Nullable final String id,
        @Nullable final Boolean verbose
    ) {
        OperationalMetricsCounter.getInstance()
            .incrementCounter(
                OperationalMetric.TOP_N_QUERIES_USAGE_COUNT,
                Tags.create()
                    .addTag(METRIC_TYPE_TAG, this.metricType.name())
                    .addTag(GROUPBY_TAG, this.queryGrouper.getGroupingType().name())
            );

        // Return empty results when service is disabled
        if (!enabled) {
            return new ArrayList<>();
        }

        // read from window snapshots
        final List<SearchQueryRecord> queries = new ArrayList<>(topQueriesCurrentSnapshot.get());
        if (includeLastWindow) {
            queries.addAll(topQueriesHistorySnapshot.get());
        }
        List<SearchQueryRecord> filterQueries = queries;

        // Time-based filtering
        if (from != null && to != null) {
            final ZonedDateTime start = ZonedDateTime.parse(from);
            final ZonedDateTime end = ZonedDateTime.parse(to);
            Predicate<SearchQueryRecord> timeFilter = element -> start.toInstant().toEpochMilli() <= element.getTimestamp()
                && element.getTimestamp() <= end.toInstant().toEpochMilli();
            filterQueries = queries.stream().filter(timeFilter).collect(Collectors.toList());
        }

        // Filter based on the id, if provided
        if (id != null) {
            filterQueries = filterQueries.stream().filter(record -> record.getId().equals(id)).collect(Collectors.toList());
        }

        // If verbose is false (not null), trim records
        if (Boolean.FALSE.equals(verbose)) {
            filterQueries = filterQueries.stream().map(SearchQueryRecord::copyAndSimplifyRecord).collect(Collectors.toList());
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
     * @param to   end timestamp
     * @param id search query record id
     * @param verbose whether to return full output
     * @throws IllegalArgumentException if query insights is disabled in the cluster
     */
    public void getTopQueriesRecordsFromIndex(
        final String from,
        final String to,
        final String id,
        final Boolean verbose,
        final ActionListener<List<SearchQueryRecord>> listener
    ) {
        final QueryInsightsReader reader = queryInsightsReaderFactory.getReader(TOP_QUERIES_READER_ID);
        if (reader == null) {
            listener.onResponse(new ArrayList<>());
            return;
        }

        try {
            reader.read(from, to, id, verbose, metricType, new ActionListener<List<SearchQueryRecord>>() {
                @Override
                public void onResponse(List<SearchQueryRecord> records) {
                    try {
                        List<SearchQueryRecord> filteredRecords = records.stream()
                            .sorted((a, b) -> SearchQueryRecord.compare(a, b, metricType) * -1)
                            .collect(Collectors.toList());
                        listener.onResponse(filteredRecords);
                    } catch (Exception e) {
                        logger.error("Failed to process records from index: ", e);
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Failed to read from index: ", e);
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            logger.error("Failed to initiate read from index: ", e);
            listener.onFailure(e);
        }
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
            // Add Source Attribute and extract user info for all top queries with truncation
            for (SearchQueryRecord record : topQueriesStore) {
                if (record.getAttributes().get(Attribute.SOURCE) == null) {
                    setSourceAndTruncation(record, maxSourceLength);
                }
                if (record.getAttributes().get(Attribute.USERNAME) == null) {
                    setUserInfo(record);
                }
            }
        }
    }

    // Add Source and Source Truncated attributes to record
    public static void setSourceAndTruncation(final SearchQueryRecord record, final int maxSourceLength) {
        String sourceString = record.getSearchSourceBuilder().toString();
        if (maxSourceLength == 0) {
            record.addAttribute(Attribute.SOURCE, new SourceString(""));
            record.addAttribute(Attribute.SOURCE_TRUNCATED, true);
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.TOP_N_QUERIES_SOURCE_TRUNCATION);
        } else if (sourceString.length() > maxSourceLength) {
            record.addAttribute(Attribute.SOURCE, new SourceString(sourceString.substring(0, maxSourceLength)));
            record.addAttribute(Attribute.SOURCE_TRUNCATED, true);
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.TOP_N_QUERIES_SOURCE_TRUNCATION);
        } else {
            record.addAttribute(Attribute.SOURCE, new SourceString(sourceString));
            record.addAttribute(Attribute.SOURCE_TRUNCATED, false);
        }
    }

    // Add Username and User Roles attributes to record
    public static void setUserInfo(final SearchQueryRecord record) {
        PrincipalExtractor principalExtractor = record.getPrincipalExtractor();
        if (principalExtractor != null) {
            PrincipalExtractor.UserPrincipalInfo userInfo = principalExtractor.extractUserInfo();
            if (userInfo != null) {
                if (userInfo.getUserName() != null) {
                    record.addAttribute(Attribute.USERNAME, userInfo.getUserName());
                }
                if (userInfo.getRoles() != null && !userInfo.getRoles().isEmpty()) {
                    record.addAttribute(Attribute.USER_ROLES, userInfo.getRoles().toArray(new String[0]));
                }
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

            // update top_n_query map for all top queries in the window
            for (SearchQueryRecord record : history) {
                record.setTopNTrue(metricType);
            }

            // export to the configured sink
            QueryInsightsExporter exporter = queryInsightsExporterFactory.getExporter(TOP_QUERIES_EXPORTER_ID);
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
    public void close() throws IOException {}

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
     * Validates if the input string is a Query Insights local index
     * in the format "top_queries-YYYY.MM.dd-XXXXX", and has the expected index metadata.
     *
     * @param indexName the index name to validate.
     * @param indexMetadata the metadata associated with the index
     * @return {@code true} if the string is valid, {@code false} otherwise.
     */
    public static boolean isTopQueriesIndex(String indexName, IndexMetadata indexMetadata) {
        try {
            if (indexMetadata == null || indexMetadata.mapping() == null) {
                return false;
            }
            Map<String, Object> sourceMap = Objects.requireNonNull(indexMetadata.mapping()).getSourceAsMap();
            if (sourceMap == null || !sourceMap.containsKey("_meta")) {
                return false;
            }
            Map<String, Object> metaMap = (Map<String, Object>) sourceMap.get("_meta");
            if (metaMap == null || !metaMap.containsKey(QUERY_INSIGHTS_INDEX_TAG_NAME)) {
                return false;
            }
            if (!metaMap.get(QUERY_INSIGHTS_INDEX_TAG_NAME).equals(TOP_QUERIES_INDEX_TAG_VALUE)) {
                return false;
            }

            // Split the input string by '-'
            String[] parts = indexName.split("-");

            // Check if the string has exactly 3 parts
            if (parts.length != 3) {
                return false;
            }

            // Validate the first part is "top_queries"
            if (!TOP_QUERIES_INDEX_PREFIX.equals(parts[0])) {
                return false;
            }

            // Validate the second part is a valid date in "YYYY.MM.dd" format
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(INDEX_DATE_FORMAT_PATTERN, Locale.ROOT);
            LocalDate date;
            try {
                date = LocalDate.parse(parts[1], formatter);
            } catch (DateTimeParseException e) {
                return false;
            }

            // Generate the expected hash for the date and compare with the third part
            String expectedHash = ExporterReaderUtils.generateLocalIndexDateHash(date);
            return parts[2].equals(expectedHash);
        } catch (Exception e) {
            return false;
        }
    }
}
