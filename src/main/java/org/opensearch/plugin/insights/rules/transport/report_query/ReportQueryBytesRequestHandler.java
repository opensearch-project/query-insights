/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.rules.transport.report_query;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;
import org.opensearch.plugin.insights.core.service.ChildSearchTracker;
import org.opensearch.plugin.insights.core.service.FinishedQueriesCache;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.core.service.TopQueriesService;
import org.opensearch.plugin.insights.rules.action.report_query.ReportQueryBytesAction;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.tasks.Task;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;

/**
 * Handles the SQL plugin's report of a finished SQL/PPL query (see {@link ReportQueryBytesAction}).
 * It turns the payload into a {@link SearchQueryRecord}, adds the searches the query ran (their
 * indices, shards, type and cost, and the first of them as {@link Attribute#CHILD_SEARCHES}), and adds
 * it to Top N and the finished queries cache like a DSL search.
 * <p>
 * Errors are logged, and the sender always gets an empty response, so reporting never fails a query.
 */
public class ReportQueryBytesRequestHandler implements TransportRequestHandler<BytesTransportRequest> {

    private static final Logger logger = LogManager.getLogger(ReportQueryBytesRequestHandler.class);

    /** A search's default type, written as the search listener writes a search's. */
    static final String DEFAULT_SEARCH_TYPE = SearchType.DEFAULT.toString().toLowerCase(Locale.ROOT);

    private final QueryInsightsService queryInsightsService;

    /** @param queryInsightsService the service records are added to */
    public ReportQueryBytesRequestHandler(final QueryInsightsService queryInsightsService) {
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    public void messageReceived(final BytesTransportRequest request, final TransportChannel channel, final Task task) throws Exception {
        try {
            final Reported reported = deserialize(request);
            if (reported != null) {
                record(reported);
            }
        } catch (final Exception e) {
            logger.warn("Failed to ingest reported PPL/SQL query record into Query Insights", e);
        }
        // A core response type, for the same classloader reason as the request.
        channel.sendResponse(TransportResponse.Empty.INSTANCE);
    }

    private void record(final Reported reported) {
        final SearchQueryRecord record = reported.queryRecord();
        // Its searches have all ended, so they're final.
        final ChildSearchTracker.Tracked searches = queryInsightsService.takeChildSearches(reported.marker());
        if (searches != null && searches.isExcluded()) {
            // One of them read an excluded index. A DSL search on it isn't recorded, so neither is the query.
            return;
        }
        addItsSearches(record, searches);
        // Set the user now so the finished entry has it for filter_by_mode.
        TopQueriesService.setUserInfo(record);
        // Capture before addRecord hands the record to the drain thread, which changes it.
        captureFinished(record, reported.marker());
        queryInsightsService.addRecord(record);
    }

    /**
     * Gives the query the indices its searches read, their shards and their search type, as a DSL
     * search's record has. Then adds their CPU and memory to the query's, and lists them in its record,
     * which keeps them if it makes Top N. Latency already covers the searches. A query that ran none
     * gets no indices and the default search type, since readers such as an older Dashboards expect
     * both on every record.
     */
    private static void addItsSearches(final SearchQueryRecord record, final ChildSearchTracker.Tracked searches) {
        final String searchType = searches == null ? null : searches.getSearchType();
        record.addAttribute(Attribute.INDICES, searches == null ? new String[0] : searches.getIndices());
        record.addAttribute(Attribute.TOTAL_SHARDS, searches == null ? 0 : searches.getTotalShards());
        record.addAttribute(Attribute.SEARCH_TYPE, searchType == null ? DEFAULT_SEARCH_TYPE : searchType);
        if (searches == null) {
            return;
        }
        addTo(record, MetricType.CPU, searches.getCpuNanos());
        addTo(record, MetricType.MEMORY, searches.getMemoryBytes());
        record.addAttribute(Attribute.CHILD_SEARCHES, searches.toChildSearches());
    }

    private static void addTo(final SearchQueryRecord record, final MetricType metricType, final long add) {
        final Number own = record.getMeasurement(metricType);
        record.getMeasurements().put(metricType, new Measurement((own == null ? 0L : own.longValue()) + add, AggregationType.NONE));
    }

    /** Adds the query to finished queries under its Live Queries id, so the dashboard can follow it. */
    private void captureFinished(final SearchQueryRecord record, final String marker) {
        final FinishedQueriesCache finishedQueries = queryInsightsService.getFinishedQueriesCache();
        final String liveQueryId = liveQueryIdOf(marker);
        if (finishedQueries != null && liveQueryId != null) {
            finishedQueries.capture(record, liveQueryId);
        }
    }

    /**
     * Strips the source from a marker: {@code PPL:<nodeId>:<taskId>} becomes the Live Queries id
     * {@code <nodeId>:<taskId>}.
     *
     * @param parentMarker the query's marker
     * @return the Live Queries id, or {@code null} if the marker has another form
     */
    static String liveQueryIdOf(final String parentMarker) {
        if (parentMarker == null) {
            return null;
        }
        final String[] parts = parentMarker.split(":", -1);
        if (parts.length != 3 || parts[1].isEmpty() || parts[2].isEmpty()) {
            return null;
        }
        for (int i = 0; i < parts[2].length(); i++) {
            if (!Character.isDigit(parts[2].charAt(i))) {
                return null;
            }
        }
        return parts[1] + ":" + parts[2];
    }

    /** A reported query's record, and the marker its searches carry. */
    private record Reported(SearchQueryRecord queryRecord, String marker) {
    }

    /**
     * Reads the payload into a {@link SearchQueryRecord}.
     *
     * @param request the request
     * @return the record and the query's marker, or {@code null} for an unknown format version
     * @throws java.io.IOException if the payload is malformed
     */
    private Reported deserialize(final BytesTransportRequest request) throws java.io.IOException {
        try (StreamInput in = request.bytes().streamInput()) {
            final int version = in.readVInt();
            if (version != ReportQueryBytesAction.FORMAT_VERSION) {
                logger.warn("Ignoring reported query record with unsupported wire version {}", version);
                return null;
            }
            final String querySource = in.readString();
            final String coordinatorId = in.readString();
            final String nodeId = in.readString();
            final String queryText = in.readString();
            final long timestampMillis = in.readVLong();
            final long latencyMillis = in.readVLong();
            final long cpuNanos = in.readVLong();
            final long memoryBytes = in.readVLong();

            final String userInfo = in.readString();

            final boolean failed = in.readBoolean();

            final Map<MetricType, Measurement> measurements = new HashMap<>();
            measurements.put(MetricType.LATENCY, new Measurement(latencyMillis, AggregationType.NONE));
            measurements.put(MetricType.CPU, new Measurement(cpuNanos, AggregationType.NONE));
            measurements.put(MetricType.MEMORY, new Measurement(memoryBytes, AggregationType.NONE));

            final Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.QUERY_SOURCE, querySource);
            attributes.put(Attribute.NODE_ID, nodeId);
            attributes.put(Attribute.FAILED, failed);
            // Set group_by as the search listener does; the dashboard shows records without it as aggregated.
            attributes.put(Attribute.GROUP_BY, QueryInsightsSettings.DEFAULT_GROUPING_TYPE);
            // Also as the listener does. Marking it Top N at window rotation then changes only this map,
            // not the attributes map, which a response may be writing at the time.
            attributes.put(Attribute.TOP_N_QUERY, new HashMap<>(SearchQueryRecord.DEFAULT_TOP_N_QUERY_MAP));
            // Its indices, shards and search type come from its searches when it's recorded (see addItsSearches).

            // The user comes as a string, since the thread context doesn't cross the transport call.
            final UserPrincipalContext userPrincipalContext = userInfo == null || userInfo.isEmpty()
                ? null
                : UserPrincipalContext.fromUserString(userInfo);

            // A null id gets a random UUID, as a DSL search's record does.
            final SearchQueryRecord record = new SearchQueryRecord(
                timestampMillis,
                measurements,
                attributes,
                null,
                userPrincipalContext,
                null
            );
            // Its text goes in query_text, cut to max_source_length. Readers parse source as a DSL body,
            // so a SQL/PPL query has none.
            if (queryText != null && queryText.isEmpty() == false) {
                TopQueriesService.setQueryText(record, queryText, queryInsightsService.getMaxSourceLength());
            }
            return new Reported(record, coordinatorId);
        }
    }
}
