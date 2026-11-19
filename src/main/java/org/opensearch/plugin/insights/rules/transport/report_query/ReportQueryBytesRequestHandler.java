/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.rules.transport.report_query;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;
import org.opensearch.plugin.insights.core.service.ChildSearchCosts;
import org.opensearch.plugin.insights.core.service.FinishedQueriesCache;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.core.service.TopQueriesService;
import org.opensearch.plugin.insights.rules.action.report_query.ReportQueryBytesAction;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.SourceString;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.tasks.Task;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;

/**
 * Handles the SQL plugin's report of a finished SQL/PPL query (see {@link ReportQueryBytesAction}).
 * It turns the payload into a {@link SearchQueryRecord}, adds the cost of the searches the query ran,
 * and adds it to Top N and the finished queries cache like a DSL search.
 * <p>
 * Errors are logged, and the sender always gets an empty response, so reporting never fails a query.
 */
public class ReportQueryBytesRequestHandler implements TransportRequestHandler<BytesTransportRequest> {

    private static final Logger logger = LogManager.getLogger(ReportQueryBytesRequestHandler.class);

    private final QueryInsightsService queryInsightsService;

    /** @param queryInsightsService the service records are added to */
    public ReportQueryBytesRequestHandler(final QueryInsightsService queryInsightsService) {
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    public void messageReceived(final BytesTransportRequest request, final TransportChannel channel, final Task task) throws Exception {
        try {
            final SearchQueryRecord record = deserialize(request);
            if (record != null) {
                // Its searches have all ended, so their cost is final.
                addCostOfItsSearches(record);
                // Set the user now so the finished entry has it for filter_by_mode.
                TopQueriesService.setUserInfo(record);
                // Capture before addRecord hands the record to the drain thread, which changes it.
                captureFinished(record);
                queryInsightsService.addRecord(record);
            }
        } catch (final Exception e) {
            logger.warn("Failed to ingest reported PPL/SQL query record into Query Insights", e);
        }
        // A core response type, for the same classloader reason as the request.
        channel.sendResponse(TransportResponse.Empty.INSTANCE);
    }

    /** Adds its searches' CPU and memory to the query's. Latency already covers the searches. */
    private void addCostOfItsSearches(final SearchQueryRecord record) {
        final ChildSearchCosts.Cost searches = queryInsightsService.takeChildSearchCosts(
            (String) record.getAttributes().get(Attribute.PARENT_MARKER)
        );
        if (searches == null) {
            return;
        }
        addTo(record, MetricType.CPU, searches.getCpuNanos());
        addTo(record, MetricType.MEMORY, searches.getMemoryBytes());
    }

    private static void addTo(final SearchQueryRecord record, final MetricType metricType, final long add) {
        final Number own = record.getMeasurement(metricType);
        record.getMeasurements().put(metricType, new Measurement((own == null ? 0L : own.longValue()) + add, AggregationType.NONE));
    }

    /** Adds the query to finished queries under its Live Queries id, so the dashboard can follow it. */
    private void captureFinished(final SearchQueryRecord record) {
        final FinishedQueriesCache finishedQueries = queryInsightsService.getFinishedQueriesCache();
        final String liveQueryId = liveQueryIdOf((String) record.getAttributes().get(Attribute.PARENT_MARKER));
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

    /**
     * Reads the payload into a {@link SearchQueryRecord}.
     *
     * @param request the request
     * @return the record, or {@code null} for an unknown format version
     * @throws java.io.IOException if the payload is malformed
     */
    private SearchQueryRecord deserialize(final BytesTransportRequest request) throws java.io.IOException {
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
            // A SQL/PPL query has no SearchSourceBuilder, so its text is the source.
            if (queryText != null && queryText.isEmpty() == false) {
                attributes.put(Attribute.SOURCE, new SourceString(queryText));
            }
            // Set group_by as the search listener does; the dashboard shows records without it as aggregated.
            attributes.put(Attribute.GROUP_BY, QueryInsightsSettings.DEFAULT_GROUPING_TYPE);
            // Its searches carry this marker as DERIVED_FROM.
            attributes.put(Attribute.PARENT_MARKER, coordinatorId);
            // No INDICES: the SQL plugin doesn't resolve them. Each search's record has its own.

            // The user comes as a string, since the thread context doesn't cross the transport call.
            final UserPrincipalContext userPrincipalContext = userInfo == null || userInfo.isEmpty()
                ? null
                : UserPrincipalContext.fromUserString(userInfo);

            // A null id gets a random UUID, as a DSL search's record does.
            return new SearchQueryRecord(timestampMillis, measurements, attributes, null, userPrincipalContext, null);
        }
    }
}
