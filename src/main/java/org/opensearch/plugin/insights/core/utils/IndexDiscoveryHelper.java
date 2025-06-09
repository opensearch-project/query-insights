/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.utils;

import static org.opensearch.plugin.insights.core.utils.ExporterReaderUtils.generateLocalIndexDateHash;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_QUERIES_INDEX_PATTERN_GLOB;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;

/**
 * Utility class for discovering and managing indices for Query Insights operations.
 * This class provides methods to find existing indices within date ranges and
 * handle cluster state operations.
 */
public final class IndexDiscoveryHelper {

    private static final Logger logger = LogManager.getLogger(IndexDiscoveryHelper.class);

    private IndexDiscoveryHelper() {}

    /**
     * Discovers existing indices within the specified date range and executes a callback
     * with the list of found indices.
     *
     * @param client OpenSearch client for cluster operations
     * @param indexPattern DateTimeFormatter pattern for index naming
     * @param start Start date for the search range
     * @param end End date for the search range
     * @param callback Callback to execute with the discovered indices
     */
    public static void discoverIndicesInDateRange(
        final Client client,
        final DateTimeFormatter indexPattern,
        final ZonedDateTime start,
        final ZonedDateTime end,
        final ActionListener<List<String>> callback
    ) {
        ClusterStateRequest clusterStateRequest = createClusterStateRequest();

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(ClusterStateResponse clusterStateResponse) {
                try {
                    Set<String> existingIndices = clusterStateResponse.getState().metadata().indices().keySet();
                    List<String> indexNames = findIndicesInDateRange(existingIndices, indexPattern, start, end);
                    callback.onResponse(indexNames);
                } catch (Exception e) {
                    logger.error("Error processing cluster state response for index discovery: ", e);
                    OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_READER_SEARCH_EXCEPTIONS);
                    callback.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_READER_SEARCH_EXCEPTIONS);
                logger.error("Failed to get cluster state for indices matching {}: ", TOP_QUERIES_INDEX_PATTERN_GLOB, e);
                callback.onFailure(e);
            }
        });
    }

    /**
     * Creates a cluster state request configured for index discovery.
     *
     * @return Configured ClusterStateRequest
     */
    private static ClusterStateRequest createClusterStateRequest() {
        return createClusterStateRequest(IndicesOptions.lenientExpandOpen());
    }

    /**
     * Creates a cluster state request configured for index operations with custom indices options.
     *
     * @param indicesOptions The indices options to use
     * @return Configured ClusterStateRequest
     */
    public static ClusterStateRequest createClusterStateRequest(final IndicesOptions indicesOptions) {
        return new ClusterStateRequest().clear()
            .indices(TOP_QUERIES_INDEX_PATTERN_GLOB)
            .metadata(true)
            .local(true)
            .indicesOptions(indicesOptions);
    }

    /**
     * Finds existing indices within the specified date range.
     *
     * @param existingIndices Set of all existing indices from cluster state
     * @param indexPattern DateTimeFormatter pattern for index naming
     * @param start Start date for the search range
     * @param end End date for the search range
     * @return List of index names that exist within the date range
     */
    static List<String> findIndicesInDateRange(
        final Set<String> existingIndices,
        final DateTimeFormatter indexPattern,
        final ZonedDateTime start,
        final ZonedDateTime end
    ) {
        List<String> indexNames = new ArrayList<>();

        // Normalize dates to start of day to ensure proper day-by-day iteration
        ZonedDateTime currentDay = start.toLocalDate().atStartOfDay(start.getZone());
        ZonedDateTime endDay = end.toLocalDate().atStartOfDay(end.getZone());

        // Iterate through each day in the range [start, end] and add only existing indices
        while (!currentDay.isAfter(endDay)) {
            String potentialIndexName = buildLocalIndexName(indexPattern, currentDay);
            if (existingIndices.contains(potentialIndexName)) {
                indexNames.add(potentialIndexName);
            }
            currentDay = currentDay.plusDays(1);
        }

        return indexNames;
    }

    /**
     * Builds a local index name for the specified date using the given pattern.
     *
     * @param indexPattern DateTimeFormatter pattern for index naming
     * @param date The date for which to build the index name
     * @return The constructed index name
     */
    public static String buildLocalIndexName(final DateTimeFormatter indexPattern, final ZonedDateTime date) {
        return indexPattern.format(date) + "-" + generateLocalIndexDateHash(date.toLocalDate());
    }
}
