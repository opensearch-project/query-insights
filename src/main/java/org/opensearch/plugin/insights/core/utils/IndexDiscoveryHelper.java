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
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.support.IndicesOptions;

/**
 * Utility class for discovering and managing indices for Query Insights operations.
 * This class provides methods to find existing indices within date ranges and
 * handle cluster state operations.
 */
public final class IndexDiscoveryHelper {

    private IndexDiscoveryHelper() {}

    /**
     * Generates the expected Query Insights local index name for every day in
     * interval start-end inclusive, without getting the cluster state.
     *
     * Names that do not correspond to an actual index are skipped
     * by the search request itself.
     *
     * @param indexPattern DateTimeFormatter pattern for index naming
     * @param start Start date for the search range
     * @param end End date for the search range
     * @return List of index names, one per day, in ascending date order
     */
    public static List<String> buildIndexNamesInDateRange(
        final DateTimeFormatter indexPattern,
        final ZonedDateTime start,
        final ZonedDateTime end
    ) {
        final List<String> indexNames = new ArrayList<>();

        ZonedDateTime currentDay = start.toLocalDate().atStartOfDay(start.getZone());
        final ZonedDateTime endDay = end.toLocalDate().atStartOfDay(end.getZone());

        while (!currentDay.isAfter(endDay)) {
            indexNames.add(buildLocalIndexName(indexPattern, currentDay));
            currentDay = currentDay.plusDays(1);
        }

        return indexNames;
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
