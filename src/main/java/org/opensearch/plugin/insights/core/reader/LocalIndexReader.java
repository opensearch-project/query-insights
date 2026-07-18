/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.reader;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.utils.IndexDiscoveryHelper;
import org.opensearch.plugin.insights.core.utils.QueryInsightsQueryBuilder;
import org.opensearch.plugin.insights.core.utils.SearchResponseParser;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.transport.client.Client;
import reactor.util.annotation.NonNull;

/**
 * Local index reader for reading query insights data from local OpenSearch indices.
 */
public final class LocalIndexReader implements QueryInsightsReader {
    /**
     * Logger of the local index reader
     */
    private final Logger logger = LogManager.getLogger();
    private final Client client;
    private DateTimeFormatter indexPattern;
    private final NamedXContentRegistry namedXContentRegistry;
    private final String id;
    private volatile int deleteAfterDays;

    /**
     * Constructor of LocalIndexReader
     *
     * @param client OS client
     * @param indexPattern the pattern of index to read from
     * @param namedXContentRegistry for parsing purposes
     * @param id reader id
     * @param deleteAfterDays value of the delete-after retention setting
     */
    public LocalIndexReader(
        final Client client,
        final DateTimeFormatter indexPattern,
        final NamedXContentRegistry namedXContentRegistry,
        final String id,
        final int deleteAfterDays
    ) {
        this.indexPattern = indexPattern;
        this.client = client;
        this.id = id;
        this.namedXContentRegistry = namedXContentRegistry;
        this.deleteAfterDays = deleteAfterDays;
    }

    @Override
    public String getId() {
        return id;
    }

    /**
     * Getter of indexPattern
     *
     * @return indexPattern
     */
    public DateTimeFormatter getIndexPattern() {
        return indexPattern;
    }

    /**
     * Setter of indexPattern
     *
     * @param indexPattern index pattern
     * @return the current LocalIndexReader
     */
    public LocalIndexReader setIndexPattern(DateTimeFormatter indexPattern) {
        this.indexPattern = indexPattern;
        return this;
    }

    public void setDeleteAfterDays(final int deleteAfterDays) {
        this.deleteAfterDays = deleteAfterDays;
    }

    /**
     * Export a list of SearchQueryRecord from local index
     *
     * @param from       start timestamp
     * @param to         end timestamp
     * @param id         query/group id
     * @param verbose    whether to return full output
     * @param metricType metric type to read
     */
    @Override
    public void read(
        final String from,
        final String to,
        final String id,
        final Boolean verbose,
        @NonNull final MetricType metricType,
        final ActionListener<List<SearchQueryRecord>> listener
    ) {
        read(from, to, id, verbose, metricType, null, null, listener);
    }

    @Override
    public void read(
        final String from,
        final String to,
        final String id,
        final Boolean verbose,
        @NonNull final MetricType metricType,
        final String username,
        final List<String> backendRoles,
        final ActionListener<List<SearchQueryRecord>> listener
    ) {
        // Validate input parameters
        if (from == null || to == null) {
            listener.onResponse(new ArrayList<>());
            return;
        }

        // Parse and validate date range
        final ZonedDateTime start = ZonedDateTime.parse(from);
        ZonedDateTime end = ZonedDateTime.parse(to);
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        if (end.isAfter(now)) {
            end = now;
        }
        final ZonedDateTime finalEnd = end;

        final ZonedDateTime retentionTime = now.minusDays(this.deleteAfterDays);
        final ZonedDateTime effectiveStart = start.isBefore(retentionTime) ? retentionTime : start;

        if (effectiveStart.isAfter(finalEnd)) {
            listener.onResponse(Collections.emptyList());
            return;
        }

        final List<String> indexNames = IndexDiscoveryHelper.buildIndexNamesInDateRange(indexPattern, effectiveStart, finalEnd);

        if (indexNames.isEmpty()) {
            listener.onResponse(Collections.emptyList());
            return;
        }

        executeSearchRequest(indexNames, start, finalEnd, id, verbose, metricType, username, backendRoles, listener);
    }

    /**
     * Executes the search request against the discovered indices.
     */
    private void executeSearchRequest(
        final List<String> indexNames,
        final ZonedDateTime start,
        final ZonedDateTime end,
        final String id,
        final Boolean verbose,
        final MetricType metricType,
        final String username,
        final List<String> backendRoles,
        final ActionListener<List<SearchQueryRecord>> listener
    ) {
        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(
            indexNames,
            start,
            end,
            id,
            verbose,
            metricType,
            username,
            backendRoles
        );

        client.search(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                SearchResponseParser.parseSearchResponse(searchResponse, namedXContentRegistry, listener);
            }

            @Override
            public void onFailure(Exception e) {
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_READER_SEARCH_EXCEPTIONS);
                logger.error("Failed to search indices {}: ", indexNames, e);
                listener.onFailure(e);
            }
        });
    }

    /**
     * Close the reader sink
     */
    @Override
    public void close() {
        logger.debug("Closing the LocalIndexReader..");
    }
}
