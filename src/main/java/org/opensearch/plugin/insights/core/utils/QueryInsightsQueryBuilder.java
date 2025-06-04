/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.utils;

import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.TOP_N_QUERY;
import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.VERBOSE_ONLY_FIELDS;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

/**
 * Utility class for building search queries for Query Insights operations.
 * This class provides methods to construct search requests for TopN queries
 * with appropriate filters, sorting, and field exclusions.
 */
public final class QueryInsightsQueryBuilder {

    private static final int MAX_TOP_N_INDEX_READ_SIZE = 50;

    private QueryInsightsQueryBuilder() {
        // Utility class - prevent instantiation
    }

    /**
     * Builds a search request for TopN queries with the specified parameters.
     *
     * @param indexNames List of index names to search
     * @param start Start timestamp for the query range
     * @param end End timestamp for the query range
     * @param id Optional query/group id to filter by
     * @param verbose Whether to return full output (if false, excludes verbose-only fields)
     * @param metricType Metric type to filter by
     * @return Configured SearchRequest ready for execution
     */
    public static SearchRequest buildTopNSearchRequest(
        final List<String> indexNames,
        final ZonedDateTime start,
        final ZonedDateTime end,
        final String id,
        final Boolean verbose,
        final MetricType metricType
    ) {
        SearchRequest searchRequest = new SearchRequest(indexNames.toArray(new String[0]));

        // Configure indices options to ignore unavailable indices and allow no indices
        searchRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, false));

        // Build the search source
        SearchSourceBuilder searchSourceBuilder = buildSearchSourceBuilder(start, end, id, verbose, metricType);
        searchRequest.source(searchSourceBuilder);

        return searchRequest;
    }

    /**
     * Builds a SearchSourceBuilder with the appropriate query, sorting, and field exclusions.
     *
     * @param start Start timestamp for the query range
     * @param end End timestamp for the query range
     * @param id Optional query/group id to filter by
     * @param verbose Whether to return full output
     * @param metricType Metric type to filter by
     * @return Configured SearchSourceBuilder
     */
    private static SearchSourceBuilder buildSearchSourceBuilder(
        final ZonedDateTime start,
        final ZonedDateTime end,
        final String id,
        final Boolean verbose,
        final MetricType metricType
    ) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(MAX_TOP_N_INDEX_READ_SIZE);

        // Build the main query
        BoolQueryBuilder query = buildTopNQuery(start, end, id, metricType);
        searchSourceBuilder.query(query);

        // Configure field exclusions for non-verbose mode
        if (Boolean.FALSE.equals(verbose)) {
            searchSourceBuilder.fetchSource(
                Strings.EMPTY_ARRAY,
                Arrays.stream(VERBOSE_ONLY_FIELDS).map(Attribute::toString).toArray(String[]::new)
            );
        }

        // Add sorting by latency in descending order
        searchSourceBuilder.sort(SortBuilders.fieldSort("measurements.latency.number").order(SortOrder.DESC));

        return searchSourceBuilder;
    }

    /**
     * Builds the main boolean query for TopN search operations.
     *
     * @param start Start timestamp for the query range
     * @param end End timestamp for the query range
     * @param id Optional query/group id to filter by
     * @param metricType Metric type to filter by
     * @return Configured BoolQueryBuilder
     */
    private static BoolQueryBuilder buildTopNQuery(
        final ZonedDateTime start,
        final ZonedDateTime end,
        final String id,
        final MetricType metricType
    ) {
        // Build exclude query for top_queries* indices
        MatchQueryBuilder excludeQuery = QueryBuilders.matchQuery("indices", "top_queries*");

        // Build range query for timestamp
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("timestamp")
            .from(start.toInstant().toEpochMilli())
            .to(end.toInstant().toEpochMilli());

        // Build the main boolean query
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(rangeQuery).mustNot(excludeQuery);

        // Add ID-specific or metric-type-specific filter
        if (id != null) {
            query.must(QueryBuilders.matchQuery("id", id));
        } else {
            query.must(QueryBuilders.termQuery(TOP_N_QUERY + "." + metricType, true));
        }

        return query;
    }
}
