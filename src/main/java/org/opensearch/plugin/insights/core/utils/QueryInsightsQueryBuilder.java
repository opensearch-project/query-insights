/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.utils;

import static org.opensearch.plugin.insights.rules.model.Measurement.NUMBER;
import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.ID;
import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.MEASUREMENTS;
import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.TIMESTAMP;
import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.TOP_N_QUERY;
import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.VERBOSE_ONLY_FIELDS;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_SEARCH_REQUEST_TIMEOUT;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import reactor.util.annotation.NonNull;

/**
 * Utility class for building search queries for Query Insights operations.
 * This class provides methods to construct search requests for TopN queries
 * with appropriate filters, sorting, and field exclusions.
 */
public final class QueryInsightsQueryBuilder {

    private static final int MAX_TOP_N_INDEX_READ_SIZE = 500;

    private QueryInsightsQueryBuilder() {}

    /**
     * Builds a search request for TopN queries with the specified parameters.
     *
     * <p>This method constructs a search request that queries query insights indices
     * to retrieve the top N queries based on the specified metric type and time range.
     * The request includes appropriate caching, timeout, and indices options for optimal performance.
     *
     * <p><strong>Example JSON representation of the generated search request:</strong>
     * <pre>{@code
     * {
     *   "size": 50,
     *   "timeout": "30s",
     *   "query": {
     *     "bool": {
     *       "must": [
     *         {
     *           "range": {
     *             "timestamp": {
     *               "from": 1640995200000,
     *               "to": 1641081600000
     *             }
     *           }
     *         },
     *         {
     *           "term": {
     *             "top_n_query.latency": true
     *           }
     *         }
     *       ]
     *     }
     *   },
     *   "sort": [
     *     {
     *       "measurements.latency.number": {
     *         "order": "desc"
     *       }
     *     }
     *   ],
     *   "_source": {
     *     "excludes": ["verbose_field1", "verbose_field2"]
     *   }
     * }
     * }</pre>
     *
     * @param indexNames List of index names to search
     * @param start Start timestamp for the query range
     * @param end End timestamp for the query range
     * @param id Optional query/group id to filter by
     * @param verbose Whether to return full output (if false, excludes verbose-only fields)
     * @param metricType Metric type to filter by
     * @return Configured SearchRequest ready for execution with timeout, caching, and indices options
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

        searchRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, false));

        // Enable request caching for better performance on repeated queries
        searchRequest.requestCache(true);

        SearchSourceBuilder searchSourceBuilder = buildSearchSourceBuilder(start, end, id, verbose, metricType);

        // Set timeout to prevent long-running queries from blocking resources
        searchSourceBuilder.timeout(DEFAULT_SEARCH_REQUEST_TIMEOUT);

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
        @NonNull final MetricType metricType
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

        // Add sorting by requested metric type in descending order
        searchSourceBuilder.sort(SortBuilders.fieldSort(MEASUREMENTS + "." + metricType + "." + NUMBER).order(SortOrder.DESC));

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
        // Build range query for timestamp
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(TIMESTAMP)
            .from(start.toInstant().toEpochMilli())
            .to(end.toInstant().toEpochMilli());

        // Build the main boolean query
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(rangeQuery);

        // Add ID-specific or metric-type-specific filter
        if (id != null) {
            query.must(QueryBuilders.termQuery(ID, id));
        } else {
            query.must(QueryBuilders.termQuery(TOP_N_QUERY + "." + metricType, true));
        }

        return query;
    }
}
