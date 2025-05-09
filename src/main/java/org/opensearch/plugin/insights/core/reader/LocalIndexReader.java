/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.reader;

import static org.opensearch.plugin.insights.core.utils.ExporterReaderUtils.generateLocalIndexDateHash;
import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.TOP_N_QUERY;
import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.VERBOSE_ONLY_FIELDS;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_QUERIES_INDEX_PATTERN_GLOB;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.Client;
import reactor.util.annotation.NonNull;

/**
 * Local index reader for reading query insights data from local OpenSearch indices.
 */
public final class LocalIndexReader implements QueryInsightsReader {
    private final static int MAX_TOP_N_INDEX_READ_SIZE = 50;
    /**
     * Logger of the local index reader
     */
    private final Logger logger = LogManager.getLogger();
    private final Client client;
    private DateTimeFormatter indexPattern;
    private final NamedXContentRegistry namedXContentRegistry;
    private final String id;

    /**
     * Constructor of LocalIndexReader
     *
     * @param client OS client
     * @param indexPattern the pattern of index to read from
     * @param namedXContentRegistry for parsing purposes
     */
    public LocalIndexReader(
        final Client client,
        final DateTimeFormatter indexPattern,
        final NamedXContentRegistry namedXContentRegistry,
        final String id
    ) {
        this.indexPattern = indexPattern;
        this.client = client;
        this.id = id;
        this.namedXContentRegistry = namedXContentRegistry;
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
        final List<SearchQueryRecord> records = new ArrayList<>();
        if (from == null || to == null) {
            listener.onResponse(records);
            return;
        }
        final ZonedDateTime start = ZonedDateTime.parse(from);
        ZonedDateTime end = ZonedDateTime.parse(to);
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        if (end.isAfter(now)) {
            end = now;
        }
        final ZonedDateTime finalEnd = end;

        ClusterStateRequest clusterStateRequest = new ClusterStateRequest().clear()
            .indices(TOP_QUERIES_INDEX_PATTERN_GLOB)
            .metadata(true)
            .local(true)
            .indicesOptions(IndicesOptions.lenientExpandOpen()); // Allow no indices and ignore unavailable

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(ClusterStateResponse clusterStateResponse) {
                Set<String> existingIndices = clusterStateResponse.getState().metadata().indices().keySet();
                List<String> indexNames = new ArrayList<>();
                ZonedDateTime currentDay = start;

                // Iterate through each day in the range [start, finalEnd] and add only existing indices
                while (!currentDay.isAfter(finalEnd)) {
                    String potentialIndexName = buildLocalIndexName(currentDay);
                    if (existingIndices.contains(potentialIndexName)) {
                        indexNames.add(potentialIndexName);
                    }
                    currentDay = currentDay.plusDays(1);
                }

                if (indexNames.isEmpty()) {
                    listener.onResponse(Collections.emptyList());
                    return;
                }

                SearchRequest searchRequest = new SearchRequest(indexNames.toArray(new String[0]));
                // Ignore unavailable indices and allow no indices, similar to previous behavior of skipping days
                searchRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, false));

                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(MAX_TOP_N_INDEX_READ_SIZE);
                MatchQueryBuilder excludeQuery = QueryBuilders.matchQuery("indices", "top_queries*");
                RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("timestamp")
                    .from(start.toInstant().toEpochMilli())
                    .to(finalEnd.toInstant().toEpochMilli());
                BoolQueryBuilder query = QueryBuilders.boolQuery().must(rangeQuery).mustNot(excludeQuery);

                if (id != null) {
                    query.must(QueryBuilders.matchQuery("id", id));
                } else {
                    query.must(QueryBuilders.termQuery(TOP_N_QUERY + "." + metricType, true));
                }
                searchSourceBuilder.query(query);

                if (Boolean.FALSE.equals(verbose)) {
                    searchSourceBuilder.fetchSource(
                        Strings.EMPTY_ARRAY,
                        Arrays.stream(VERBOSE_ONLY_FIELDS).map(Attribute::toString).toArray(String[]::new)
                    );
                }
                searchSourceBuilder.sort(SortBuilders.fieldSort("measurements.latency.number").order(SortOrder.DESC));
                searchRequest.source(searchSourceBuilder);

                client.search(searchRequest, new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        try {
                            for (SearchHit hit : searchResponse.getHits()) {
                                XContentParser parser = XContentType.JSON.xContent()
                                    .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.getSourceAsString());
                                SearchQueryRecord record = SearchQueryRecord.fromXContent(parser);
                                records.add(record);
                            }
                            listener.onResponse(records);
                        } catch (Exception e) {
                            OperationalMetricsCounter.getInstance()
                                .incrementCounter(OperationalMetric.LOCAL_INDEX_READER_PARSING_EXCEPTIONS);
                            logger.error("Unable to parse search hit during multi-index search: ", e);
                            listener.onFailure(e); // Fail the entire operation if parsing fails
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_READER_SEARCH_EXCEPTIONS);
                        final List<String> finalIndexNames = new ArrayList<>(indexNames);
                        logger.error("Failed to fetch historical top queries for indices " + String.join(",", finalIndexNames), e);
                        listener.onFailure(e);
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_READER_SEARCH_EXCEPTIONS);
                logger.error("Failed to get cluster state for indices matching {}: ", TOP_QUERIES_INDEX_PATTERN_GLOB, e);
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

    private String buildLocalIndexName(ZonedDateTime current) {
        return current.format(indexPattern) + "-" + generateLocalIndexDateHash(current.toLocalDate());
    }
}
