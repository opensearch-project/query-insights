/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.reader;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

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

    /**
     * Constructor of LocalIndexReader
     *
     * @param client OS client
     * @param indexPattern the pattern of index to read from
     * @param namedXContentRegistry for parsing purposes
     */
    public LocalIndexReader(final Client client, final DateTimeFormatter indexPattern, final NamedXContentRegistry namedXContentRegistry) {
        this.indexPattern = indexPattern;
        this.client = client;
        this.namedXContentRegistry = namedXContentRegistry;
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
     * @param from start timestamp
     * @param to end timestamp
     * @return list of SearchQueryRecords whose timestamps fall between from and to
     */
    @Override
    public List<SearchQueryRecord> read(final String from, final String to) {
        List<SearchQueryRecord> records = new ArrayList<>();
        if (from == null || to == null) {
            return records;
        }
        final DateTime start = DateTime.parse(from);
        DateTime end = DateTime.parse(to);
        if (end.compareTo(DateTime.now(DateTimeZone.UTC)) > 0) {
            end = DateTime.now(DateTimeZone.UTC);
        }
        DateTime curr = start;
        while (curr.compareTo(end.plusDays(1).withTimeAtStartOfDay()) < 0) {
            String index = getDateTimeFromFormat(curr);
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            MatchQueryBuilder excludeQuery = QueryBuilders.matchQuery("indices", "top_queries*");
            RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("timestamp").from(start.getMillis()).to(end.getMillis());
            QueryBuilder query = QueryBuilders.boolQuery().must(rangeQuery).mustNot(excludeQuery);
            searchSourceBuilder.query(query);
            searchRequest.source(searchSourceBuilder);
            try {
                SearchResponse searchResponse = client.search(searchRequest).actionGet();
                for (SearchHit hit : searchResponse.getHits()) {
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.getSourceAsString());
                    SearchQueryRecord record = SearchQueryRecord.fromXContent(parser);
                    records.add(record);
                }
            } catch (IndexNotFoundException ignored) {} catch (Exception e) {
                logger.error("Unable to parse search hit: ", e);
            }
            curr = curr.plusDays(1);

        }
        return records;
    }

    /**
     * Close the reader sink
     */
    @Override
    public void close() {
        logger.debug("Closing the LocalIndexReader..");
    }

    private String getDateTimeFromFormat(DateTime current) {
        return indexPattern.print(current);
    }
}
