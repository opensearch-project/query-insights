/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.utils;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.SearchHit;

/**
 * Utility class for parsing search responses into SearchQueryRecord objects.
 * This class handles the conversion of OpenSearch search hits into structured
 * query insight records with proper error handling and metrics tracking.
 */
public final class SearchResponseParser {

    private static final Logger logger = LogManager.getLogger(SearchResponseParser.class);

    private SearchResponseParser() {
        // Utility class - prevent instantiation
    }

    /**
     * Parses a SearchResponse into a list of SearchQueryRecord objects and invokes
     * the provided callback with the results.
     *
     * @param searchResponse The SearchResponse to parse
     * @param namedXContentRegistry Registry for parsing XContent
     * @param callback Callback to invoke with the parsed records or error
     */
    public static void parseSearchResponse(
        final SearchResponse searchResponse,
        final NamedXContentRegistry namedXContentRegistry,
        final ActionListener<List<SearchQueryRecord>> callback
    ) {
        try {
            List<SearchQueryRecord> records = parseSearchHits(searchResponse, namedXContentRegistry);
            callback.onResponse(records);
        } catch (Exception e) {
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_READER_PARSING_EXCEPTIONS);
            logger.error("Unable to parse search response during multi-index search: ", e);
            callback.onFailure(e);
        }
    }

    /**
     * Parses search hits from a SearchResponse into SearchQueryRecord objects.
     *
     * @param searchResponse The SearchResponse containing the hits to parse
     * @param namedXContentRegistry Registry for parsing XContent
     * @return List of parsed SearchQueryRecord objects
     * @throws Exception if parsing fails
     */
    private static List<SearchQueryRecord> parseSearchHits(
        final SearchResponse searchResponse,
        final NamedXContentRegistry namedXContentRegistry
    ) throws Exception {
        List<SearchQueryRecord> records = new ArrayList<>();

        for (SearchHit hit : searchResponse.getHits()) {
            SearchQueryRecord record = parseSearchHit(hit, namedXContentRegistry);
            records.add(record);
        }

        return records;
    }

    /**
     * Parses a single SearchHit into a SearchQueryRecord object.
     *
     * @param hit The SearchHit to parse
     * @param namedXContentRegistry Registry for parsing XContent
     * @return Parsed SearchQueryRecord object
     * @throws Exception if parsing fails
     */
    private static SearchQueryRecord parseSearchHit(final SearchHit hit, final NamedXContentRegistry namedXContentRegistry)
        throws Exception {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.getSourceAsString())
        ) {
            return SearchQueryRecord.fromXContent(parser);
        }
    }
}
