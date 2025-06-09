/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.utils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link SearchResponseParser}.
 */
public class SearchResponseParserTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Initialize OperationalMetricsCounter for tests
        MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );
        OperationalMetricsCounter.initialize("test-cluster", metricsRegistry);
    }

    public void testParseSearchResponseWithValidHits() throws InterruptedException {
        // Create test data using the same pattern as LocalIndexReaderTests
        List<SearchQueryRecord> testRecords = QueryInsightsTestUtils.generateQueryInsightRecords(3);
        SearchResponse searchResponse = createSearchResponseFromRecords(testRecords);

        // Test parsing
        AtomicReference<List<SearchQueryRecord>> parsedRecordsRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        SearchResponseParser.parseSearchResponse(searchResponse, NamedXContentRegistry.EMPTY, ActionListener.wrap(records -> {
            parsedRecordsRef.set(records);
            latch.countDown();
        }, exception -> {
            exceptionRef.set(exception);
            latch.countDown();
        }));

        assertTrue("Parsing should complete within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNull("No exception should be thrown", exceptionRef.get());

        List<SearchQueryRecord> parsedRecords = parsedRecordsRef.get();
        assertNotNull("Parsed records should not be null", parsedRecords);
        assertEquals("Should parse correct number of records", testRecords.size(), parsedRecords.size());

        assertTrue(
            "Parsed records should match original records exactly",
            QueryInsightsTestUtils.checkRecordsEquals(testRecords, parsedRecords)
        );
    }

    public void testParseSearchResponseWithEmptyHits() throws InterruptedException {
        SearchResponse searchResponse = createEmptySearchResponse();

        AtomicReference<List<SearchQueryRecord>> parsedRecordsRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        SearchResponseParser.parseSearchResponse(searchResponse, NamedXContentRegistry.EMPTY, ActionListener.wrap(records -> {
            parsedRecordsRef.set(records);
            latch.countDown();
        }, exception -> {
            exceptionRef.set(exception);
            latch.countDown();
        }));

        assertTrue("Parsing should complete within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNull("No exception should be thrown", exceptionRef.get());

        List<SearchQueryRecord> parsedRecords = parsedRecordsRef.get();
        assertNotNull("Parsed records should not be null", parsedRecords);
        assertEquals("Should return empty list for empty hits", 0, parsedRecords.size());
    }

    public void testParseSearchResponseWithInvalidJson() throws InterruptedException {
        SearchResponse searchResponse = createSearchResponseWithInvalidJson();

        AtomicReference<List<SearchQueryRecord>> parsedRecordsRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        SearchResponseParser.parseSearchResponse(searchResponse, NamedXContentRegistry.EMPTY, ActionListener.wrap(records -> {
            parsedRecordsRef.set(records);
            latch.countDown();
        }, exception -> {
            exceptionRef.set(exception);
            latch.countDown();
        }));

        assertTrue("Parsing should complete within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNotNull("Exception should be thrown for invalid JSON", exceptionRef.get());
        assertNull("No records should be returned when parsing fails", parsedRecordsRef.get());
    }

    public void testParseSearchResponseWithNullResponse() throws InterruptedException {
        AtomicReference<List<SearchQueryRecord>> parsedRecordsRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        SearchResponseParser.parseSearchResponse(null, NamedXContentRegistry.EMPTY, ActionListener.wrap(records -> {
            parsedRecordsRef.set(records);
            latch.countDown();
        }, exception -> {
            exceptionRef.set(exception);
            latch.countDown();
        }));

        assertTrue("Parsing should complete within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNotNull("Exception should be thrown for null response", exceptionRef.get());
        assertNull("No records should be returned when parsing fails", parsedRecordsRef.get());
    }

    public void testParseSearchResponseWithSingleRecord() throws InterruptedException {
        SearchQueryRecord testRecord = QueryInsightsTestUtils.createFixedSearchQueryRecord("test-id-123");
        SearchResponse searchResponse = createSearchResponseFromRecords(List.of(testRecord));

        AtomicReference<List<SearchQueryRecord>> parsedRecordsRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        SearchResponseParser.parseSearchResponse(searchResponse, NamedXContentRegistry.EMPTY, ActionListener.wrap(records -> {
            parsedRecordsRef.set(records);
            latch.countDown();
        }, exception -> {
            exceptionRef.set(exception);
            latch.countDown();
        }));

        assertTrue("Parsing should complete within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNull("No exception should be thrown", exceptionRef.get());

        List<SearchQueryRecord> parsedRecords = parsedRecordsRef.get();
        assertNotNull("Parsed records should not be null", parsedRecords);
        assertEquals("Should parse exactly one record", 1, parsedRecords.size());

        List<SearchQueryRecord> originalRecords = List.of(testRecord);
        assertTrue(
            "Parsed record should match original record exactly",
            QueryInsightsTestUtils.checkRecordsEquals(originalRecords, parsedRecords)
        );
    }

    public void testParseSearchResponseWithLargeNumberOfHits() throws InterruptedException {
        // Test with many records to ensure performance and correctness
        List<SearchQueryRecord> testRecords = QueryInsightsTestUtils.generateQueryInsightRecords(50);
        SearchResponse searchResponse = createSearchResponseFromRecords(testRecords);

        AtomicReference<List<SearchQueryRecord>> parsedRecordsRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        SearchResponseParser.parseSearchResponse(searchResponse, NamedXContentRegistry.EMPTY, ActionListener.wrap(records -> {
            parsedRecordsRef.set(records);
            latch.countDown();
        }, exception -> {
            exceptionRef.set(exception);
            latch.countDown();
        }));

        assertTrue("Parsing should complete within timeout", latch.await(10, TimeUnit.SECONDS));
        assertNull("No exception should be thrown", exceptionRef.get());

        List<SearchQueryRecord> parsedRecords = parsedRecordsRef.get();
        assertNotNull("Parsed records should not be null", parsedRecords);
        assertEquals("Should parse correct number of records", testRecords.size(), parsedRecords.size());

        assertTrue(
            "All parsed records should match original records exactly",
            QueryInsightsTestUtils.checkRecordsEquals(testRecords, parsedRecords)
        );
    }

    // Helper methods to create test SearchResponse objects
    /**
     * Creates a SearchResponse from a list of SearchQueryRecord objects.
     * This follows the same pattern as LocalIndexReaderTests.
     */
    private SearchResponse createSearchResponseFromRecords(List<SearchQueryRecord> records) {
        SearchHit[] hits = new SearchHit[records.size()];

        for (int i = 0; i < records.size(); i++) {
            SearchQueryRecord record = records.get(i);

            // Convert record to JSON source
            BytesReference sourceRef;
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                record.toXContentForExport(builder, null);
                sourceRef = BytesReference.bytes(builder);
            } catch (IOException e) {
                throw new RuntimeException("Failed to build XContent for test record", e);
            }

            // Create SearchHit with the JSON source
            SearchHit hit = new SearchHit(i + 1, record.getId(), Collections.emptyMap(), new HashMap<>());
            hit.sourceRef(sourceRef);
            hits[i] = hit;
        }

        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(searchHits);

        return searchResponse;
    }

    /**
     * Creates an empty SearchResponse with no hits.
     */
    private SearchResponse createEmptySearchResponse() {
        SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(searchHits);

        return searchResponse;
    }

    /**
     * Creates a SearchResponse with invalid JSON to test error handling.
     */
    private SearchResponse createSearchResponseWithInvalidJson() {
        // Create a SearchHit with invalid JSON source
        BytesReference invalidSourceRef = new BytesArray("{invalid json");

        SearchHit hit = new SearchHit(1, "invalid-id", Collections.emptyMap(), new HashMap<>());
        hit.sourceRef(invalidSourceRef);

        SearchHits searchHits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(searchHits);

        return searchResponse;
    }
}
