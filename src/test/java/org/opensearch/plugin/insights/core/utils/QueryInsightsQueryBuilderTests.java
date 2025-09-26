/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.utils;

import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.TOP_N_QUERY;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link QueryInsightsQueryBuilder}.
 */
public class QueryInsightsQueryBuilderTests extends OpenSearchTestCase {

    public void testBasicBuildTopNSearchRequest() {
        List<String> indexNames = Arrays.asList("index1", "index2");
        ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        String id = "test-id";
        Boolean verbose = true;
        MetricType metricType = MetricType.LATENCY;

        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(indexNames, start, end, id, verbose, metricType);

        assertNotNull(searchRequest);
        assertEquals(2, searchRequest.indices().length);
        assertEquals("index1", searchRequest.indices()[0]);
        assertEquals("index2", searchRequest.indices()[1]);

        SearchSourceBuilder sourceBuilder = searchRequest.source();
        assertNotNull(sourceBuilder);
        assertNotNull(sourceBuilder.query());
    }

    public void testBuildTopNSearchRequestWithNullId() {
        List<String> indexNames = Arrays.asList("index1");
        ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        String id = null;
        Boolean verbose = false;
        MetricType metricType = MetricType.CPU;

        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(indexNames, start, end, id, verbose, metricType);

        assertNotNull(searchRequest);
        assertEquals(1, searchRequest.indices().length);
        assertEquals("index1", searchRequest.indices()[0]);

        SearchSourceBuilder sourceBuilder = searchRequest.source();
        assertNotNull(sourceBuilder);
        assertNotNull(sourceBuilder.query());

        // When verbose is false, should have field exclusions
        assertNotNull(sourceBuilder.fetchSource());
    }

    public void testBuildTopNSearchRequestWithVerboseFalse() {
        List<String> indexNames = Arrays.asList("index1");
        ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        String id = "test-id";
        Boolean verbose = false;
        MetricType metricType = MetricType.MEMORY;

        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(indexNames, start, end, id, verbose, metricType);

        assertNotNull(searchRequest);
        SearchSourceBuilder sourceBuilder = searchRequest.source();
        assertNotNull(sourceBuilder);

        // When verbose is false, should have field exclusions configured
        assertNotNull(sourceBuilder.fetchSource());
    }

    public void testBuildTopNSearchRequestWithMultipleIndices() {
        List<String> indexNames = Arrays.asList("index1", "index2", "index3");
        ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC).minusHours(2);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        String id = null;
        Boolean verbose = true;
        MetricType metricType = MetricType.LATENCY;

        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(indexNames, start, end, id, verbose, metricType);

        assertNotNull(searchRequest);
        assertEquals(3, searchRequest.indices().length);
        assertTrue(Arrays.asList(searchRequest.indices()).contains("index1"));
        assertTrue(Arrays.asList(searchRequest.indices()).contains("index2"));
        assertTrue(Arrays.asList(searchRequest.indices()).contains("index3"));

        // Verify indices options are set correctly
        assertNotNull(searchRequest.indicesOptions());
        assertTrue(searchRequest.indicesOptions().allowNoIndices());
        assertTrue(searchRequest.indicesOptions().ignoreUnavailable());
    }

    public void testBuildTopNSearchRequestWithEmptyIndexList() {
        List<String> indexNames = Arrays.asList();
        ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        String id = "test-id";
        Boolean verbose = true;
        MetricType metricType = MetricType.CPU;

        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(indexNames, start, end, id, verbose, metricType);

        assertNotNull(searchRequest);
        assertEquals(0, searchRequest.indices().length);
    }

    public void testBuildTopNSearchRequestWithDifferentMetricTypes() {
        List<String> indexNames = Arrays.asList("test-index");
        ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        String id = null; // Important: id must be null to trigger metric type filtering
        Boolean verbose = true;

        for (MetricType metricType : MetricType.allMetricTypes()) {
            SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(indexNames, start, end, id, verbose, metricType);

            assertNotNull(searchRequest);
            assertNotNull(searchRequest.source());
            assertNotNull(searchRequest.source().query());

            // Verify the metric type is correctly used in the query
            SearchSourceBuilder sourceBuilder = searchRequest.source();
            QueryBuilder query = sourceBuilder.query();
            assertTrue("Query should be a BoolQueryBuilder", query instanceof BoolQueryBuilder);

            BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
            List<QueryBuilder> mustClauses = boolQuery.must();

            // Find the term query for the metric type
            boolean foundMetricTypeQuery = false;
            String expectedField = TOP_N_QUERY + "." + metricType.toString();

            for (QueryBuilder mustClause : mustClauses) {
                if (mustClause instanceof TermQueryBuilder) {
                    TermQueryBuilder termQuery = (TermQueryBuilder) mustClause;
                    if (expectedField.equals(termQuery.fieldName())) {
                        assertEquals("Term query value should be true", true, termQuery.value());
                        foundMetricTypeQuery = true;
                        break;
                    }
                }
            }

            assertTrue("Should find term query for metric type: " + expectedField, foundMetricTypeQuery);
        }
    }

    public void testBuildTopNSearchRequestWithIdShouldNotUseMetricTypeFilter() {
        List<String> indexNames = Arrays.asList("test-index");
        ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        String id = "specific-query-id"; // When ID is provided, metric type filter should NOT be used
        Boolean verbose = true;
        MetricType metricType = MetricType.LATENCY;

        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(indexNames, start, end, id, verbose, metricType);

        assertNotNull(searchRequest);
        SearchSourceBuilder sourceBuilder = searchRequest.source();
        QueryBuilder query = sourceBuilder.query();
        assertTrue("Query should be a BoolQueryBuilder", query instanceof BoolQueryBuilder);

        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
        List<QueryBuilder> mustClauses = boolQuery.must();

        // Should NOT find metric type query when ID is provided
        String metricTypeField = TOP_N_QUERY + "." + metricType.toString();
        boolean foundMetricTypeQuery = false;
        boolean foundIdQuery = false;

        for (QueryBuilder mustClause : mustClauses) {
            if (mustClause instanceof TermQueryBuilder) {
                TermQueryBuilder termQuery = (TermQueryBuilder) mustClause;
                if (metricTypeField.equals(termQuery.fieldName())) {
                    foundMetricTypeQuery = true;
                }
            }
            // Check for ID query (could be MatchQueryBuilder)
            if (mustClause.toString().contains("id") && mustClause.toString().contains(id)) {
                foundIdQuery = true;
            }
        }

        assertFalse("Should NOT find metric type query when ID is provided", foundMetricTypeQuery);
        assertTrue("Should find ID query when ID is provided", foundIdQuery);
    }

    public void testBuildTopNSearchRequestWithTimeRange() {
        List<String> indexNames = Arrays.asList("test-index");
        ZonedDateTime start = ZonedDateTime.of(2023, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime end = ZonedDateTime.of(2023, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC);
        String id = null;
        Boolean verbose = true;
        MetricType metricType = MetricType.LATENCY;

        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(indexNames, start, end, id, verbose, metricType);

        assertNotNull(searchRequest);
        SearchSourceBuilder sourceBuilder = searchRequest.source();
        assertNotNull(sourceBuilder);
        assertNotNull(sourceBuilder.query());

        // Verify the time range is correctly set in the query
        QueryBuilder query = sourceBuilder.query();
        assertTrue("Query should be a BoolQueryBuilder", query instanceof BoolQueryBuilder);

        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
        List<QueryBuilder> mustClauses = boolQuery.must();

        // Find the range query for timestamp
        boolean foundRangeQuery = false;
        long expectedFromMillis = start.toInstant().toEpochMilli();
        long expectedToMillis = end.toInstant().toEpochMilli();

        for (QueryBuilder mustClause : mustClauses) {
            if (mustClause instanceof RangeQueryBuilder) {
                RangeQueryBuilder rangeQuery = (RangeQueryBuilder) mustClause;
                if ("timestamp".equals(rangeQuery.fieldName())) {
                    // Verify the from and to values
                    assertEquals("Range query 'from' value should match start time", expectedFromMillis, rangeQuery.from());
                    assertEquals("Range query 'to' value should match end time", expectedToMillis, rangeQuery.to());

                    // Verify the range query includes both bounds (inclusive)
                    assertTrue("Range query should include lower bound", rangeQuery.includeLower());
                    assertTrue("Range query should include upper bound", rangeQuery.includeUpper());

                    foundRangeQuery = true;
                    break;
                }
            }
        }

        assertTrue("Should find range query for timestamp field", foundRangeQuery);

        // Verify sorting is configured
        assertNotNull(sourceBuilder.sorts());
        assertFalse(sourceBuilder.sorts().isEmpty());
    }

    public void testBuildTopNSearchRequestWithDifferentTimeZones() {
        List<String> indexNames = Arrays.asList("test-index");
        // Test with different time zones to ensure proper conversion to epoch millis
        ZonedDateTime startUTC = ZonedDateTime.of(2023, 6, 15, 14, 30, 0, 0, ZoneOffset.UTC);
        ZonedDateTime startEST = startUTC.withZoneSameInstant(ZoneOffset.ofHours(-5)); // EST
        ZonedDateTime endUTC = ZonedDateTime.of(2023, 6, 15, 15, 30, 0, 0, ZoneOffset.UTC);
        ZonedDateTime endPST = endUTC.withZoneSameInstant(ZoneOffset.ofHours(-8)); // PST

        String id = null;
        Boolean verbose = true;
        MetricType metricType = MetricType.CPU;

        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(
            indexNames,
            startEST, // Different timezone for start
            endPST,   // Different timezone for end
            id,
            verbose,
            metricType
        );

        assertNotNull(searchRequest);
        SearchSourceBuilder sourceBuilder = searchRequest.source();
        QueryBuilder query = sourceBuilder.query();
        assertTrue("Query should be a BoolQueryBuilder", query instanceof BoolQueryBuilder);

        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
        List<QueryBuilder> mustClauses = boolQuery.must();

        // Find the range query and verify epoch millis conversion
        boolean foundRangeQuery = false;
        long expectedFromMillis = startEST.toInstant().toEpochMilli();
        long expectedToMillis = endPST.toInstant().toEpochMilli();

        for (QueryBuilder mustClause : mustClauses) {
            if (mustClause instanceof RangeQueryBuilder) {
                RangeQueryBuilder rangeQuery = (RangeQueryBuilder) mustClause;
                if ("timestamp".equals(rangeQuery.fieldName())) {
                    assertEquals("Range query should convert timezone correctly for 'from'", expectedFromMillis, rangeQuery.from());
                    assertEquals("Range query should convert timezone correctly for 'to'", expectedToMillis, rangeQuery.to());

                    // Verify the time range makes sense (end should be after start)
                    assertTrue("End time should be after start time", (Long) rangeQuery.to() > (Long) rangeQuery.from());

                    foundRangeQuery = true;
                    break;
                }
            }
        }

        assertTrue("Should find range query for timestamp field", foundRangeQuery);
    }

    public void testBuildTopNSearchRequestWithSameStartAndEndTime() {
        List<String> indexNames = Arrays.asList("test-index");
        ZonedDateTime sameTime = ZonedDateTime.of(2023, 12, 25, 12, 0, 0, 0, ZoneOffset.UTC);
        String id = null;
        Boolean verbose = true;
        MetricType metricType = MetricType.MEMORY;

        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(
            indexNames,
            sameTime, // Same time for both start and end
            sameTime,
            id,
            verbose,
            metricType
        );

        assertNotNull(searchRequest);
        SearchSourceBuilder sourceBuilder = searchRequest.source();
        QueryBuilder query = sourceBuilder.query();
        assertTrue("Query should be a BoolQueryBuilder", query instanceof BoolQueryBuilder);

        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
        List<QueryBuilder> mustClauses = boolQuery.must();

        // Find the range query and verify same time handling
        boolean foundRangeQuery = false;
        long expectedMillis = sameTime.toInstant().toEpochMilli();

        for (QueryBuilder mustClause : mustClauses) {
            if (mustClause instanceof RangeQueryBuilder) {
                RangeQueryBuilder rangeQuery = (RangeQueryBuilder) mustClause;
                if ("timestamp".equals(rangeQuery.fieldName())) {
                    assertEquals("Range query 'from' should equal the same time", expectedMillis, rangeQuery.from());
                    assertEquals("Range query 'to' should equal the same time", expectedMillis, rangeQuery.to());
                    assertEquals("From and to should be equal for same time", rangeQuery.from(), rangeQuery.to());

                    foundRangeQuery = true;
                    break;
                }
            }
        }

        assertTrue("Should find range query for timestamp field", foundRangeQuery);
    }

    public void testBuildTopNSearchRequestWithCachingEnabled() {
        List<String> indexNames = Arrays.asList("test-index");
        ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        String id = null;
        Boolean verbose = true;
        MetricType metricType = MetricType.LATENCY;

        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(indexNames, start, end, id, verbose, metricType);

        assertNotNull(searchRequest);

        // Verify request caching is enabled
        assertTrue("Request caching should be enabled for better performance on repeated queries", searchRequest.requestCache());
    }

    public void testBuildTopNSearchRequestWithTimeout() {
        List<String> indexNames = Arrays.asList("test-index");
        ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        String id = null;
        Boolean verbose = true;
        MetricType metricType = MetricType.CPU;

        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(indexNames, start, end, id, verbose, metricType);

        assertNotNull(searchRequest);
        SearchSourceBuilder sourceBuilder = searchRequest.source();
        assertNotNull(sourceBuilder);

        // Verify timeout is set
        assertNotNull("Timeout should be configured to prevent long-running queries", sourceBuilder.timeout());
        assertEquals(
            "Timeout should be set to the default value",
            QueryInsightsSettings.DEFAULT_SEARCH_REQUEST_TIMEOUT,
            sourceBuilder.timeout()
        );
    }

    public void testBuildTopNSearchRequestIndicesOptions() {
        List<String> indexNames = Arrays.asList("test-index", "non-existent-index");
        ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        String id = null;
        Boolean verbose = true;
        MetricType metricType = MetricType.MEMORY;

        SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(indexNames, start, end, id, verbose, metricType);

        assertNotNull(searchRequest);

        // Verify indices options are configured correctly
        assertNotNull("Indices options should be configured", searchRequest.indicesOptions());
        assertTrue("Should ignore unavailable indices", searchRequest.indicesOptions().ignoreUnavailable());
        assertTrue("Should allow no indices", searchRequest.indicesOptions().allowNoIndices());
        assertTrue("Should expand to open indices", searchRequest.indicesOptions().expandWildcardsOpen());
        assertFalse("Should not expand to closed indices", searchRequest.indicesOptions().expandWildcardsClosed());
    }

    public void testIdQueryMatch() {
        List<String> indexNames = Arrays.asList("test-index");
        ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        Boolean verbose = true;
        MetricType metricType = MetricType.LATENCY;

        String[] testIds = {
            "a7f3b2c8-5291-7634-9c4e-def123cd4589",
            "b8e4c3d9-5291-8745-1a2b-abc456ef7890",
            "c9f5d4ea-7634-5291-3c4d-fed789ab1234" };

        for (String id : testIds) {
            SearchRequest searchRequest = QueryInsightsQueryBuilder.buildTopNSearchRequest(indexNames, start, end, id, verbose, metricType);

            SearchSourceBuilder sourceBuilder = searchRequest.source();
            BoolQueryBuilder boolQuery = (BoolQueryBuilder) sourceBuilder.query();
            List<QueryBuilder> mustClauses = boolQuery.must();

            boolean foundExactIdMatch = false;
            for (QueryBuilder mustClause : mustClauses) {
                if (mustClause instanceof TermQueryBuilder) {
                    TermQueryBuilder termQuery = (TermQueryBuilder) mustClause;
                    if ("id".equals(termQuery.fieldName()) && id.equals(termQuery.value())) {
                        foundExactIdMatch = true;
                        break;
                    }
                }
            }

            assertTrue("Query should match the exact ID without tokenization: " + id, foundExactIdMatch);
        }
    }
}
