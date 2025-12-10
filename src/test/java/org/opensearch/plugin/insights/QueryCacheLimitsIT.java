/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/**
 * Integration tests for Live Queries cache limits and parameters
 */
public class QueryCacheLimitsIT extends QueryInsightsRestTestCase {

    private void runSearchQueries(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            Request searchRequest = new Request("GET", "/_search");
            client().performRequest(searchRequest);
        }
    }

    /**
     * Test live queries returns at most 100 queries (MAX_RETURNED_QUERIES)
     */
    @SuppressWarnings("unchecked")
    public void testLiveQueriesCacheLimit() throws Exception {
        runSearchQueries(5);

        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI);
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = entityAsMap(response);

        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));
        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");

        assertTrue("Should return at most 100 queries, got: " + liveQueries.size(), liveQueries.size() <= 100);
    }

    /**
     * Test size parameter limits results correctly
     */
    @SuppressWarnings("unchecked")
    public void testLiveQueriesWithSizeParameter() throws Exception {
        runSearchQueries(15);

        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI);
        request.addParameter("size", "10");
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = entityAsMap(response);

        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));
        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");

        assertTrue("Should return at most 10 queries, got: " + liveQueries.size(), liveQueries.size() <= 10);
    }

    /**
     * Test cached parameter triggers lazy initialization and returns cached results
     */
    @SuppressWarnings("unchecked")
    public void testLiveQueriesWithCachedParameter() throws Exception {
        runSearchQueries(3);

        // First call with cached=true triggers lazy initialization
        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI);
        request.addParameter("cached", "true");
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = entityAsMap(response);

        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));
        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");

        assertTrue("Should return at most 100 queries, got: " + liveQueries.size(), liveQueries.size() <= 100);

        // Second call should use cached data
        Response response2 = client().performRequest(request);
        Map<String, Object> responseMap2 = entityAsMap(response2);
        assertTrue("Second call should also contain live_queries", responseMap2.containsKey("live_queries"));
    }

    /**
     * Test include_finished parameter
     */
    @SuppressWarnings("unchecked")
    public void testLiveQueriesWithIncludeFinished() throws Exception {
        runSearchQueries(3);
        Thread.sleep(100);

        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI);
        request.addParameter("include_finished", "true");
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = entityAsMap(response);

        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));
        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");

        for (Map<String, Object> query : liveQueries) {
            assertTrue("Query should have state field", query.containsKey("state"));
        }
    }

    /**
     * Test sort parameter with different metric types
     */
    @SuppressWarnings("unchecked")
    public void testLiveQueriesWithSortParameter() throws Exception {
        runSearchQueries(3);

        String[] sortMetrics = { "latency", "cpu", "memory" };

        for (String metric : sortMetrics) {
            Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI);
            request.addParameter("sort", metric);
            Response response = client().performRequest(request);
            Map<String, Object> responseMap = entityAsMap(response);

            assertTrue("Response should contain live_queries for sort=" + metric, responseMap.containsKey("live_queries"));
        }
    }

    /**
     * Test verbose parameter
     */
    @SuppressWarnings("unchecked")
    public void testLiveQueriesWithVerboseParameter() throws Exception {
        runSearchQueries(3);

        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI);
        request.addParameter("verbose", "false");
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = entityAsMap(response);

        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));
        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");

        // When verbose=false, queries should not have description field
        for (Map<String, Object> query : liveQueries) {
            assertFalse("Query should not have description when verbose=false", query.containsKey("description"));
        }
    }

    /**
     * Test combined parameters
     */
    @SuppressWarnings("unchecked")
    public void testLiveQueriesWithMultipleParameters() throws Exception {
        runSearchQueries(10);

        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI);
        request.addParameter("cached", "true");
        request.addParameter("size", "5");
        request.addParameter("sort", "cpu");
        request.addParameter("verbose", "true");
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = entityAsMap(response);

        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));
        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");

        assertTrue("Should return at most 5 queries, got: " + liveQueries.size(), liveQueries.size() <= 5);
    }
}
