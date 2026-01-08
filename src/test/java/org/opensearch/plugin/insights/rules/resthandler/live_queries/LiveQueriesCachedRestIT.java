/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.live_queries;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/**
 * Integration tests for cached parameter in Live Queries API
 */
public class LiveQueriesCachedRestIT extends QueryInsightsRestTestCase {

    /**
     * Test use_live_cache parameter functionality
     */
    @SuppressWarnings("unchecked")
    public void testCachedParameter() throws IOException {
        // Test use_live_cache=true
        Request cachedRequest = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?use_live_cache=true");
        Response cachedResponse = client().performRequest(cachedRequest);
        assertEquals(200, cachedResponse.getStatusLine().getStatusCode());

        Map<String, Object> cachedMap = entityAsMap(cachedResponse);
        assertTrue("Response should contain live_queries", cachedMap.containsKey("live_queries"));
        List<Map<String, Object>> cachedQueries = (List<Map<String, Object>>) cachedMap.get("live_queries");
        assertNotNull("Live queries list should not be null", cachedQueries);

        // Test use_live_cache=false (default behavior)
        Request nonCachedRequest = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?use_live_cache=false");
        Response nonCachedResponse = client().performRequest(nonCachedRequest);
        assertEquals(200, nonCachedResponse.getStatusLine().getStatusCode());

        Map<String, Object> nonCachedMap = entityAsMap(nonCachedResponse);
        assertTrue("Response should contain live_queries", nonCachedMap.containsKey("live_queries"));
        List<Map<String, Object>> nonCachedQueries = (List<Map<String, Object>>) nonCachedMap.get("live_queries");
        assertNotNull("Live queries list should not be null", nonCachedQueries);
    }

    /**
     * Test use_live_cache parameter
     */
    @SuppressWarnings("unchecked")
    public void testCachedAndIncludeFinishedTogether() throws IOException {
        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?use_live_cache=true");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));

        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");
        assertNotNull("Live queries list should not be null", liveQueries);
    }

    /**
     * Test wlmGroupId parameter
     */
    @SuppressWarnings("unchecked")
    public void testWlmGroupIdParameter() throws IOException {
        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?wlmGroupId=DEFAULT_WORKLOAD_GROUP");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));
        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");
        assertNotNull("Live queries list should not be null", liveQueries);
    }

    /**
     * Test all new parameters together
     */
    @SuppressWarnings("unchecked")
    public void testAllNewParametersTogether() throws IOException {
        String params = "?use_live_cache=true&wlmGroupId=DEFAULT_WORKLOAD_GROUP&verbose=true&sort=latency&size=10";
        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + params);
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));

        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");
        assertNotNull("Live queries list should not be null", liveQueries);

        // Verify that the size limit is respected (should be <= 10)
        assertTrue("Live queries should respect size limit", liveQueries.size() <= 10);
    }

    /**
     * Test parameter validation for use_live_cache parameter
     */
    public void testInvalidCachedParameterValidation() throws IOException {
        String[] invalidCachedValues = { "invalid", "1", "yes", "no" };

        for (String invalidValue : invalidCachedValues) {
            Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?use_live_cache=" + invalidValue);
            try {
                client().performRequest(request);
                fail("Should have failed with invalid use_live_cache parameter: " + invalidValue);
            } catch (Exception e) {
                assertTrue(
                    "Error should mention boolean parameter validation",
                    e.getMessage().contains("Failed to parse value") || e.getMessage().contains("only [true] or [false] are allowed")
                );
            }
        }
    }

    /**
     * Test that default values work correctly
     */
    @SuppressWarnings("unchecked")
    public void testDefaultParameterValues() throws IOException {
        // Test with no parameters (should use defaults)
        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI);
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));

        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");
        assertNotNull("Live queries list should not be null", liveQueries);
    }
}
