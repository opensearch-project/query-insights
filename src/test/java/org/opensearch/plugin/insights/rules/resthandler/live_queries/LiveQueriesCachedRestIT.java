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
 * Integration tests for cached and include_finished parameters in Live Queries API
 */
public class LiveQueriesCachedRestIT extends QueryInsightsRestTestCase {

    /**
     * Test cached parameter functionality
     */
    @SuppressWarnings("unchecked")
    public void testCachedParameter() throws IOException {
        // Test cached=true
        Request cachedRequest = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?cached=true");
        Response cachedResponse = client().performRequest(cachedRequest);
        assertEquals(200, cachedResponse.getStatusLine().getStatusCode());

        Map<String, Object> cachedMap = entityAsMap(cachedResponse);
        assertTrue("Response should contain live_queries", cachedMap.containsKey("live_queries"));
        List<Map<String, Object>> cachedQueries = (List<Map<String, Object>>) cachedMap.get("live_queries");
        assertNotNull("Live queries list should not be null", cachedQueries);

        // Test cached=false (default behavior)
        Request nonCachedRequest = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?cached=false");
        Response nonCachedResponse = client().performRequest(nonCachedRequest);
        assertEquals(200, nonCachedResponse.getStatusLine().getStatusCode());

        Map<String, Object> nonCachedMap = entityAsMap(nonCachedResponse);
        assertTrue("Response should contain live_queries", nonCachedMap.containsKey("live_queries"));
        List<Map<String, Object>> nonCachedQueries = (List<Map<String, Object>>) nonCachedMap.get("live_queries");
        assertNotNull("Live queries list should not be null", nonCachedQueries);
    }

    /**
     * Test include_finished parameter functionality
     */
    @SuppressWarnings("unchecked")
    public void testIncludeFinishedParameter() throws IOException {
        // Test include_finished=true
        Request includeFinishedRequest = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?include_finished=true");
        Response includeFinishedResponse = client().performRequest(includeFinishedRequest);
        assertEquals(200, includeFinishedResponse.getStatusLine().getStatusCode());

        Map<String, Object> includeFinishedMap = entityAsMap(includeFinishedResponse);
        assertTrue("Response should contain live_queries", includeFinishedMap.containsKey("live_queries"));
        assertTrue(
            "Response should contain finished_queries when include_finished=true",
            includeFinishedMap.containsKey("finished_queries")
        );

        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) includeFinishedMap.get("live_queries");
        List<Map<String, Object>> finishedQueries = (List<Map<String, Object>>) includeFinishedMap.get("finished_queries");
        assertNotNull("Live queries list should not be null", liveQueries);
        assertNotNull("Finished queries list should not be null", finishedQueries);

        // Test include_finished=false (default behavior)
        Request excludeFinishedRequest = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?include_finished=false");
        Response excludeFinishedResponse = client().performRequest(excludeFinishedRequest);
        assertEquals(200, excludeFinishedResponse.getStatusLine().getStatusCode());

        Map<String, Object> excludeFinishedMap = entityAsMap(excludeFinishedResponse);
        assertTrue("Response should contain live_queries", excludeFinishedMap.containsKey("live_queries"));
        assertFalse(
            "Response should not contain finished_queries when include_finished=false",
            excludeFinishedMap.containsKey("finished_queries")
        );
    }

    /**
     * Test both cached and include_finished parameters together
     */
    @SuppressWarnings("unchecked")
    public void testCachedAndIncludeFinishedTogether() throws IOException {
        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?cached=true&include_finished=true");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));
        assertTrue("Response should contain finished_queries", responseMap.containsKey("finished_queries"));

        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");
        List<Map<String, Object>> finishedQueries = (List<Map<String, Object>>) responseMap.get("finished_queries");
        assertNotNull("Live queries list should not be null", liveQueries);
        assertNotNull("Finished queries list should not be null", finishedQueries);
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
        String params = "?cached=true&include_finished=true&wlmGroupId=DEFAULT_WORKLOAD_GROUP&verbose=true&sort=latency&size=10";
        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + params);
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));
        assertTrue("Response should contain finished_queries", responseMap.containsKey("finished_queries"));

        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");
        List<Map<String, Object>> finishedQueries = (List<Map<String, Object>>) responseMap.get("finished_queries");
        assertNotNull("Live queries list should not be null", liveQueries);
        assertNotNull("Finished queries list should not be null", finishedQueries);

        // Verify that the size limit is respected (should be <= 10)
        assertTrue("Live queries should respect size limit", liveQueries.size() <= 10);
    }

    /**
     * Test parameter validation for cached parameter
     */
    public void testInvalidCachedParameterValidation() throws IOException {
        String[] invalidCachedValues = { "invalid", "1", "yes", "no" };

        for (String invalidValue : invalidCachedValues) {
            Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?cached=" + invalidValue);
            try {
                client().performRequest(request);
                fail("Should have failed with invalid cached parameter: " + invalidValue);
            } catch (Exception e) {
                assertTrue(
                    "Error should mention boolean parameter validation",
                    e.getMessage().contains("Failed to parse value") || e.getMessage().contains("only [true] or [false] are allowed")
                );
            }
        }
    }

    /**
     * Test parameter validation for include_finished parameter
     */
    public void testInvalidIncludeFinishedParameterValidation() throws IOException {
        String[] invalidIncludeFinishedValues = { "invalid", "1", "yes", "no" };

        for (String invalidValue : invalidIncludeFinishedValues) {
            Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?include_finished=" + invalidValue);
            try {
                client().performRequest(request);
                fail("Should have failed with invalid include_finished parameter: " + invalidValue);
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
        // Should not contain finished_queries by default (include_finished=false by default)
        assertFalse("Response should not contain finished_queries by default", responseMap.containsKey("finished_queries"));

        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");
        assertNotNull("Live queries list should not be null", liveQueries);
    }
}
