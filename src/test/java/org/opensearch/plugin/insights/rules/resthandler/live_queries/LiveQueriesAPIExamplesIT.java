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
 * Integration tests that validate the exact API behavior shown in the examples
 */
public class LiveQueriesAPIExamplesIT extends QueryInsightsRestTestCase {

    /**
     * Test the exact API call: GET /_insights/live_queries?use_live_cache=true
     * Should return only live queries
     */
    @SuppressWarnings("unchecked")
    public void testLiveQueriesOnlyCached() throws IOException {
        Request request = new Request("GET", "/_insights/live_queries?use_live_cache=true");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);

        // Validate response structure matches the example
        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));
        assertFalse("Response should not contain finished_queries", responseMap.containsKey("finished_queries"));

        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");
        assertNotNull("Live queries list should not be null", liveQueries);

        // If there are live queries, validate their structure
        for (Map<String, Object> query : liveQueries) {
            validateLiveQueryStructure(query);
        }
    }

    private void validateLiveQueryStructure(Map<String, Object> query) {
        // Required fields for live queries
        assertTrue("Query should have timestamp", query.containsKey("timestamp"));
        assertTrue("Query should have id", query.containsKey("id"));
        assertTrue("Query should have node_id", query.containsKey("node_id"));
        assertTrue("Query should have measurements", query.containsKey("measurements"));

        // Validate timestamp is a number
        assertTrue("Timestamp should be a number", query.get("timestamp") instanceof Number);

        // Validate id is a string
        assertTrue("ID should be a string", query.get("id") instanceof String);

        // Validate node_id is a string
        assertTrue("Node ID should be a string", query.get("node_id") instanceof String);

        // Validate measurements structure
        @SuppressWarnings("unchecked")
        Map<String, Object> measurements = (Map<String, Object>) query.get("measurements");
        validateMeasurementsStructure(measurements);
    }

    /**
     * Validate the measurements structure
     */
    private void validateMeasurementsStructure(Map<String, Object> measurements) {
        // Should have the three main metrics
        assertTrue("Measurements should include latency", measurements.containsKey("latency"));
        assertTrue("Measurements should include cpu", measurements.containsKey("cpu"));
        assertTrue("Measurements should include memory", measurements.containsKey("memory"));

        // Validate each measurement's structure
        for (String metricType : new String[] { "latency", "cpu", "memory" }) {
            @SuppressWarnings("unchecked")
            Map<String, Object> metric = (Map<String, Object>) measurements.get(metricType);
            assertTrue("Metric should have number", metric.containsKey("number"));
            assertTrue("Metric should have count", metric.containsKey("count"));
            assertTrue("Metric should have aggregationType", metric.containsKey("aggregationType"));

            // Validate types
            assertTrue("Number should be a number", metric.get("number") instanceof Number);
            assertTrue("Count should be a number", metric.get("count") instanceof Number);
            assertTrue("AggregationType should be a string", metric.get("aggregationType") instanceof String);
        }
    }

    /**
     * Test parameter combinations that should work
     */
    @SuppressWarnings("unchecked")
    public void testValidParameterCombinations() throws IOException {
        String[] validCombinations = {
            "?use_live_cache=true",
            "?use_live_cache=false",
            "?use_live_cache=true&verbose=true",
            "?use_live_cache=true&sort=cpu&size=5",
            "?wlmGroupId=DEFAULT_WORKLOAD_GROUP",
            "?use_live_cache=true&wlmGroupId=DEFAULT_WORKLOAD_GROUP" };

        for (String params : validCombinations) {
            Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + params);
            Response response = client().performRequest(request);
            assertEquals("Parameters: " + params, 200, response.getStatusLine().getStatusCode());

            Map<String, Object> responseMap = entityAsMap(response);
            assertTrue("Response should contain live_queries for params: " + params, responseMap.containsKey("live_queries"));
        }
    }

    /**
     * Test that the API respects size limits
     */
    @SuppressWarnings("unchecked")
    public void testSizeLimits() throws IOException {
        int[] sizeLimits = { 1, 5, 10, 50 };

        for (int size : sizeLimits) {
            Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?size=" + size);
            Response response = client().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());

            Map<String, Object> responseMap = entityAsMap(response);
            List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");

            assertTrue("Live queries should respect size limit of " + size, liveQueries.size() <= size);
        }
    }
}
