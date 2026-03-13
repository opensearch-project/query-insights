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
     * Test the exact API call: GET /_insights/live_queries
     * Should return only live queries
     */
    @SuppressWarnings("unchecked")
    public void testLiveQueriesOnlyCached() throws IOException {
        Request request = new Request("GET", "/_insights/live_queries");
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

    /**
     * Test the exact API call: GET /_insights/live_queries?use_finished_cache=true
     * Should return both live and finished queries
     */
    @SuppressWarnings("unchecked")
    public void testLiveAndFinishedQueriesCached() throws IOException {
        Request request = new Request("GET", "/_insights/live_queries?use_finished_cache=true");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);

        // Validate response structure matches the example
        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));
        assertTrue("Response should contain finished_queries", responseMap.containsKey("finished_queries"));

        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");
        List<Map<String, Object>> finishedQueries = (List<Map<String, Object>>) responseMap.get("finished_queries");

        assertNotNull("Live queries list should not be null", liveQueries);
        assertNotNull("Finished queries list should not be null", finishedQueries);

        // Validate structure of live queries
        for (Map<String, Object> query : liveQueries) {
            validateLiveQueryStructure(query);
        }

        // Validate structure of finished queries
        for (Map<String, Object> query : finishedQueries) {
            validateFinishedQueryStructure(query);
        }
    }

    /**
     * Test the exact API call: GET /_insights/live_queries?use_finished_cache=true
     * Should return both live and finished queries without cache
     */
    @SuppressWarnings("unchecked")
    public void testLiveAndFinishedQueriesNonCached() throws IOException {
        Request request = new Request("GET", "/_insights/live_queries?use_finished_cache=true");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);

        // Validate response structure matches the example
        assertTrue("Response should contain live_queries", responseMap.containsKey("live_queries"));
        assertTrue("Response should contain finished_queries", responseMap.containsKey("finished_queries"));

        List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");
        List<Map<String, Object>> finishedQueries = (List<Map<String, Object>>) responseMap.get("finished_queries");

        assertNotNull("Live queries list should not be null", liveQueries);
        assertNotNull("Finished queries list should not be null", finishedQueries);
    }

    /**
     * Validate the structure of a live query matches the expected format
     */
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
     * Validate the structure of a finished query matches the expected format
     */
    private void validateFinishedQueryStructure(Map<String, Object> query) {
        // Required fields for finished queries (more detailed than live queries)
        assertTrue("Query should have timestamp", query.containsKey("timestamp"));
        assertTrue("Query should have id", query.containsKey("id"));
        assertTrue("Query should have node_id", query.containsKey("node_id"));
        assertTrue("Query should have measurements", query.containsKey("measurements"));

        // Additional fields that may be present in finished queries
        if (query.containsKey("total_shards")) {
            assertTrue("Total shards should be a number", query.get("total_shards") instanceof Number);
        }

        if (query.containsKey("group_by")) {
            assertTrue("Group by should be a string", query.get("group_by") instanceof String);
        }

        if (query.containsKey("wlm_group_id")) {
            assertTrue("WLM group ID should be a string", query.get("wlm_group_id") instanceof String);
        }

        if (query.containsKey("source")) {
            assertTrue("Source should be an object", query.get("source") instanceof Map);
        }

        if (query.containsKey("indices")) {
            assertTrue("Indices should be a list", query.get("indices") instanceof List);
        }

        if (query.containsKey("search_type")) {
            assertTrue("Search type should be a string", query.get("search_type") instanceof String);
        }

        if (query.containsKey("phase_latency_map")) {
            assertTrue("Phase latency map should be an object", query.get("phase_latency_map") instanceof Map);
        }

        if (query.containsKey("task_resource_usages")) {
            assertTrue("Task resource usages should be a list", query.get("task_resource_usages") instanceof List);
        }

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
            "",
            "?use_finished_cache=true",
            "?use_finished_cache=false",
            "?use_finished_cache=true&verbose=true",
            "?use_finished_cache=true&sort=cpu&size=5",
            "?wlmGroupId=DEFAULT_WORKLOAD_GROUP",
            "?wlmGroupId=DEFAULT_WORKLOAD_GROUP&use_finished_cache=true" };

        for (String params : validCombinations) {
            Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + params);
            Response response = client().performRequest(request);
            assertEquals("Parameters: " + params, 200, response.getStatusLine().getStatusCode());

            Map<String, Object> responseMap = entityAsMap(response);
            assertTrue("Response should contain live_queries for params: " + params, responseMap.containsKey("live_queries"));

            // Check if finished_queries should be present
            boolean shouldHaveFinished = params.contains("use_finished_cache=true");
            assertEquals(
                "Finished queries presence for params: " + params,
                shouldHaveFinished,
                responseMap.containsKey("finished_queries")
            );
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
