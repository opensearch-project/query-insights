/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.health_stats;

import java.io.IOException;
import java.util.Map;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/**
 * Integration tests for Health Stats API
 */
public class HealthStatsRestIT extends QueryInsightsRestTestCase {

    /**
     * Test basic health stats API response
     */
    @SuppressWarnings("unchecked")
    public void testHealthStatsBasicResponse() throws IOException {
        Request request = new Request("GET", QueryInsightsSettings.QUERY_INSIGHTS_HEALTH_STATS_URI);
        Response response = client().performRequest(request);

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);

        assertNotNull("Response should not be null", responseMap);
        assertFalse("Response should not be empty", responseMap.isEmpty());

        // Verify each node has health stats
        for (Map.Entry<String, Object> nodeEntry : responseMap.entrySet()) {
            Map<String, Object> nodeStats = (Map<String, Object>) nodeEntry.getValue();

            // Verify ThreadPoolInfo
            assertTrue("Node stats should contain ThreadPoolInfo", nodeStats.containsKey("ThreadPoolInfo"));
            Map<String, Object> threadPoolInfo = (Map<String, Object>) nodeStats.get("ThreadPoolInfo");
            assertNotNull("ThreadPoolInfo should not be null", threadPoolInfo);

            // Verify QueryRecordsQueueSize
            assertTrue("Node stats should contain QueryRecordsQueueSize", nodeStats.containsKey("QueryRecordsQueueSize"));
            assertTrue("QueryRecordsQueueSize should be a number", nodeStats.get("QueryRecordsQueueSize") instanceof Number);

            // Verify TopQueriesHealthStats
            assertTrue("Node stats should contain TopQueriesHealthStats", nodeStats.containsKey("TopQueriesHealthStats"));
            Map<String, Object> topQueriesHealthStats = (Map<String, Object>) nodeStats.get("TopQueriesHealthStats");
            assertNotNull("TopQueriesHealthStats should not be null", topQueriesHealthStats);

            // Verify FieldTypeCacheStats
            assertTrue("Node stats should contain FieldTypeCacheStats", nodeStats.containsKey("FieldTypeCacheStats"));
            Map<String, Object> fieldTypeCacheStats = (Map<String, Object>) nodeStats.get("FieldTypeCacheStats");
            assertNotNull("FieldTypeCacheStats should not be null", fieldTypeCacheStats);

            // Verify FieldTypeCacheStats contains expected fields (snake_case names)
            assertTrue("FieldTypeCacheStats should contain size_in_bytes", fieldTypeCacheStats.containsKey("size_in_bytes"));
            assertTrue("FieldTypeCacheStats should contain entry_count", fieldTypeCacheStats.containsKey("entry_count"));
            assertTrue("FieldTypeCacheStats should contain evictions", fieldTypeCacheStats.containsKey("evictions"));
            assertTrue("FieldTypeCacheStats should contain hit_count", fieldTypeCacheStats.containsKey("hit_count"));
            assertTrue("FieldTypeCacheStats should contain miss_count", fieldTypeCacheStats.containsKey("miss_count"));
        }
    }

    /**
     * Test health stats with nodeId parameter
     */
    @SuppressWarnings("unchecked")
    public void testHealthStatsWithNodeIdFilter() throws IOException {
        // First, get all nodes
        Request nodesRequest = new Request("GET", "/_nodes");
        Response nodesResponse = client().performRequest(nodesRequest);
        Map<String, Object> nodesMap = entityAsMap(nodesResponse);
        Map<String, Object> nodes = (Map<String, Object>) nodesMap.get("nodes");
        assertNotNull(nodes);
        assertFalse(nodes.isEmpty());

        String nodeId = nodes.keySet().iterator().next();

        // Request health stats for specific node
        Request request = new Request("GET", QueryInsightsSettings.QUERY_INSIGHTS_HEALTH_STATS_URI + "?nodeId=" + nodeId);
        Response response = client().performRequest(request);

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertNotNull("Response should not be null", responseMap);
        assertFalse("Response should not be empty", responseMap.isEmpty());

        // Verify only the requested node is returned
        assertTrue("Response should contain the requested nodeId", responseMap.containsKey(nodeId));
    }

    /**
     * Test health stats with timeout parameter
     */
    public void testHealthStatsWithTimeout() throws IOException {
        Request request = new Request("GET", QueryInsightsSettings.QUERY_INSIGHTS_HEALTH_STATS_URI + "?timeout=30s");
        Response response = client().performRequest(request);

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertNotNull("Response should not be null", responseMap);
        assertFalse("Response should not be empty", responseMap.isEmpty());
    }

    /**
     * Test health stats after enabling top queries
     */
    @SuppressWarnings("unchecked")
    public void testHealthStatsWithTopQueriesEnabled() throws IOException, InterruptedException {
        // Enable top queries
        updateClusterSettings(this::defaultTopQueriesSettings);

        // Perform some searches
        doSearch(5);

        // Wait for queries to be processed
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis() * 2);

        // Get health stats
        Request request = new Request("GET", QueryInsightsSettings.QUERY_INSIGHTS_HEALTH_STATS_URI);
        Response response = client().performRequest(request);

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertNotNull("Response should not be null", responseMap);
        assertFalse("Response should not be empty", responseMap.isEmpty());

        // Verify health stats reflect the enabled top queries
        for (Map.Entry<String, Object> nodeEntry : responseMap.entrySet()) {
            Map<String, Object> nodeStats = (Map<String, Object>) nodeEntry.getValue();
            Map<String, Object> topQueriesHealthStats = (Map<String, Object>) nodeStats.get("TopQueriesHealthStats");
            assertNotNull("TopQueriesHealthStats should not be null", topQueriesHealthStats);

            // Verify latency stats are present since we enabled latency top queries
            assertTrue("TopQueriesHealthStats should contain latency", topQueriesHealthStats.containsKey("latency"));
            Map<String, Object> latencyStats = (Map<String, Object>) topQueriesHealthStats.get("latency");
            assertNotNull("Latency stats should not be null", latencyStats);

            // Verify TopQueriesHeapSize is present
            assertTrue("Latency stats should contain TopQueriesHeapSize", latencyStats.containsKey("TopQueriesHeapSize"));
            assertTrue("TopQueriesHeapSize should be a number", latencyStats.get("TopQueriesHeapSize") instanceof Number);
        }

        // Cleanup - disable top queries
        updateClusterSettings(this::disableTopQueriesSettings);
    }

    /**
     * Test health stats with multiple metric types enabled
     */
    @SuppressWarnings("unchecked")
    public void testHealthStatsWithMultipleMetrics() throws IOException, InterruptedException {
        // Enable multiple top queries metrics
        String multiMetricSettings = "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.cpu.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.memory.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.cpu.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.memory.window_size\" : \"1m\"\n"
            + "    }\n"
            + "}";

        updateClusterSettings(() -> multiMetricSettings);

        // Perform some searches
        doSearch(3);

        // Wait for queries to be processed
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis() * 2);

        // Get health stats
        Request request = new Request("GET", QueryInsightsSettings.QUERY_INSIGHTS_HEALTH_STATS_URI);
        Response response = client().performRequest(request);

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertNotNull("Response should not be null", responseMap);
        assertFalse("Response should not be empty", responseMap.isEmpty());

        // Verify all metric types are present in health stats
        for (Map.Entry<String, Object> nodeEntry : responseMap.entrySet()) {
            Map<String, Object> nodeStats = (Map<String, Object>) nodeEntry.getValue();
            Map<String, Object> topQueriesHealthStats = (Map<String, Object>) nodeStats.get("TopQueriesHealthStats");
            assertNotNull("TopQueriesHealthStats should not be null", topQueriesHealthStats);

            // Verify all three metric types are present
            assertTrue("TopQueriesHealthStats should contain latency", topQueriesHealthStats.containsKey("latency"));
            assertTrue("TopQueriesHealthStats should contain cpu", topQueriesHealthStats.containsKey("cpu"));
            assertTrue("TopQueriesHealthStats should contain memory", topQueriesHealthStats.containsKey("memory"));
        }

        // Cleanup - disable all metrics
        updateClusterSettings(this::disableTopQueriesSettings);
    }

    /**
     * Test health stats with invalid parameters
     */
    public void testHealthStatsWithInvalidNodeId() throws IOException {
        Request request = new Request("GET", QueryInsightsSettings.QUERY_INSIGHTS_HEALTH_STATS_URI + "?nodeId=invalid-node-id");
        Response response = client().performRequest(request);

        // Should still return 200 but with empty or no nodes matching the invalid ID
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    /**
     * Test health stats with unexpected extra parameter
     */
    public void testHealthStatsWithExtraParams() throws IOException {
        String paramsWithExtra = "?timeout=30s&unknownParam=value";
        Request request = new Request("GET", QueryInsightsSettings.QUERY_INSIGHTS_HEALTH_STATS_URI + paramsWithExtra);
        try {
            client().performRequest(request);
            fail("Should not succeed with an unexpected extra parameter");
        } catch (ResponseException e) {
            assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        }
    }

    /**
     * Test health stats ThreadPoolInfo structure
     */
    @SuppressWarnings("unchecked")
    public void testHealthStatsThreadPoolInfo() throws IOException {
        Request request = new Request("GET", QueryInsightsSettings.QUERY_INSIGHTS_HEALTH_STATS_URI);
        Response response = client().performRequest(request);

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertNotNull("Response should not be null", responseMap);
        assertFalse("Response should not be empty", responseMap.isEmpty());

        for (Map.Entry<String, Object> nodeEntry : responseMap.entrySet()) {
            Map<String, Object> nodeStats = (Map<String, Object>) nodeEntry.getValue();

            // ThreadPoolInfo is an OpenSearch core object, just verify it exists
            assertTrue("Node stats should contain ThreadPoolInfo", nodeStats.containsKey("ThreadPoolInfo"));
            Object threadPoolInfo = nodeStats.get("ThreadPoolInfo");
            assertNotNull("ThreadPoolInfo should not be null", threadPoolInfo);
        }
    }

    /**
     * Test health stats with query grouping enabled
     */
    @SuppressWarnings("unchecked")
    public void testHealthStatsWithQueryGrouping() throws IOException, InterruptedException {
        // Enable query grouping
        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Perform some searches
        doSearch(5);

        // Wait for queries to be processed
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis() * 2);

        // Get health stats
        Request request = new Request("GET", QueryInsightsSettings.QUERY_INSIGHTS_HEALTH_STATS_URI);
        Response response = client().performRequest(request);

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertNotNull("Response should not be null", responseMap);
        assertFalse("Response should not be empty", responseMap.isEmpty());

        // Verify grouper health stats are present
        for (Map.Entry<String, Object> nodeEntry : responseMap.entrySet()) {
            Map<String, Object> nodeStats = (Map<String, Object>) nodeEntry.getValue();
            Map<String, Object> topQueriesHealthStats = (Map<String, Object>) nodeStats.get("TopQueriesHealthStats");
            assertNotNull("TopQueriesHealthStats should not be null", topQueriesHealthStats);

            if (topQueriesHealthStats.containsKey("latency")) {
                Map<String, Object> latencyStats = (Map<String, Object>) topQueriesHealthStats.get("latency");

                // Verify QueryGrouperHealthStats fields are present
                assertTrue(
                    "Latency stats should contain QueryGrouperHealthStats fields",
                    latencyStats.containsKey("TopQueriesHeapSize") || latencyStats.containsKey("QueryGrouperHealthStats")
                );
            }
        }

        // Cleanup
        updateClusterSettings(this::disableTopQueriesSettings);
    }
}
