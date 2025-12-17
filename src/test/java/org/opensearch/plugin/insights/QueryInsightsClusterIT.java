/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Multi-Node & Cluster Integration Tests for Query Insights Plugin
 *
 * @see <a href="https://github.com/opensearch-project/opensearch-build/issues/5868">Issue #5868</a>
 *
 * This test suite covers:
 * - Plugin functionality across multiple nodes
 * - Data aggregation from multiple nodes
 * - Cluster state changes impact on plugin
 * - Cross-node query insights data consistency
 */
public class QueryInsightsClusterIT extends QueryInsightsRestTestCase {

    /**
     * Test multi-node data collection and aggregation with explicit node targeting
     * Tests that queries sent to different nodes are properly collected and aggregated
     */
    @SuppressWarnings("unchecked")
    public void testMultiNodeDataCollectionAndAggregation() throws IOException, InterruptedException {
        // Wait for all nodes to be ready before testing
        waitForExpectedNodes(2);

        // Verify cluster is healthy and ready
        verifyMultiNodeClusterSetup();

        // Clear any existing queries from previous tests
        updateClusterSettings(this::disableTopQueriesSettings);
        waitForEmptyTopQueriesResponse();

        // Enable all metric types for comprehensive testing
        updateClusterSettings(this::multiNodeAllMetricsSettings);

        // Allow settings to propagate
        Thread.sleep(1000);

        // Distribute different query types across nodes
        try (RestClient node1Client = getNodeClient(0); RestClient node2Client = getNodeClient(1)) {
            // Node 1: Send match and range queries
            String[] node1Queries = { "match", "range" };
            for (String queryType : node1Queries) {
                Request request = new Request("GET", "/my-index-0/_search?size=20");
                request.setJsonEntity(searchBody(queryType));
                Response response = node1Client.performRequest(request);
                assertEquals(200, response.getStatusLine().getStatusCode());
            }

            // Node 2: Send term and match queries
            String[] node2Queries = { "term", "match" };
            for (String queryType : node2Queries) {
                Request request = new Request("GET", "/my-index-0/_search?size=20");
                request.setJsonEntity(searchBody(queryType));
                Response response = node2Client.performRequest(request);
                assertEquals(200, response.getStatusLine().getStatusCode());
            }
        }

        // Wait for query processing and drain
        Thread.sleep(6000);

        // Verify aggregated data from all nodes with data integrity checks
        // Fetch queries and validate both count and content in a single API call
        List<Map<String, Object>> topQueries = fetchHistoricalTopQueries(null, null, "latency");
        assertEquals("Should have collected 4 queries for latency", 4, topQueries.size());

        Set<String> uniqueNodeIds = new HashSet<>();
        for (Map<String, Object> query : topQueries) {
            // Verify data integrity
            assertNotNull("Query should have measurements", query.get("measurements"));
            assertNotNull("Query should have source", query.get("source"));
            assertNotNull("Query should have timestamp", query.get("timestamp"));

            Map<String, Object> measurements = (Map<String, Object>) query.get("measurements");
            assertTrue("Should have latency measurement", measurements.containsKey("latency"));

            String nodeId = (String) query.get("node_id");
            if (nodeId != null) {
                uniqueNodeIds.add(nodeId);
            }
        }
        assertTrue("Should have data from multiple nodes", uniqueNodeIds.size() >= 1);

        // Verify CPU and memory metrics are also working
        assertTopQueriesCount(4, "cpu");
        assertTopQueriesCount(4, "memory");
    }

    /**
     * Helper method to get search body for different query types
     */
    private String searchBody(String queryType) {
        switch (queryType) {
            case "match":
                return "{ \"query\": { \"match\": { \"message\": \"document\" } } }";
            case "range":
                return "{ \"query\": { \"range\": { \"@timestamp\": { \"gte\": \"2024-01-01\" } } } }";
            case "term":
                return "{ \"query\": { \"term\": { \"user.id\": \"cyji\" } } }";
            default:
                return "{}";
        }
    }

    /**
     * Test cross-node query insights data consistency with explicit node targeting
     */
    @SuppressWarnings("unchecked")
    public void testCrossNodeQueryInsightsDataConsistency() throws IOException, InterruptedException {
        // Wait for all nodes to be ready before testing
        waitForExpectedNodes(2);

        // Clear any existing queries
        updateClusterSettings(this::disableTopQueriesSettings);
        waitForEmptyTopQueriesResponse();

        // Enable query insights with grouping for consistency testing
        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Allow settings to propagate
        Thread.sleep(1000);

        // Send identical queries to both nodes
        String identicalQuery = "{ \"query\": { \"match\": { \"message\": \"document\" } } }";

        // Send to node 1
        try (RestClient node1Client = getNodeClient(0)) {
            for (int i = 0; i < 3; i++) {
                Request request = new Request("GET", "/my-index-0/_search?size=20");
                request.setJsonEntity(identicalQuery);
                Response response = node1Client.performRequest(request);
                assertEquals(200, response.getStatusLine().getStatusCode());
            }
        }

        // Send identical query to node 2
        try (RestClient node2Client = getNodeClient(1)) {
            for (int i = 0; i < 2; i++) {
                Request request = new Request("GET", "/my-index-0/_search?size=20");
                request.setJsonEntity(identicalQuery);
                Response response = node2Client.performRequest(request);
                assertEquals(200, response.getStatusLine().getStatusCode());
            }
        }

        // Wait for processing
        Thread.sleep(6000);

        // With grouping enabled, identical queries should be grouped as 1 query shape
        assertTopQueriesCount(1, "latency");

        // Verify query grouping consistency - should have grouping info
        List<Map<String, Object>> topQueries = fetchHistoricalTopQueries(null, null, "latency");
        assertFalse("Should have collected queries", topQueries.isEmpty());
        for (Map<String, Object> query : topQueries) {
            assertTrue("Query should have source", query.containsKey("source"));
            Map<String, Object> source = (Map<String, Object>) query.get("source");
            assertNotNull("Query source should not be null", source);
        }
    }

    /**
     * Test top queries API with explicit node targeting and filtering
     */
    @SuppressWarnings("unchecked")
    public void testTopQueriesNodeFiltering() throws IOException, InterruptedException {
        // Wait for all nodes to be ready before testing
        waitForExpectedNodes(2);

        // Clear any existing queries
        updateClusterSettings(this::disableTopQueriesSettings);
        waitForEmptyTopQueriesResponse();

        // Enable query insights
        updateClusterSettings(this::defaultTopQueriesSettings);

        // Allow settings to propagate
        Thread.sleep(1000);

        // Get node IDs
        List<String> nodeIds = getClusterNodeIds();
        assertTrue("Expected at least 2 nodes after waiting", nodeIds.size() >= 2);
        String node1Id = nodeIds.get(0);
        String node2Id = nodeIds.get(1);

        // Send 3 queries explicitly to node 1
        try (RestClient node1Client = getNodeClient(0)) {
            for (int i = 0; i < 3; i++) {
                Request request = new Request("GET", "/my-index-0/_search?size=20");
                request.setJsonEntity(searchBody());
                Response response = node1Client.performRequest(request);
                assertEquals(200, response.getStatusLine().getStatusCode());
            }
        }

        // Send 2 queries explicitly to node 2
        try (RestClient node2Client = getNodeClient(1)) {
            for (int i = 0; i < 2; i++) {
                Request request = new Request("GET", "/my-index-0/_search?size=20");
                request.setJsonEntity(searchBody());
                Response response = node2Client.performRequest(request);
                assertEquals(200, response.getStatusLine().getStatusCode());
            }
        }

        // Wait for processing
        Thread.sleep(10000);

        // Verify queries were recorded (with retries for timing issues)
        List<Map<String, Object>> allQueries = null;
        for (int retry = 0; retry < 3; retry++) {
            allQueries = fetchHistoricalTopQueries(null, null, "latency");
            if (!allQueries.isEmpty()) {
                break;
            }
            Thread.sleep(1000);
        }

        // Test filtering by node ID
        List<Map<String, Object>> node1Queries = fetchHistoricalTopQueries(null, node1Id, "latency");
        for (Map<String, Object> query : node1Queries) {
            String queryNodeId = (String) query.get("node_id");
            if (queryNodeId != null) {
                assertEquals("Filtered queries should only be from node 1", node1Id, queryNodeId);
            }
        }

        List<Map<String, Object>> node2Queries = fetchHistoricalTopQueries(null, node2Id, "latency");
        for (Map<String, Object> query : node2Queries) {
            String queryNodeId = (String) query.get("node_id");
            if (queryNodeId != null) {
                assertEquals("Filtered queries should only be from node 2", node2Id, queryNodeId);
            }
        }
    }

    /**
     * Test cluster health impact of query insights
     */
    public void testClusterHealthImpactOfQueryInsights() throws IOException, InterruptedException {
        // Monitor cluster health before enabling query insights
        verifyClusterHealth();

        // Clear any existing queries
        updateClusterSettings(this::disableTopQueriesSettings);
        waitForEmptyTopQueriesResponse();

        // Enable query insights
        updateClusterSettings(this::defaultTopQueriesSettings);

        // Allow settings to propagate
        Thread.sleep(1000);

        // Perform searches
        doSearch(15);

        // Verify cluster health remains stable
        verifyClusterHealth();

        // Verify query insights are still functioning
        assertTopQueriesCount(5, "latency");
    }

    /**
     * Test health stats API across multiple nodes
     */
    @SuppressWarnings("unchecked")
    public void testHealthStatsAcrossMultipleNodes() throws IOException, InterruptedException {
        // Enable query insights
        updateClusterSettings(this::defaultTopQueriesSettings);

        // Perform some searches to generate load
        doSearch(10);
        Thread.sleep(6000); // Allow processing

        // Query health stats from all nodes
        Request healthRequest = new Request("GET", "/_insights/health_stats");
        Response healthResponse = client().performRequest(healthRequest);
        assertEquals(200, healthResponse.getStatusLine().getStatusCode());

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                healthResponse.getEntity().getContent()
            )
        ) {
            Map<String, Object> response = parser.map();
            assertNotNull("Health stats response should not be null", response);
            assertFalse("Health stats response should not be empty", response.isEmpty());
            assertTrue("Should have at least 1 node reporting health stats", response.size() >= 1);

            // Verify each node has proper health stats structure
            for (Map.Entry<String, Object> nodeEntry : response.entrySet()) {
                Map<String, Object> nodeStats = (Map<String, Object>) nodeEntry.getValue();

                assertTrue("Node stats should contain ThreadPoolInfo", nodeStats.containsKey("ThreadPoolInfo"));
                assertTrue("Node stats should contain QueryRecordsQueueSize", nodeStats.containsKey("QueryRecordsQueueSize"));
                assertTrue("Node stats should contain TopQueriesHealthStats", nodeStats.containsKey("TopQueriesHealthStats"));
            }
        }
    }

    /**
     * Test live queries API across multiple nodes
     */
    @SuppressWarnings("unchecked")
    public void testLiveQueriesAcrossMultipleNodes() throws IOException, InterruptedException {
        Request liveRequest = new Request("GET", "/_insights/live_queries");
        Response liveResponse = client().performRequest(liveRequest);
        assertEquals(200, liveResponse.getStatusLine().getStatusCode());

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                liveResponse.getEntity().getContent()
            )
        ) {
            Map<String, Object> response = parser.map();
            assertTrue("Live queries response should contain live_queries field", response.containsKey("live_queries"));

            List<Map<String, Object>> liveQueries = (List<Map<String, Object>>) response.get("live_queries");
            assertNotNull("Live queries list should not be null", liveQueries);
        }

        // Test with nodeId filter
        List<String> nodeIds = getClusterNodeIds();
        if (!nodeIds.isEmpty()) {
            String nodeId = nodeIds.get(0);
            Request filteredRequest = new Request("GET", "/_insights/live_queries?nodeId=" + nodeId);
            Response filteredResponse = client().performRequest(filteredRequest);
            assertEquals(200, filteredResponse.getStatusLine().getStatusCode());
        }
    }

    // Helper Methods

    /**
     * Wait for the expected number of nodes to be available in the cluster
     * This prevents race conditions where tests run before all nodes are ready
     * @param expectedNodeCount the minimum number of nodes to wait for
     * @throws IOException if there's an error querying the cluster
     * @throws InterruptedException if the wait is interrupted
     */
    private void waitForExpectedNodes(int expectedNodeCount) throws IOException, InterruptedException {
        long timeoutMillis = 30000; // 30 seconds timeout
        long startTime = System.currentTimeMillis();
        int actualNodeCount = 0;

        while ((System.currentTimeMillis() - startTime) < timeoutMillis) {
            try {
                List<String> nodeIds = getClusterNodeIds();
                actualNodeCount = nodeIds.size();

                if (actualNodeCount >= expectedNodeCount) {
                    // Verify cluster health as well
                    Request request = new Request("GET", "/_cluster/health");
                    Response response = client().performRequest(request);
                    if (response.getStatusLine().getStatusCode() == 200) {
                        return; // Success - we have enough nodes and cluster is responsive
                    }
                }
            } catch (IOException e) {
                // Cluster might not be ready yet, continue waiting
            }

            Thread.sleep(500); // Wait 500ms before retry
        }

        throw new IllegalStateException(
            "Timeout waiting for " + expectedNodeCount + " nodes. Found only " + actualNodeCount + " nodes after 30 seconds"
        );
    }

    /**
     * Verify multi-node cluster setup
     */
    private void verifyMultiNodeClusterSetup() throws IOException, InterruptedException {
        long timeoutMillis = 60000; // 60 seconds timeout for cluster to become green
        long startTime = System.currentTimeMillis();
        boolean isGreen = false;
        int numberOfNodes = 0;

        while ((System.currentTimeMillis() - startTime) < timeoutMillis) {
            Request request = new Request("GET", "/_cluster/health");
            Response response = client().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());

            String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    responseBody.getBytes(StandardCharsets.UTF_8)
                )
            ) {
                Map<String, Object> healthMap = parser.map();
                numberOfNodes = (Integer) healthMap.get("number_of_nodes");
                String status = (String) healthMap.get("status");

                if (numberOfNodes >= 2 && "green".equals(status)) {
                    isGreen = true;
                    break;
                }

                // If yellow, wait and retry; if red, fail immediately
                if ("red".equals(status)) {
                    fail("Cluster status is red, cannot proceed with testing. Number of nodes: " + numberOfNodes);
                }
            }

            Thread.sleep(1000); // Wait 1 second before retry
        }

        assertTrue("Expected at least 2 nodes for multi-node testing, found: " + numberOfNodes, numberOfNodes >= 2);
        assertTrue("Cluster did not reach green status within timeout. This may indicate replica allocation issues.", isGreen);
    }

    /**
     * Verify cluster health
     */
    private void verifyClusterHealth() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseBody.getBytes(StandardCharsets.UTF_8)
            )
        ) {
            Map<String, Object> healthMap = parser.map();
            String status = (String) healthMap.get("status");
            assertTrue("Cluster should be healthy", "green".equals(status) || "yellow".equals(status));
        }
    }

    /**
     * Get list of cluster node IDs
     */
    private List<String> getClusterNodeIds() throws IOException {
        Request request = new Request("GET", "/_nodes?format=json");
        Response response = client().performRequest(request);

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                response.getEntity().getContent()
            )
        ) {
            Map<String, Object> nodesInfo = parser.map();
            Map<String, Object> nodes = (Map<String, Object>) nodesInfo.get("nodes");

            return new ArrayList<>(nodes.keySet());
        }
    }

    /**
     * Multi-node all metrics settings with longer window size
     * Uses 5-minute windows to prevent queries from being lost during test execution
     * when multiple assertions are performed (latency, cpu, memory)
     */
    private String multiNodeAllMetricsSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.cpu.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.memory.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"5m\",\n"
            + "        \"search.insights.top_queries.cpu.window_size\" : \"5m\",\n"
            + "        \"search.insights.top_queries.memory.window_size\" : \"5m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 5,\n"
            + "        \"search.insights.top_queries.cpu.top_n_size\" : 5,\n"
            + "        \"search.insights.top_queries.memory.top_n_size\" : 5,\n"
            + "        \"search.insights.top_queries.grouping.group_by\" : \"none\"\n"
            + "    }\n"
            + "}";
    }

}
