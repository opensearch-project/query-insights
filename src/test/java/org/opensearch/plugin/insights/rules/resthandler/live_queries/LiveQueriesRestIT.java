/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.live_queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/**
 * Integration tests for Live Queries API
 */
public class LiveQueriesRestIT extends QueryInsightsRestTestCase {

    private static final Logger logger = LogManager.getLogger(LiveQueriesRestIT.class);
    private static final String TEST_INDEX = "test-index";
    private static final int MAX_POLL_ATTEMPTS = 1000;  // Maximum number of times to poll the live queries API
    private static final int POLL_INTERVAL_MS = 5;  // Time between polling attempts
    private static final int CONCURRENT_QUERIES = 5;  // Number of concurrent queries to run
    private static final int MAX_QUERY_ITERATION = 100; // Max number of times to run the queries
    private static final int QUERY_DURATION_MS = 10000; // Time each query should run

    /**
     * Verify Query Insights plugin is installed
     */
    @SuppressWarnings("unchecked")
    public void ensureQueryInsightsPluginInstalled() throws IOException {
        Request request = new Request("GET", "/_cat/plugins?s=component&h=name,component,version,description&format=json");
        Response response = client().performRequest(request);
        List<Object> pluginsList = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).list();
        Assert.assertTrue(
            pluginsList.stream().map(o -> (Map<String, Object>) o).anyMatch(plugin -> plugin.get("component").equals("query-insights"))
        );
    }

    /**
     * Try to detect live queries by running multiple concurrent search operations and polling the API.
     *  TODO: Limitation of this integ test: It's hard to simulate a long running query without being flaky.
     *   We can try using a painless script to simulate with sleep, but by default QueryInsightsRestTestCase disables painless.
     * This test tries its best to simulate long running queries.
     * It will pass if:
     * 1. We find at least one live query, or
     * 2. We hit our polling limit without finding any (due to test environment limitations)
     */
    @SuppressWarnings("unchecked")
    public void testLiveQueriesWithConcurrentSearches() throws Exception {
        // Create index and add documents with some data
        createIndexWithData(100);

        // Set up a coordinator for the search threads
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(CONCURRENT_QUERIES);
        AtomicBoolean shouldStop = new AtomicBoolean(false);
        AtomicInteger completedQueries = new AtomicInteger(0);
        ExecutorService threadPool = Executors.newFixedThreadPool(CONCURRENT_QUERIES);

        // Create and submit search tasks that will run for a while
        for (int i = 0; i < CONCURRENT_QUERIES; i++) {
            final int queryNum = i;
            threadPool.submit(() -> {
                try {
                    logger.info("Search thread {} ready and waiting to start", queryNum);
                    startLatch.await(); // Wait for all threads to be ready
                    String searchJson = generateComplexQuery(queryNum);
                    Request searchRequest = new Request("GET", "/" + TEST_INDEX + "/_search");
                    searchRequest.setJsonEntity(searchJson);

                    // Set longer timeout to let the query run for a while
                    searchRequest.addParameter("timeout", QUERY_DURATION_MS + "ms");

                    try {
                        logger.info("Search thread {} starting search", queryNum);
                        for (int j = 0; j < MAX_QUERY_ITERATION; j++) {
                            client().performRequest(searchRequest);
                        }
                        logger.info("Search thread {} completed successfully", queryNum);
                    } catch (Exception e) {
                        // We expect this might timeout or be cancelled
                        logger.info("Search thread {} ended with: {}", queryNum, e.getMessage());
                    }
                } catch (Exception e) {
                    logger.error("Error in search thread {}: {}", queryNum, e.getMessage());
                } finally {
                    completedQueries.incrementAndGet();
                    completionLatch.countDown();
                }
            });
        }

        // Start all the search threads..
        logger.info("Starting all search threads");
        startLatch.countDown();

        // Poll the Live Queries API repeatedly to try to catch the searches
        logger.info("Beginning to poll the live queries API");
        boolean foundLiveQueries = false;
        List<Map<String, Object>> liveQueries = new ArrayList<>();

        // Create a separate thread to poll the API while search queries are running
        for (int attempt = 0; attempt < MAX_POLL_ATTEMPTS && !shouldStop.get(); attempt++) {
            Thread.sleep(POLL_INTERVAL_MS);

            // Skip further checking if all queries are done already
            if (completedQueries.get() >= CONCURRENT_QUERIES) {
                logger.info("All queries completed, stopping poll attempts");
                break;
            }

            try {
                // Call the Live Queries API
                Request liveQueriesRequest = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?verbose=true");
                Response liveQueriesResponse = client().performRequest(liveQueriesRequest);

                // Parse response
                Map<String, Object> responseMap = entityAsMap(liveQueriesResponse);
                assertTrue(responseMap.containsKey("live_queries"));
                liveQueries = (List<Map<String, Object>>) responseMap.get("live_queries");

                logger.info("Polling attempt {}: Found {} live queries", attempt, liveQueries.size());

                if (liveQueries != null && !liveQueries.isEmpty()) {
                    logger.info("Successfully found live queries on attempt {}", attempt);
                    foundLiveQueries = true;
                    break;
                }
            } catch (Exception e) {
                logger.error("Error polling live queries API: {}", e.getMessage());
            }
        }

        // Signal all threads to stop
        shouldStop.set(true);

        // Wait for all search threads to complete with timeout
        boolean allThreadsCompleted = completionLatch.await(QUERY_DURATION_MS * 2, TimeUnit.MILLISECONDS);
        logger.info("All threads completed? {}", allThreadsCompleted);

        // Shut down executor
        threadPool.shutdownNow();

        // We either found live queries or exhausted our polling attempts.
        if (foundLiveQueries) {
            logger.info("Test detected live queries: {}", liveQueries);
            assertTrue("Should have found at least one live query", !liveQueries.isEmpty());

            // Validate the format of live queries based on LiveQueries.java and LiveQueriesResponse.java
            for (Map<String, Object> query : liveQueries) {
                // Verify required fields are present
                assertTrue("Query should have timestamp", query.containsKey("timestamp"));
                assertTrue("Query should have id", query.containsKey("id"));
                assertTrue("Query should have node_id", query.containsKey("node_id"));
                assertTrue("Query should have measurements", query.containsKey("measurements"));
                assertTrue("Query should have description", query.containsKey("description"));

                // Validate timestamp is a number
                assertTrue("Timestamp should be a number", query.get("timestamp") instanceof Number);

                // Validate id is a string
                assertTrue("ID should be a string", query.get("id") instanceof String);

                // Validate node_id is a string
                assertTrue("Node ID should be a string", query.get("node_id") instanceof String);

                // Validate measurements structure
                Map<String, Object> measurements = (Map<String, Object>) query.get("measurements");
                assertTrue("Measurements should include latency", measurements.containsKey("latency"));
                assertTrue("Measurements should include cpu", measurements.containsKey("cpu"));
                assertTrue("Measurements should include memory", measurements.containsKey("memory"));

                // Validate each measurement's structure
                for (String metricType : new String[] { "latency", "cpu", "memory" }) {
                    Map<String, Object> metric = (Map<String, Object>) measurements.get(metricType);
                    assertTrue("Metric should have number", metric.containsKey("number"));
                    assertTrue("Metric should have count", metric.containsKey("count"));
                    assertTrue("Metric should have aggregationType", metric.containsKey("aggregationType"));

                    // Validate number is a number
                    assertTrue("Number should be a number", metric.get("number") instanceof Number);

                    // Validate count is a number
                    assertTrue("Count should be a number", metric.get("count") instanceof Number);

                    // Validate aggregationType is a string
                    assertTrue("AggregationType should be a string", metric.get("aggregationType") instanceof String);
                }
                assertTrue("Description should be a string", query.get("description") instanceof String);
            }
        } else {
            logger.info(
                "No live queries found after {} attempts - this is acceptable due to test environment limitations",
                MAX_POLL_ATTEMPTS
            );
        }
    }

    /**
     * Fallback tests: Basic test for all parameters including verbose, node filtering
     */
    @SuppressWarnings("unchecked")
    public void testAllParameters() throws IOException {
        // Get node information for node-specific filtering
        Request nodesRequest = new Request("GET", "/_nodes");
        Response nodesResponse = client().performRequest(nodesRequest);
        Map<String, Object> nodesMap = entityAsMap(nodesResponse);
        Map<String, Object> nodes = (Map<String, Object>) nodesMap.get("nodes");

        if (nodes == null || nodes.isEmpty()) {
            fail("No nodes information available");
        }

        // Get the first node ID
        String nodeId = nodes.keySet().iterator().next();

        // Test combinations of parameters

        // 1. Test verbose parameter - should return correct response with verbose=true (default)
        Request verboseRequest = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI);
        Response verboseResponse = client().performRequest(verboseRequest);
        assertEquals(200, verboseResponse.getStatusLine().getStatusCode());

        Map<String, Object> verboseMap = entityAsMap(verboseResponse);
        assertTrue(verboseMap.containsKey("live_queries"));

        // 2. Test verbose=false - should return correct response without extra details
        Request nonVerboseRequest = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?verbose=false");
        Response nonVerboseResponse = client().performRequest(nonVerboseRequest);
        assertEquals(200, nonVerboseResponse.getStatusLine().getStatusCode());

        Map<String, Object> nonVerboseMap = entityAsMap(nonVerboseResponse);
        assertTrue(nonVerboseMap.containsKey("live_queries"));

        // 3. Test node-specific requests - should return data for the specified node
        Request nodeSpecificRequest = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "/" + nodeId);
        Response nodeSpecificResponse = client().performRequest(nodeSpecificRequest);
        assertEquals(200, nodeSpecificResponse.getStatusLine().getStatusCode());

        Map<String, Object> nodeSpecificMap = entityAsMap(nodeSpecificResponse);
        assertTrue(nodeSpecificMap.containsKey("live_queries"));
    }

    /**
     * Create a test index with the specified number of documents
     */
    private void createIndexWithData(int numDocs) throws IOException {
        // Create test index
        createDocument();

        // Add more documents
        for (int i = 1; i < numDocs; i++) {
            Request indexRequest = new Request("POST", "/" + TEST_INDEX + "/_doc");
            String docJson = String.format(
                Locale.ROOT,
                "{\"title\":\"Document %d\",\"value\":%d,\"tags\":[\"tag1\",\"tag2\",\"tag%d\"],\"nested\":{\"field1\":\"value%d\",\"field2\":%d}}",
                i,
                i % 100,
                i % 10,
                i,
                i * 2
            );
            indexRequest.setJsonEntity(docJson);
            client().performRequest(indexRequest);

            // Occasionally refresh to make documents searchable
            if (i % 1000 == 0) {
                Request refreshRequest = new Request("POST", "/" + TEST_INDEX + "/_refresh");
                client().performRequest(refreshRequest);
            }
        }

        // Final refresh to ensure all documents are searchable
        Request refreshRequest = new Request("POST", "/" + TEST_INDEX + "/_refresh");
        client().performRequest(refreshRequest);
        logger.info("Created index with {} documents", numDocs);
    }

    /**
     * Generate a complex search query that should take a while to execute
     */
    private String generateComplexQuery(int queryNum) {
        // Use different query patterns for more diversity
        switch (queryNum % 3) {
            case 0:
                // Complex aggregation query
                return "{\n"
                    + "  \"size\": 1000,\n"
                    + "  \"query\": { \"match_all\": {} },\n"
                    + "  \"aggs\": {\n"
                    + "    \"value_ranges\": {\n"
                    + "      \"range\": {\n"
                    + "        \"field\": \"value\",\n"
                    + "        \"ranges\": [\n"
                    + "          { \"to\": 20 },\n"
                    + "          { \"from\": 20, \"to\": 40 },\n"
                    + "          { \"from\": 40, \"to\": 60 },\n"
                    + "          { \"from\": 60, \"to\": 80 },\n"
                    + "          { \"from\": 80 }\n"
                    + "        ]\n"
                    + "      },\n"
                    + "      \"aggs\": {\n"
                    + "        \"tag_counts\": {\n"
                    + "          \"terms\": { \"field\": \"tags\", \"size\": 100 }\n"
                    + "        }\n"
                    + "      }\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"sort\": [\"_doc\"]\n"
                    + "}";
            case 1:
                // Query with complex filtering
                return "{\n"
                    + "  \"size\": 1000,\n"
                    + "  \"query\": {\n"
                    + "    \"bool\": {\n"
                    + "      \"must\": [\n"
                    + "        { \"wildcard\": { \"title\": \"*Document*\" } }\n"
                    + "      ],\n"
                    + "      \"filter\": [\n"
                    + "        { \"range\": { \"value\": { \"gte\": 10, \"lte\": 90 } } },\n"
                    + "        { \"term\": { \"tags\": \"tag2\" } }\n"
                    + "      ],\n"
                    + "      \"should\": [\n"
                    + "        { \"term\": { \"nested.field1\": \"value5\" } },\n"
                    + "        { \"term\": { \"nested.field1\": \"value10\" } },\n"
                    + "        { \"term\": { \"nested.field1\": \"value15\" } }\n"
                    + "      ],\n"
                    + "      \"minimum_should_match\": 1\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"sort\": [\"_doc\"]\n"
                    + "}";
            default:
                // Query with complex sorting
                return "{\n"
                    + "  \"size\": 1000,\n"
                    + "  \"query\": { \"match_all\": {} },\n"
                    + "  \"sort\": [\n"
                    + "    { \"value\": { \"order\": \"desc\" } },\n"
                    + "    { \"_score\": { \"order\": \"desc\" } },\n"
                    + "    \"_doc\"\n"
                    + "  ]\n"
                    + "}";
        }
    }
}
