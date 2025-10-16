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
import org.opensearch.client.ResponseException;
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
    private static final int MAX_POLL_ATTEMPTS = 4000;  // Maximum number of times to poll the live queries API
    private static final int POLL_INTERVAL_MS = 5;  // Time between polling attempts
    private static final int CONCURRENT_QUERIES = 5;  // Number of concurrent queries to run
    private static final int MAX_QUERY_ITERATION = 1000; // Max number of times to run the queries
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
     */
    @SuppressWarnings("unchecked")
    public void testLiveQueriesWithConcurrentSearches() throws Exception {
        // Create index and add documents with some data
        createIndexWithData(500);

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
        Response nodesRes = client().performRequest(new Request("GET", "/_nodes"));
        Map<String, Object> nodesMap = entityAsMap(nodesRes);
        Map<String, Object> nodes = (Map<String, Object>) nodesMap.get("nodes");
        String nodeId = nodes.keySet().iterator().next();

        String[] params = new String[] { "?size=1", "", "?sort=cpu", "?verbose=false", "?nodeId=" + nodeId };
        Map<String, Boolean> foundParams = new java.util.HashMap<>();
        for (String param : params) {
            foundParams.put(param, false);
        }

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

                if (!liveQueries.isEmpty()) {
                    foundLiveQueries = true;
                }
                // Run parameter tests on each polling cycle
                for (String param : params) {
                    if (!foundParams.get(param)) {
                        Request pReq = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + param);
                        Response pRes = client().performRequest(pReq);
                        assertEquals(200, pRes.getStatusLine().getStatusCode());
                        Map<String, Object> pMap = entityAsMap(pRes);
                        assertTrue(pMap.containsKey("live_queries"));
                        List<?> pList = (List<?>) pMap.get("live_queries");
                        boolean ok;
                        if ("?size=0".equals(param)) {
                            ok = pList.isEmpty();
                        } else if ("?verbose=false".equals(param)) {
                            ok = pList.stream().allMatch(q -> !((Map<String, Object>) q).containsKey("description"));
                        } else if (param.startsWith("?nodeId=")) {
                            String filterNode = param.substring("?nodeId=".length());
                            ok = pList.stream().allMatch(q -> filterNode.equals(((Map<String, Object>) q).get("node_id")));
                        } else {
                            ok = !pList.isEmpty();
                        }
                        if (ok) {
                            foundParams.put(param, true);
                        }
                    }
                }
                // Break only when main and all param tests have passed
                if (foundLiveQueries && !foundParams.containsValue(false)) {
                    logger.info("All checks succeeded by attempt {}", attempt);
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
            logger.info("Test detected live queries, total: {}", liveQueries.size());
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
            fail("No live queries found.");
        }
        for (String param : params) {
            assertTrue("Parameter test for '" + param + "' did not pass", foundParams.get(param));
        }
    }

    /**
     * Fallback tests: Basic test for all parameters including verbose, node filtering
     */
    @SuppressWarnings("unchecked")
    public void testAllParameters() throws IOException {
        // Retrieve one node ID for filtering tests
        Request nodesRequest = new Request("GET", "/_nodes");
        Response nodesResponse = client().performRequest(nodesRequest);
        Map<String, Object> nodesMap = entityAsMap(nodesResponse);
        Map<String, Object> nodes = (Map<String, Object>) nodesMap.get("nodes");
        assertNotNull(nodes);
        assertFalse(nodes.isEmpty());
        String nodeId = nodes.keySet().iterator().next();

        // Define parameter combinations to test
        String[] params = new String[] { "", "?verbose=false", "?sort=cpu", "?size=1", "?nodeId=" + nodeId };
        for (String param : params) {
            String uri = QueryInsightsSettings.LIVE_QUERIES_BASE_URI + param;
            Request req = new Request("GET", uri);
            Response res = client().performRequest(req);
            assertEquals("Status for param '" + param + "'", 200, res.getStatusLine().getStatusCode());
            Map<String, Object> map = entityAsMap(res);
            assertTrue("Response should contain live_queries for param '" + param + "'", map.containsKey("live_queries"));
        }
    }

    /**
     * Test invalid sort parameter
     *
     * @throws IOException IOException
     */
    public void testInvalidSortParameter() throws IOException {
        String[] invalidSortValues = {
            "invalid",
            "{\"script\":\"doc['field'].value\"}",
            "<script>alert('xss')</script>",
            "../../../etc/passwd" };

        for (String sortValue : invalidSortValues) {
            Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI);
            request.addParameter("sort", sortValue);
            try {
                client().performRequest(request);
                fail("Should not succeed with invalid sort parameter: " + sortValue);
            } catch (ResponseException e) {
                assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
                assertTrue(
                    "Error message should contain sort validation error for: " + sortValue,
                    e.getMessage().contains("invalid sort metric type")
                );
            }
        }
    }

    /**
     * Test invalid size parameter
     *
     * @throws IOException IOException
     */
    public void testInvalidSizeParameter() throws IOException {
        String[] invalidSizeParams = { "?size=-1", "?size=invalid" };

        for (String param : invalidSizeParams) {
            Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + param);
            try {
                client().performRequest(request);
                fail("Should not succeed with invalid size parameter: " + param);
            } catch (ResponseException e) {
                assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
                if (param.contains("size=-1")) {
                    assertTrue(
                        "Error message should contain 'invalid size parameter [-1]' for: " + param,
                        e.getMessage().contains("invalid size parameter [-1]")
                    );
                } else if (param.contains("size=invalid")) {
                    assertTrue(
                        "Error message should contain parameter parsing error for: " + param,
                        e.getMessage().contains("Failed to parse int parameter [size] with value [invalid]")
                    );
                }
            }
        }
    }

    /**
     * Test multiple invalid parameters
     *
     * @throws IOException IOException
     */
    public void testMultipleInvalidParameters() throws IOException {
        String[] multipleInvalidParams = { "?verbose=invalid&size=-1", "?sort=invalid&size=-1" };

        for (String param : multipleInvalidParams) {
            Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + param);
            try {
                client().performRequest(request);
                fail("Should not succeed with multiple invalid parameters: " + multipleInvalidParams);
            } catch (ResponseException e) {
                assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
                if (param.contains("verbose=invalid")) {
                    assertTrue(
                        "Error message should contain verbose parameter error",
                        e.getMessage().contains("Failed to parse value [invalid] as only [true] or [false] are allowed")
                    );
                } else if (param.contains("sort=invalid")) {
                    assertTrue(
                        "Error message should contain sort parameter error",
                        e.getMessage().contains("invalid sort metric type [invalid]")
                    );
                }
            }
        }
    }

    /**
     * Test valid parameters with unexpected extra parameter
     *
     * @throws IOException IOException
     */
    public void testValidParametersWithExtraParams() throws IOException {
        Request nodesRequest = new Request("GET", "/_nodes");
        Response nodesResponse = client().performRequest(nodesRequest);
        Map<String, Object> nodesMap = entityAsMap(nodesResponse);
        Map<String, Object> nodes = (Map<String, Object>) nodesMap.get("nodes");
        String nodeId = nodes.keySet().iterator().next();

        // Test all expected parameters plus an unexpected one
        String paramsWithExtra = "?sort=latency&verbose=true&size=5&nodeId=" + nodeId + "&unknownParam=value";
        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + paramsWithExtra);
        try {
            client().performRequest(request);
            fail("Should not succeed with an unexpected extra parameter");
        } catch (ResponseException e) {
            assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        }
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
