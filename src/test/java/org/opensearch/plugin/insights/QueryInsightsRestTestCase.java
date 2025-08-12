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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.rest.OpenSearchRestTestCase;

public abstract class QueryInsightsRestTestCase extends OpenSearchRestTestCase {
    protected static final String QUERY_INSIGHTS_INDICES_PREFIX = "top_queries";
    private static final Logger logger = Logger.getLogger(QueryInsightsRestTestCase.class.getName());
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);

    protected boolean isHttps() {
        return Optional.ofNullable(System.getProperty("https")).map("true"::equalsIgnoreCase).orElse(false);
    }

    @Override
    protected String getProtocol() {
        return isHttps() ? "https" : "http";
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        if (isHttps()) {
            configureHttpsClient(builder, settings);
        } else {
            configureClient(builder, settings);
        }

        builder.setStrictDeprecationMode(false);
        return builder.build();
    }

    protected static void configureClient(RestClientBuilder builder, Settings settings) throws IOException {
        String userName = System.getProperty("user");
        String password = System.getProperty("password");
        if (userName != null && password != null) {
            builder.setHttpClientConfigCallback(httpClientBuilder -> {
                BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(new AuthScope(null, -1), new UsernamePasswordCredentials(userName, password));
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            });
        }
        OpenSearchRestTestCase.configureClient(builder, settings);
    }

    protected static void configureHttpsClient(RestClientBuilder builder, Settings settings) throws IOException {
        // Similar to client configuration with OpenSearch:
        // https://github.com/opensearch-project/OpenSearch/blob/2.15.1/test/framework/src/main/java/org/opensearch/test/rest/OpenSearchRestTestCase.java#L841-L863
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            String userName = Optional.ofNullable(System.getProperty("user"))
                .orElseThrow(() -> new RuntimeException("user name is missing"));
            String password = Optional.ofNullable(System.getProperty("password"))
                .orElseThrow(() -> new RuntimeException("password is missing"));
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            final AuthScope anyScope = new AuthScope(null, -1);
            credentialsProvider.setCredentials(anyScope, new UsernamePasswordCredentials(userName, password));
            try {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    // disable the certificate since our testing cluster just uses the default security configuration
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .setSSLContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
        Header[] defaultHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
        }
        builder.setDefaultHeaders(defaultHeaders);
        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        final TimeValue socketTimeout = TimeValue.parseTimeValue(
            socketTimeoutString == null ? "60s" : socketTimeoutString,
            CLIENT_SOCKET_TIMEOUT
        );
        builder.setRequestConfigCallback(conf -> conf.setSocketTimeout(Math.toIntExact(socketTimeout.getMillis())));
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    /**
     * wipeAllIndices won't work since it cannot delete security index. Use
     * wipeAllQueryInsightsIndices instead.
     */
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void runBeforeEachTest() throws IOException {
        // Create documents for search
        Request request = new Request("POST", "/my-index-0/_doc");
        request.setJsonEntity(createDocumentsBody());
        Response response = client().performRequest(request);

        Assert.assertEquals(201, response.getStatusLine().getStatusCode());
    }

    @SuppressWarnings("unchecked")
    @After
    public void wipeAllQueryInsightsIndices() throws Exception {
        Response response = adminClient().performRequest(new Request("GET", "/_cat/indices?format=json&expand_wildcards=all"));
        MediaType xContentType = MediaType.fromMediaType(response.getEntity().getContentType().getValue());
        try (
            XContentParser parser = xContentType.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.getEntity().getContent()
                )
        ) {
            XContentParser.Token token = parser.nextToken();
            List<Map<String, Object>> parserList = null;
            if (token == XContentParser.Token.START_ARRAY) {
                parserList = parser.listOrderedMap().stream().map(obj -> (Map<String, Object>) obj).collect(Collectors.toList());
            } else {
                parserList = Collections.singletonList(parser.mapOrdered());
            }

            for (Map<String, Object> index : parserList) {
                final String indexName = (String) index.get("index");
                if (indexName.startsWith(QUERY_INSIGHTS_INDICES_PREFIX)) {
                    adminClient().performRequest(new Request("DELETE", "/" + indexName));
                }
            }
        }
    }

    protected String defaultTopQueriesSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 5,\n"
            + "        \"search.insights.top_queries.memory.enabled\" : \"false\",\n"
            + "        \"search.insights.top_queries.cpu.enabled\" : \"false\",\n"
            + "        \"search.insights.top_queries.group_by\" : \"none\"\n"
            + "    }\n"
            + "}";
    }

    protected String defaultTopQueryGroupingSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 5,\n"
            + "        \"search.insights.top_queries.group_by\" : \"similarity\",\n"
            + "        \"search.insights.top_queries.max_groups_excluding_topn\" : 5\n"
            + "    }\n"
            + "}";
    }

    protected String createDocumentsBody() {
        return "{\n"
            + "  \"@timestamp\": \"2024-04-01T13:12:00\",\n"
            + "  \"message\": \"this is document 1\",\n"
            + "  \"user\": {\n"
            + "    \"id\": \"cyji\"\n"
            + "  }\n"
            + "}";
    }

    protected String searchBody() {
        return "{}";
    }

    protected void doSearch(int times) throws IOException {
        for (int i = 0; i < times; i++) {
            // Do Search
            Request request = new Request("GET", "/my-index-0/_search?size=20&pretty");
            request.setJsonEntity(searchBody());
            Response response = client().performRequest(request);
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        }
    }

    protected void doSearch(String queryType, int times) throws IOException {
        for (int i = 0; i < times; i++) {
            // Do Search
            Request request = new Request("GET", "/my-index-0/_search?size=20&pretty");

            // Set query based on the query type
            request.setJsonEntity(searchBody(queryType));

            Response response = client().performRequest(request);
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        }
    }

    private String searchBody(String queryType) {
        switch (queryType) {
            case "match":
                // Query shape 1: Match query
                return "{\n" + "  \"query\": {\n" + "    \"match\": {\n" + "      \"field1\": \"value1\"\n" + "    }\n" + "  }\n" + "}";

            case "range":
                // Query shape 2: Range query
                return "{\n"
                    + "  \"query\": {\n"
                    + "    \"range\": {\n"
                    + "      \"field2\": {\n"
                    + "        \"gte\": 10,\n"
                    + "        \"lte\": 50\n"
                    + "      }\n"
                    + "    }\n"
                    + "  }\n"
                    + "}";

            case "term":
                // Query shape 3: Term query
                return "{\n"
                    + "  \"query\": {\n"
                    + "    \"term\": {\n"
                    + "      \"field3\": {\n"
                    + "        \"value\": \"exact-value\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  }\n"
                    + "}";

            default:
                throw new IllegalArgumentException("Unknown query type: " + queryType);
        }
    }

    protected int countTopQueries(String json) {
        // Basic pattern to match JSON array elements in `top_queries`
        Pattern pattern = Pattern.compile("\\{\\s*\"timestamp\"");
        Matcher matcher = pattern.matcher(json);

        int count = 0;
        while (matcher.find()) {
            count++;
        }

        return count;
    }

    protected void waitForEmptyTopQueriesResponse() throws IOException, InterruptedException {
        boolean isEmpty = false;
        long timeoutMillis = 70000; // 70 seconds timeout
        long startTime = System.currentTimeMillis();

        while (!isEmpty && (System.currentTimeMillis() - startTime) < timeoutMillis) {
            Request request = new Request("GET", "/_insights/top_queries?pretty");
            Response response = client().performRequest(request);

            if (response.getStatusLine().getStatusCode() != 200) {
                Thread.sleep(1000); // Sleep before retrying
                continue;
            }

            String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

            if (countTopQueries(responseBody) == 0) {
                isEmpty = true;
            } else {
                Thread.sleep(1000); // Sleep before retrying
            }
        }

        if (!isEmpty) {
            throw new IllegalStateException("Top queries response did not become empty within the timeout period");
        }
    }

    protected void assertTopQueriesCount(int expectedTopQueriesCount, String type) throws IOException, InterruptedException {
        // Ensure records are drained to the top queries service
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis());

        // run five times to make sure the records are drained to the top queries services
        for (int i = 0; i < 5; i++) {
            String responseBody = getTopQueries(type);

            int topNArraySize = countTopQueries(responseBody);

            if (topNArraySize < expectedTopQueriesCount) {
                // Ensure records are drained to the top queries service
                Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis());
                continue;
            }

            // Validate that all queries are listed separately (no grouping)
            Assert.assertEquals(expectedTopQueriesCount, topNArraySize);
        }
    }

    protected String getTopQueries(String type) throws IOException {
        // Base URL
        String endpoint = "/_insights/top_queries?pretty";

        if (type != null) {
            switch (type) {
                case "cpu":
                case "memory":
                case "latency":
                    endpoint = "/_insights/top_queries?type=" + type + "&pretty";
                    break;
                case "all":
                    // Keep the default endpoint (no type parameter)
                    break;
                default:
                    // Throw an exception if the type is invalid
                    throw new IllegalArgumentException("Invalid type: " + type + ". Valid types are 'all', 'cpu', 'memory', or 'latency'.");
            }
        }

        Request request = new Request("GET", endpoint);
        Response response = client().performRequest(request);

        Assert.assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        return responseBody;
    }

    protected void updateClusterSettings(Supplier<String> settingsSupplier) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(settingsSupplier.get());
        Response response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    protected void createDocument() throws IOException {
        String json = "{ \"title\": \"Test Document\", \"content\": \"This is a test document for OpenSearch\" }";
        Request req = new Request("POST", "/my-index-0/_doc/");
        req.setJsonEntity(json);
        Response response = client().performRequest(req);
        assertEquals(201, response.getStatusLine().getStatusCode());
    }

    protected void performSearch() throws IOException, InterruptedException {
        Thread.sleep(5000);

        String searchJson = "{ \"query\": { \"match\": { \"title\": \"Test Document\" } } }";
        Request req = new Request("POST", "/my-index-0/_search?size=20");
        req.setJsonEntity(searchJson);
        Response response = client().performRequest(req);
        assertEquals(200, response.getStatusLine().getStatusCode());
        String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("Expected search result for title", content.contains("\"Test Document\""));
    }

    protected void setLatencyWindowSize(String size) throws IOException {
        String json = "{ \"persistent\": { \"search.insights.top_queries.latency.window_size\": \"" + size + "\" } }";
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity(json);
        client().performRequest(req);
    }

    protected void defaultExporterSettings() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{ \"persistent\": { "
                + "\"search.insights.top_queries.exporter.type\": \"local_index\", "
                + "\"search.insights.top_queries.latency.enabled\": \"true\" } }"
        );
        Response response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    protected void cleanup() throws IOException, InterruptedException {
        Thread.sleep(12000);

        try {
            client().performRequest(new Request("DELETE", "/top_queries"));
        } catch (ResponseException e) {
            logger.warning("Cleanup: Failed to delete /top_queries: " + e.getMessage());
        }

        try {
            client().performRequest(new Request("DELETE", "/my-index-0"));
        } catch (ResponseException e) {
            logger.warning("Cleanup: Failed to delete /my-index-0: " + e.getMessage());
        }

        String resetSettings = "{ \"persistent\": { "
            + "\"search.insights.top_queries.exporter.type\": \"none\", "
            + "\"search.insights.top_queries.latency.enabled\": \"false\" } }";
        Request resetReq = new Request("PUT", "/_cluster/settings");
        resetReq.setJsonEntity(resetSettings);
        client().performRequest(resetReq);
    }

    protected void cleanupIndextemplate() throws IOException, InterruptedException {
        Thread.sleep(3000);

        try {
            client().performRequest(new Request("DELETE", "/_index_template"));
        } catch (ResponseException e) {
            logger.warning("Failed to delete /_index_template: " + e.getMessage());
        }
    }

    protected void checkLocalIndices() throws IOException {
        Request indicesRequest = new Request("GET", "/_cat/indices?v");
        Response response = client().performRequest(indicesRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        assertTrue("Expected top_queries-* index to be green", responseContent.contains("green"));

        String suffix = null;
        Pattern pattern = Pattern.compile("top_queries-(\\d{4}\\.\\d{2}\\.\\d{2}-\\d{5})");
        Matcher matcher = pattern.matcher(responseContent);
        if (matcher.find()) {
            suffix = matcher.group(1);
        } else {
            fail("Failed to extract top_queries index suffix");
        }

        assertNotNull("Failed to extract suffix from top_queries-* index", suffix);
        String fullIndexName = "top_queries-" + suffix;
        assertTrue("Expected top_queries-{" + fullIndexName + "} index to be present", responseContent.contains(fullIndexName));

        Request fetchRequest = new Request("GET", "/" + fullIndexName + "/_search?size=10");
        Response fetchResponse = client().performRequest(fetchRequest);
        assertEquals(200, fetchResponse.getStatusLine().getStatusCode());

        byte[] bytes = fetchResponse.getEntity().getContent().readAllBytes();

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                bytes
            )
        ) {
            Map<String, Object> responseMap = parser.map();

            Map<String, Object> hitsWrapper = (Map<String, Object>) responseMap.get("hits");
            List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsWrapper.get("hits");

            Map<String, Object> firstHit = hits.get(0);
            Map<String, Object> source = (Map<String, Object>) firstHit.get("_source");

            assertEquals("query_then_fetch", source.get("search_type"));
            assertEquals("NONE", source.get("group_by"));
            assertEquals(1, ((Number) source.get("total_shards")).intValue());

            Map<String, Object> queryBlock = (Map<String, Object>) source.get("query");

            if (queryBlock != null && queryBlock.containsKey("match")) {
                Map<String, Object> match = (Map<String, Object>) queryBlock.get("match");
                if (match != null && match.containsKey("title")) {
                    Map<String, Object> title = (Map<String, Object>) match.get("title");
                    if (title != null) {
                        assertEquals("Test Document", title.get("query"));
                    }
                }
            }

            Map<String, Object> measurements = (Map<String, Object>) source.get("measurements");
            assertNotNull("Expected measurements", measurements);
            assertTrue(measurements.containsKey("cpu"));
            assertTrue(measurements.containsKey("latency"));
            assertTrue(measurements.containsKey("memory"));

            List<Map<String, Object>> taskResourceUsages = (List<Map<String, Object>>) source.get("task_resource_usages");
            assertTrue("Expected non-empty task_resource_usages", taskResourceUsages.size() > 0);
        }
    }

    protected void checkQueryInsightsIndexTemplate() throws IOException {
        Request request = new Request("GET", "/_index_template?pretty");
        Response response = client().performRequest(request);
        byte[] bytes = response.getEntity().getContent().readAllBytes();

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                bytes
            )
        ) {
            Map<String, Object> parsed = parser.map();

            List<Map<String, Object>> templates = (List<Map<String, Object>>) parsed.get("index_templates");
            assertNotNull("Expected index_templates to exist", templates);
            assertFalse("Expected at least one index_template", templates.isEmpty());

            Map<String, Object> firstTemplate = templates.get(0);
            assertEquals("query_insights_top_queries_template", firstTemplate.get("name"));

            Map<String, Object> indexTemplate = (Map<String, Object>) firstTemplate.get("index_template");

            List<String> indexPatterns = (List<String>) indexTemplate.get("index_patterns");
            assertTrue("Expected index_patterns to include top_queries-*", indexPatterns.contains("top_queries-*"));

            Map<String, Object> template = (Map<String, Object>) indexTemplate.get("template");
            Map<String, Object> settings = (Map<String, Object>) template.get("settings");
            Map<String, Object> indexSettings = (Map<String, Object>) settings.get("index");
            assertEquals("1", indexSettings.get("number_of_shards"));
            assertEquals("0-2", indexSettings.get("auto_expand_replicas"));

            Map<String, Object> mappings = (Map<String, Object>) template.get("mappings");
            Map<String, Object> meta = (Map<String, Object>) mappings.get("_meta");
            assertEquals(1, ((Number) meta.get("schema_version")).intValue());
            assertEquals("top_n_queries", meta.get("query_insights_feature_space"));

            Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
            assertTrue("Expected 'total_shards' in mappings", properties.containsKey("total_shards"));
            assertTrue("Expected 'search_type' in mappings", properties.containsKey("search_type"));
            assertTrue("Expected 'task_resource_usages' in mappings", properties.containsKey("task_resource_usages"));
            assertTrue("Expected 'measurements' in mappings", properties.containsKey("measurements"));
        }
    }

    protected void setLocalIndexToDebug() throws IOException {
        String debugExporterJson = "{ \"persistent\": { \"search.insights.top_queries.exporter.type\": \"debug\" } }";
        Request debugExporterRequest = new Request("PUT", "/_cluster/settings");
        debugExporterRequest.setJsonEntity(debugExporterJson);
        client().performRequest(debugExporterRequest);
    }

    protected void disableLocalIndexExporter() throws IOException {
        String disableExporterJson = "{ \"persistent\": { \"search.insights.top_queries.exporter.type\": \"none\" } }";
        Request disableExporterRequest = new Request("PUT", "/_cluster/settings");
        disableExporterRequest.setJsonEntity(disableExporterJson);
        client().performRequest(disableExporterRequest);
    }

    protected String[] invalidExporterSettings() {
        return new String[] {
            "{ \"persistent\" : { \"search.insights.top_queries.exporter.type\" : invalid_type } }",
            "{ \"persistent\" : { \"search.insights.top_queries.exporter.type\" : local_index, \"search.insights.top_queries.exporter.config.index\" : \"1a2b\" } }" };
    }

    protected List<String[]> fetchHistoricalTopQueries(String ID, String NODEID, String Type) throws IOException {
        String to = formatter.format(Instant.now());
        String from = formatter.format(Instant.now().minusSeconds(9600)); // Default 160 minutes
        return fetchHistoricalTopQueries(from, to, ID, NODEID, Type);
    }

    protected List<String[]> fetchHistoricalTopQueries(String from, String to, String filterId, String filterNodeID, String type)
        throws IOException {
        String endpoint = "/_insights/top_queries?from=" + from + "&to=" + to;

        if (filterId != null && !filterId.equals("null")) {
            endpoint += "&id=" + filterId;
        }
        if (filterNodeID != null && !filterNodeID.equals("null")) {
            endpoint += "&nodeId=" + filterNodeID;
        }
        if (type != null && !type.equals("null")) {
            endpoint += "&type=" + type;
        }

        Request fetchRequest = new Request("GET", endpoint);
        Response fetchResponse = client().performRequest(fetchRequest);

        assertEquals(200, fetchResponse.getStatusLine().getStatusCode());
        byte[] content = fetchResponse.getEntity().getContent().readAllBytes();

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                content
            )
        ) {
            Map<String, Object> root = parser.map();
            List<Map<String, Object>> topQueries = (List<Map<String, Object>>) root.get("top_queries");
            assertNotNull("Expected 'top_queries' field", topQueries);
            assertFalse("Expected at least one top query", topQueries.isEmpty());

            boolean matchFound = false;

            boolean idMismatchFound = false;
            boolean nodeIdMismatchFound = false;

            List<String[]> idNodePairs = new ArrayList<>();

            for (Map<String, Object> query : topQueries) {
                assertTrue(query.containsKey("timestamp"));

                List<?> indices = (List<?>) query.get("indices");
                assertNotNull("Expected 'indices' field", indices);
                String id = (String) query.get("id");
                String nodeId = (String) query.get("node_id");

                if (filterId != null && !filterId.equals("null") && !filterId.equals(id)) {
                    idMismatchFound = true;
                }
                if (filterNodeID != null && !filterNodeID.equals("null") && !filterNodeID.equals(nodeId)) {
                    nodeIdMismatchFound = true;
                }
                idNodePairs.add(new String[] { id, nodeId });

                Map<String, Object> source = (Map<String, Object>) query.get("source");
                Map<String, Object> queryBlock = (Map<String, Object>) source.get("query");
                Map<String, Object> match = (Map<String, Object>) queryBlock.get("match");
                Map<String, Object> title = (Map<String, Object>) match.get("title");
                List<Map<String, Object>> taskUsages = (List<Map<String, Object>>) query.get("task_resource_usages");
                assertFalse("task_resource_usages should not be empty", taskUsages.isEmpty());
                for (Map<String, Object> task : taskUsages) {
                    assertTrue("Missing action", task.containsKey("action"));
                    Map<String, Object> usage = (Map<String, Object>) task.get("taskResourceUsage");
                    assertNotNull("Missing cpu_time_in_nanos", usage.get("cpu_time_in_nanos"));
                    assertNotNull("Missing memory_in_bytes", usage.get("memory_in_bytes"));
                }

                Map<String, Object> measurements = (Map<String, Object>) query.get("measurements");
                assertNotNull("Expected measurements", measurements);
                assertTrue(measurements.containsKey("cpu"));
                assertTrue(measurements.containsKey("memory"));
                assertTrue(measurements.containsKey("latency"));
            }

            if (filterId != null && !filterId.equals("null")) {
                assertFalse("One or more IDs did not match the filterId", idMismatchFound);
            }
            if (filterNodeID != null && !filterNodeID.equals("null")) {
                assertFalse("One or more node IDs did not match the filterNodeID", nodeIdMismatchFound);
            }

            return idNodePairs;

        }

    }

    protected List<String[]> fetchHistoricalTopQueries(Instant from, Instant to, String ID, String NODEID, String Type) throws IOException {
        return fetchHistoricalTopQueries(formatter.format(from), formatter.format(to), ID, NODEID, Type);
    }

}
