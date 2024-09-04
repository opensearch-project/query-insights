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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.rest.OpenSearchRestTestCase;

public abstract class QueryInsightsRestTestCase extends OpenSearchRestTestCase {
    protected static final String QUERY_INSIGHTS_INDICES_PREFIX = "top_queries";

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
                credentialsProvider.setCredentials(
                    new AuthScope(null, -1),
                    new UsernamePasswordCredentials(userName, password.toCharArray())
                );
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
            credentialsProvider.setCredentials(anyScope, new UsernamePasswordCredentials(userName, password.toCharArray()));
            try {
                final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                    .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .setSslContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build())
                    // See https://issues.apache.org/jira/browse/HTTPCLIENT-2219
                    .setTlsDetailsFactory(sslEngine -> new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol()))
                    .build();
                final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                    .setTlsStrategy(tlsStrategy)
                    .build();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setConnectionManager(connectionManager);
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
        builder.setRequestConfigCallback(
            conf -> conf.setResponseTimeout(Timeout.ofMilliseconds(Math.toIntExact(socketTimeout.getMillis())))
        );
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
        MediaType mediaType = MediaType.fromMediaType(response.getEntity().getContentType());
        try (
            XContentParser parser = mediaType.xContent()
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
            + "  \"@timestamp\": \"2099-11-15T13:12:00\",\n"
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

    protected String getTopQueries() throws IOException {
        Request request = new Request("GET", "/_insights/top_queries?pretty");
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
}
