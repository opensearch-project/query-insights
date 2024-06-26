/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.top_queries;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Rest Action tests for Query Insights
 */
public class TopQueriesRestIT extends OpenSearchRestTestCase {

    /**
     * test Query Insights is installed
     * @throws IOException IOException
     */
    @SuppressWarnings("unchecked")
    public void testQueryInsightsPluginInstalled() throws IOException {
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
     * test enabling top queries
     * @throws IOException IOException
     */
    public void testTopQueriesResponses() throws IOException, InterruptedException {
        // Enable Top N Queries feature
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(defaultTopQueriesSettings());
        Response response = client().performRequest(request);

        Assert.assertEquals(200, response.getStatusLine().getStatusCode());

        // Create documents for search
        request = new Request("POST", "/my-index-0/_doc");
        request.setJsonEntity(createDocumentsBody());
        response = client().performRequest(request);

        Assert.assertEquals(201, response.getStatusLine().getStatusCode());

        // Do Search
        request = new Request("GET", "/my-index-0/_search?size=20&pretty");
        request.setJsonEntity(searchBody());
        response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());

        // run five times to make sure the records are drained to the top queries services
        for (int i = 0; i < 5; i++) {
            // Get Top Queries
            request = new Request("GET", "/_insights/top_queries?pretty");
            response = client().performRequest(request);
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());
            String top_requests = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
            Assert.assertTrue(top_requests.contains("top_queries"));
            int top_n_array_size = top_requests.split("timestamp", -1).length - 1;
            if (top_n_array_size == 0) {
                Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis());
                continue;
            }
            Assert.assertEquals(2, top_n_array_size);
        }
    }

    private String defaultTopQueriesSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"600s\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 5\n"
            + "    }\n"
            + "}";
    }

    private String createDocumentsBody() {
        return "{\n"
            + "  \"@timestamp\": \"2099-11-15T13:12:00\",\n"
            + "  \"message\": \"this is document 1\",\n"
            + "  \"user\": {\n"
            + "    \"id\": \"cyji\"\n"
            + "  }\n"
            + "}";
    }

    private String searchBody() {
        return "{}";
    }
}
