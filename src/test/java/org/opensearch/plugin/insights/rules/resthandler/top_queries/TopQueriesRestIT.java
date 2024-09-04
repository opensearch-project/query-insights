/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.top_queries;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/** Rest Action tests for Top Queries */
public class TopQueriesRestIT extends QueryInsightsRestTestCase {
    /**
     * test Query Insights is installed
     *
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
     *
     * @throws IOException IOException
     */
    public void testTopQueriesResponses() throws IOException, InterruptedException {
        // Enable Top N Queries feature
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(defaultTopQueriesSettings());
        Response response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        doSearch(2);
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

        // Enable Top N Queries by resource usage
        request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(topQueriesByResourceUsagesSettings());
        response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        // Do Search
        doSearch(2);
        // run five times to make sure the records are drained to the top queries services
        for (int i = 0; i < 5; i++) {
            // Get Top Queries
            request = new Request("GET", "/_insights/top_queries?type=cpu&pretty");
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

    /**
     * Test Top Queries setting endpoints
     *
     * @throws IOException IOException
     */
    public void testTopQueriesSettings() throws IOException {
        for (String setting : invalidTopQueriesSettings()) {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(setting);
            try {
                client().performRequest(request);
                fail("Should not succeed with invalid top queries settings");
            } catch (ResponseException e) {
                assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
            }
        }
    }

    private String topQueriesByResourceUsagesSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.memory.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.memory.window_size\" : \"600s\",\n"
            + "        \"search.insights.top_queries.memory.top_n_size\" : \"5\",\n"
            + "        \"search.insights.top_queries.cpu.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.cpu.window_size\" : \"600s\",\n"
            + "        \"search.insights.top_queries.cpu.top_n_size\" : 5\n"
            + "    }\n"
            + "}";
    }

    private String[] invalidTopQueriesSettings() {
        return new String[] {
            "{\n" + "    \"persistent\" : {\n" + "        \"search.insights.top_queries.latency.enabled\" : 1\n" + "    }\n" + "}",
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.latency.window_size\" : \"-1s\"\n"
                + "    }\n"
                + "}",
            "{\n" + "    \"persistent\" : {\n" + "        \"search.insights.top_queries.latency.top_n_size\" : -1\n" + "    }\n" + "}" };
    }
}
