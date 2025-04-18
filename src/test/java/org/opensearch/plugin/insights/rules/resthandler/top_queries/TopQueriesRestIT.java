/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.top_queries;

import java.io.IOException;
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
        // Enable only Top N Queries by latency feature
        updateClusterSettings(this::defaultTopQueriesSettings);

        doSearch(10);

        assertTopQueriesCount(5, "latency");

        // Enable Top N Queries by resource usage
        updateClusterSettings(this::topQueriesByResourceUsagesSettings);

        // Do Search
        doSearch(10);

        assertTopQueriesCount(5, "cpu");
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
            + "        \"search.insights.top_queries.memory.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.memory.top_n_size\" : \"5\",\n"
            + "        \"search.insights.top_queries.cpu.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.cpu.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.cpu.top_n_size\" : 5,\n"
            + "        \"search.insights.top_queries.group_by\" : \"none\"\n"
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
