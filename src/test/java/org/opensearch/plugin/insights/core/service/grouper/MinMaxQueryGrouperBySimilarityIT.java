/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.service.grouper;

import java.io.IOException;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/**
 * ITs for Grouping Top Queries by similarity
 */
public class MinMaxQueryGrouperBySimilarityIT extends QueryInsightsRestTestCase {

    /**
     * test grouping top queries
     *
     * @throws IOException IOException
     */
    public void testGroupingBySimilarity() throws IOException, InterruptedException {

        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(3, "latency");
    }

    /**
     * Test invalid query grouping settings
     *
     * @throws IOException IOException
     */
    public void testInvalidQueryGroupingSettings() throws IOException {
        for (String setting : invalidQueryGroupingSettings()) {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(setting);
            try {
                client().performRequest(request);
                fail("Should not succeed with invalid query grouping settings");
            } catch (ResponseException e) {
                assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
            }
        }
    }

    /**
     * Test valid query grouping settings
     *
     * @throws IOException IOException
     */
    public void testValidQueryGroupingSettings() throws IOException {
        for (String setting : validQueryGroupingSettings()) {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(setting);
            Response response = client().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
        }
    }

    private String[] invalidQueryGroupingSettings() {
        return new String[] {
            // Invalid max_groups: below minimum (-1)
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.max_groups_excluding_topn\" : -1\n"
                + "    }\n"
                + "}",

            // Invalid max_groups: above maximum (10001)
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.max_groups_excluding_topn\" : 10001\n"
                + "    }\n"
                + "}",

            // Invalid group_by: unsupported value
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.group_by\" : \"unsupported_value\"\n"
                + "    }\n"
                + "}" };
    }

    private String[] validQueryGroupingSettings() {
        return new String[] {
            // Valid max_groups: minimum value (0)
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.max_groups_excluding_topn\" : 0\n"
                + "    }\n"
                + "}",

            // Valid max_groups: maximum value (10000)
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.max_groups_excluding_topn\" : 10000\n"
                + "    }\n"
                + "}",

            // Valid group_by: supported value (SIMILARITY)
            "{\n" + "    \"persistent\" : {\n" + "        \"search.insights.top_queries.group_by\" : \"SIMILARITY\"\n" + "    }\n" + "}" };
    }

    private String groupByNoneSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 100,\n"
            + "        \"search.insights.top_queries.group_by\" : \"none\",\n"
            + "        \"search.insights.top_queries.max_groups_excluding_topn\" : 5\n"
            + "    }\n"
            + "}";
    }
}
