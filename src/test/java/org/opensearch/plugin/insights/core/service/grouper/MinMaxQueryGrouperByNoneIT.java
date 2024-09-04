/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.service.grouper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/**
 * ITs for Grouping Top Queries by none
 */
public class MinMaxQueryGrouperByNoneIT extends QueryInsightsRestTestCase {

    /**
     * Grouping by none should not group queries
     * @throws IOException
     * @throws InterruptedException
     */
    public void testGroupingByNone() throws IOException, InterruptedException {

        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::groupByNoneSettings);

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        // Ensure records are drained to the top queries service
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis());

        // run five times to make sure the records are drained to the top queries services
        for (int i = 0; i < 5; i++) {
            String responseBody = getTopQueries();

            int topNArraySize = countTopQueries(responseBody);

            if (topNArraySize == 0) {
                Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis());
                continue;
            }

            // Validate that all queries are listed separately (no grouping)
            Assert.assertEquals(12, topNArraySize);
        }
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
