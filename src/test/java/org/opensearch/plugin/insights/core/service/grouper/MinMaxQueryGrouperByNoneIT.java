/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.service.grouper;

import java.io.IOException;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

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

        updateClusterSettings(this::groupByNoneSettings);

        waitForEmptyTopQueriesResponse();

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(12, "latency");
    }

    private String groupByNoneSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 100,\n"
            + "        \"search.insights.top_queries.grouping.group_by\" : \"none\",\n"
            + "        \"search.insights.top_queries.grouping.max_groups_excluding_topn\" : 5\n"
            + "    }\n"
            + "}";
    }
}
