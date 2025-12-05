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

public class MinMaxQueryGrouperIT extends QueryInsightsRestTestCase {
    /**
     * Grouping by none should not group queries
     * @throws IOException
     * @throws InterruptedException
     */
    public void testNoneToSimilarityGroupingTransition() throws IOException, InterruptedException {

        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::defaultTopQueriesSettings);

        // Search
        doSearch("range", 1);
        doSearch("match", 3);
        doSearch("term", 2);

        assertTopQueriesCount(6, "latency");

        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Top queries should be drained due to grouping change from NONE -> SIMILARITY
        assertTopQueriesCount(0, "latency");

        // Search
        doSearch("range", 1);
        doSearch("match", 3);
        doSearch("term", 2);

        // 3 groups
        assertTopQueriesCount(3, "latency");
    }

    public void testSimilarityToNoneGroupingTransition() throws IOException, InterruptedException {

        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Search
        doSearch("range", 1);
        doSearch("match", 3);
        doSearch("term", 2);

        assertTopQueriesCount(3, "latency");

        updateClusterSettings(this::defaultTopQueriesSettings);

        // Top queries should be drained due to grouping change from SIMILARITY -> NONE
        assertTopQueriesCount(0, "latency");

        // Search
        doSearch("range", 1);
        doSearch("match", 3);
        doSearch("term", 2);

        assertTopQueriesCount(6, "latency");
    }

    public void testSimilarityMaxGroupsChanged() throws IOException, InterruptedException {

        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Search
        doSearch("range", 1);
        doSearch("match", 3);
        doSearch("term", 2);

        assertTopQueriesCount(3, "latency");

        // Change max groups exluding topn setting
        updateClusterSettings(this::updateMaxGroupsExcludingTopNSetting);

        // Top queries should be drained due to max group change
        assertTopQueriesCount(0, "latency");

        // Search
        doSearch("range", 1);
        doSearch("match", 3);
        doSearch("term", 2);

        assertTopQueriesCount(3, "latency");
    }

    protected String updateMaxGroupsExcludingTopNSetting() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.grouping.max_groups_excluding_topn\" : 1\n"
            + "    }\n"
            + "}";
    }
}
