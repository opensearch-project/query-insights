package org.opensearch.plugin.insights.core.service.grouper;

import org.junit.Assert;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

import java.io.IOException;

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
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(12, "latency");

        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Top queries should be drained due to grouping change from NONE -> SIMILARITY
        assertTopQueriesCount(0, "latency");

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        // 3 groups
        assertTopQueriesCount(3, "latency");
    }

    public void testSimilarityToNoneGroupingTransition() throws IOException, InterruptedException {

        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(3, "latency");

        updateClusterSettings(this::defaultTopQueriesSettings);

        // Top queries should be drained due to grouping change from SIMILARITY -> NONE
        assertTopQueriesCount(0, "latency");

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(12, "latency");
    }

    public void testSimilarityMaxGroupsChanged() throws IOException, InterruptedException {

        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(3, "latency");

        // Change max groups exluding topn setting
        updateClusterSettings(this::updateMaxGroupsExcludingTopNSetting);

        // Top queries should be drained due to max group change
        assertTopQueriesCount(0, "latency");

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(3, "latency");
    }

    protected String updateMaxGroupsExcludingTopNSetting() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.max_groups_excluding_topn\" : 1\n"
            + "    }\n"
            + "}";
    }
}
