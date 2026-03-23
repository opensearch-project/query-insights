/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 0, scope = OpenSearchIntegTestCase.Scope.TEST)
public class LiveQueriesFinishedCacheIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(QueryInsightsPlugin.class);
    }

    public void testFinishedQueriesCapturedFromBothNodes() throws InterruptedException {
        List<String> nodes = internalCluster().startNodes(2, Settings.EMPTY);

        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertFalse(health.isTimedOut());

        prepareCreate("test").get();
        ensureGreen("test");

        // Run a search from each node so both act as coordinator
        for (String node : nodes) {
            SearchResponse searchResponse = internalCluster().client(node)
                .prepareSearch("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertEquals(0, searchResponse.getFailedShards());
        }

        Thread.sleep(1000);

        LiveQueriesResponse response = client().execute(
            LiveQueriesAction.INSTANCE,
            new LiveQueriesRequest(true, MetricType.LATENCY, 100, new String[0], null, true)
        ).actionGet();

        // Both searches should be captured — one from each coordinator node
        assertEquals(2, response.getFinishedQueries().size());

        internalCluster().stopAllNodes();
    }
}
