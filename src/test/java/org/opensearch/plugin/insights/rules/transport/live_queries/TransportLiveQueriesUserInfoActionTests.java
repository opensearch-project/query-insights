/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;
import org.opensearch.plugin.insights.core.service.ChildSearchCosts;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesUserInfoNodeResponse;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesUserInfoRequest;
import org.opensearch.plugin.insights.rules.model.FinishedSearches;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Each node answers the live queries fan-out with what it holds for the keys it owns.
 */
public class TransportLiveQueriesUserInfoActionTests extends OpenSearchTestCase {

    private final ThreadPool threadPool = new TestThreadPool("TransportLiveQueriesUserInfoActionTests");
    private final QueryInsightsService queryInsightsService = mock(QueryInsightsService.class);
    private final DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testANodeReturnsTheSearchesItsRunningPplQueriesHaveFinished() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(node);
        TransportLiveQueriesUserInfoAction action = new TransportLiveQueriesUserInfoAction(
            threadPool,
            clusterService,
            mock(TransportService.class),
            queryInsightsService,
            mock(ActionFilters.class)
        );
        List<String> keys = List.of("node1:10", "node1:11");
        when(queryInsightsService.getLiveQueryUserInfoForKeys(keys)).thenReturn(
            Map.of("node1:11", new UserPrincipalContext.UserPrincipalInfo("alice", List.of("backend1"), List.of("user_role")))
        );
        ChildSearchCosts held = new ChildSearchCosts();
        held.add("PPL:node1:10", 10L, 100L);
        held.add("PPL:node1:10", 20L, 200L);
        when(queryInsightsService.getFinishedSearchCostsOfPplQueries(keys)).thenReturn(Map.of("node1:10", held.get("PPL:node1:10")));

        LiveQueriesUserInfoNodeResponse response = action.nodeOperation(
            new TransportLiveQueriesUserInfoAction.NodeRequest(new LiveQueriesUserInfoRequest(keys, "node1"))
        );

        assertEquals("alice", response.getUserInfoByTaskKey().get("node1:11").getUsername());
        assertEquals(Map.of("node1:10", new FinishedSearches(2, 30L, 300L)), response.getFinishedSearchesByQueryId());
    }
}
