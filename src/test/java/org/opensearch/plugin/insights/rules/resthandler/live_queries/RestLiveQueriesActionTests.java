/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.live_queries;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.Before;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Unit tests for the {@link RestLiveQueriesAction} class.
 */
public class RestLiveQueriesActionTests extends OpenSearchTestCase {

    private RestLiveQueriesAction restLiveQueriesAction;
    private NodeClient client;
    private RestChannel channel;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        restLiveQueriesAction = new RestLiveQueriesAction();
        client = mock(NodeClient.class);
        channel = mock(RestChannel.class);
        when(channel.newBuilder()).thenThrow(new AssertionError("Should not be called in prepareRequest test"));
    }

    public void testRoutes() {
        List<BaseRestHandler.Route> routes = restLiveQueriesAction.routes();
        assertEquals(2, routes.size());
        assertEquals(RestRequest.Method.GET, routes.get(0).getMethod());
        assertEquals(QueryInsightsSettings.LIVE_QUERIES_BASE_URI, routes.get(0).getPath());
        assertEquals(RestRequest.Method.GET, routes.get(1).getMethod());
        assertEquals(String.format(Locale.ROOT, "%s/{nodeId}", QueryInsightsSettings.LIVE_QUERIES_BASE_URI), routes.get(1).getPath());
    }

    public void testGetName() {
        assertEquals("query_insights_live_queries_action", restLiveQueriesAction.getName());
    }

    public void testPrepareRequestWithNodeIds() {
        Map<String, String> params = Map.of("nodeId", "node1,node2", "verbose", "false", "timeout", "1m");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_insights/live_queries/node1,node2")
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();

        LiveQueriesRequest liveQueriesRequest = RestLiveQueriesAction.prepareRequest(request);
        assertEquals(2, liveQueriesRequest.nodesIds().length);
        assertEquals("node1", liveQueriesRequest.nodesIds()[0]);
        assertEquals("node2", liveQueriesRequest.nodesIds()[1]);
        assertFalse(liveQueriesRequest.isVerbose());
        assertEquals("1m", liveQueriesRequest.timeout().getStringRep());
    }

    public void testPrepareRequestWithoutNodeIds() {
        Map<String, String> params = Map.of("verbose", "true");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_insights/live_queries")
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();

        LiveQueriesRequest liveQueriesRequest = RestLiveQueriesAction.prepareRequest(request);
        assertEquals(0, liveQueriesRequest.nodesIds().length); // Expect cluster-wide
        assertTrue(liveQueriesRequest.isVerbose());
    }

    public void testPrepareRequestDefaults() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_insights/live_queries").build();

        LiveQueriesRequest liveQueriesRequest = RestLiveQueriesAction.prepareRequest(request);
        assertEquals(0, liveQueriesRequest.nodesIds().length); // Expect cluster-wide
        assertTrue(liveQueriesRequest.isVerbose()); // Default verbose is true
    }
}
