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
import java.util.Map;
import org.junit.Before;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

/**
 * Unit tests for the {@link RestLiveQueriesAction} class.
 */
public class RestLiveQueriesActionTests extends OpenSearchTestCase {

    private RestLiveQueriesAction restLiveQueriesAction;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        restLiveQueriesAction = new RestLiveQueriesAction();
        RestChannel channel = mock(RestChannel.class);
        when(channel.newBuilder()).thenThrow(new AssertionError("Should not be called in prepareRequest test"));
    }

    public void testRoutes() {
        List<BaseRestHandler.Route> routes = restLiveQueriesAction.routes();
        assertEquals(1, routes.size());
        assertEquals(RestRequest.Method.GET, routes.getFirst().getMethod());
        assertEquals(QueryInsightsSettings.LIVE_QUERIES_BASE_URI, routes.getFirst().getPath());
    }

    public void testGetName() {
        assertEquals("query_insights_live_queries_action", restLiveQueriesAction.getName());
    }

    public void testPrepareRequestWithNodeIds() {
        Map<String, String> params = Map.of("nodeId", "node1,node2", "verbose", "false");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_insights/live_queries/node1,node2")
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();

        LiveQueriesRequest liveQueriesRequest = RestLiveQueriesAction.prepareRequest(request);
        assertEquals(2, liveQueriesRequest.nodesIds().length);
        assertEquals("node1", liveQueriesRequest.nodesIds()[0]);
        assertEquals("node2", liveQueriesRequest.nodesIds()[1]);
        assertFalse(liveQueriesRequest.isVerbose());
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

    public void testPrepareRequestWithCustomParams() {
        Map<String, String> params = Map.of("nodeId", "node1", "verbose", "false", "sort", "cpu", "size", "3");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(
            QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "/node1"
        ).withParams(params).withMethod(RestRequest.Method.GET).build();
        LiveQueriesRequest req = RestLiveQueriesAction.prepareRequest(request);
        assertArrayEquals(new String[] { "node1" }, req.nodesIds());
        assertFalse(req.isVerbose());
        assertEquals(MetricType.CPU, req.getSortBy());
        assertEquals(3, req.getSize());
    }

    public void testDefaultSortAndSize() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withMethod(RestRequest.Method.GET)
            .build();
        LiveQueriesRequest req = RestLiveQueriesAction.prepareRequest(request);
        assertEquals(MetricType.LATENCY, req.getSortBy());
        assertEquals(QueryInsightsSettings.DEFAULT_LIVE_QUERIES_SIZE, req.getSize());
    }

    public void testPrepareRequestInvalidSort() {
        Map<String, String> params = Map.of("sort", "invalid_metric");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();
        assertThrows(IllegalArgumentException.class, () -> RestLiveQueriesAction.prepareRequest(request));
    }

    public void testPrepareRequestWithSortOnly() {
        Map<String, String> params = Map.of("sort", "memory");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();
        LiveQueriesRequest req = RestLiveQueriesAction.prepareRequest(request);
        assertEquals(MetricType.MEMORY, req.getSortBy());
        assertEquals(100, req.getSize());
    }

    public void testPrepareRequestWithSizeOnly() {
        Map<String, String> params = Map.of("size", "5");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();
        LiveQueriesRequest req = RestLiveQueriesAction.prepareRequest(request);
        assertEquals(MetricType.LATENCY, req.getSortBy());
        assertEquals(5, req.getSize());
    }

    public void testPrepareRequestWithZeroSize() {
        Map<String, String> params = Map.of("size", "0");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();
        assertThrows(IllegalArgumentException.class, () -> RestLiveQueriesAction.prepareRequest(request));
    }

    public void testPrepareRequestWithNegativeSize() {
        Map<String, String> params = Map.of("size", "-1");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();
        assertThrows(IllegalArgumentException.class, () -> RestLiveQueriesAction.prepareRequest(request));
    }

    public void testPrepareRequestInvalidVerbose() {
        // non-boolean verbose parameter
        Map<String, String> params = Map.of("verbose", "notABoolean");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();
        assertThrows(IllegalArgumentException.class, () -> RestLiveQueriesAction.prepareRequest(request));
    }

    public void testPrepareRequestInvalidSize() {
        // non-numeric size parameter
        Map<String, String> params = Map.of("size", "notANumber");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();
        assertThrows(IllegalArgumentException.class, () -> RestLiveQueriesAction.prepareRequest(request));
    }
}
