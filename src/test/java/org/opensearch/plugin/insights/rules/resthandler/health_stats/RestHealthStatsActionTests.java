/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.health_stats;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.opensearch.plugin.insights.rules.action.health_stats.HealthStatsRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

public class RestHealthStatsActionTests extends OpenSearchTestCase {

    private RestHealthStatsAction restHealthStatsAction;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        restHealthStatsAction = new RestHealthStatsAction();
    }

    public void testRoutes() {
        List<BaseRestHandler.Route> routes = restHealthStatsAction.routes();
        assertEquals(1, routes.size());
        assertEquals("GET", routes.get(0).getMethod().name());
        assertEquals("/_insights/health_stats", routes.get(0).getPath());
    }

    public void testGetName() {
        assertEquals("query_insights_health_stats_action", restHealthStatsAction.getName());
    }

    public void testPrepareRequest() {
        RestRequest request = buildRestRequest(Collections.singletonMap("nodeId", "node1,node2"));
        HealthStatsRequest healthStatsRequest = RestHealthStatsAction.prepareRequest(request);
        assertEquals(2, healthStatsRequest.nodesIds().length);
        assertEquals("node1", healthStatsRequest.nodesIds()[0]);
        assertEquals("node2", healthStatsRequest.nodesIds()[1]);
    }

    public void testPrepareRequestWithEmptyNodeId() {
        RestRequest request = buildRestRequest(Collections.emptyMap());
        HealthStatsRequest healthStatsRequest = RestHealthStatsAction.prepareRequest(request);
        assertEquals(0, healthStatsRequest.nodesIds().length);
    }

    private FakeRestRequest buildRestRequest(Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_insights/health_stats")
            .withParams(params)
            .build();
    }
}
