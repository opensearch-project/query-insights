/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.settings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.insights.rules.action.settings.GetQueryInsightsSettingsRequest;
import org.opensearch.plugin.insights.rules.action.settings.GetQueryInsightsSettingsResponse;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

/**
 * Unit tests for {@link RestGetQueryInsightsSettingsAction}.
 */
public class RestGetQueryInsightsSettingsActionTests extends OpenSearchTestCase {

    /**
     * Test getName returns correct action name
     */
    public void testGetName() {
        RestGetQueryInsightsSettingsAction action = new RestGetQueryInsightsSettingsAction();
        assertEquals("query_insights_get_settings_action", action.getName());
    }

    /**
     * Test routes returns correct number of routes
     */
    public void testGetRoutes() {
        RestGetQueryInsightsSettingsAction action = new RestGetQueryInsightsSettingsAction();
        List<RestHandler.Route> routes = action.routes();
        assertEquals(2, routes.size());
    }

    /**
     * Test routes contain correct paths
     */
    public void testRoutePaths() {
        RestGetQueryInsightsSettingsAction action = new RestGetQueryInsightsSettingsAction();
        List<RestHandler.Route> routes = action.routes();

        // Verify first route: /_insights/settings
        assertEquals(RestRequest.Method.GET, routes.get(0).getMethod());
        assertTrue(routes.get(0).getPath().contains("/_insights/settings"));

        // Verify second route: /_insights/settings/{metric_type}
        assertEquals(RestRequest.Method.GET, routes.get(1).getMethod());
        assertTrue(routes.get(1).getPath().contains("/_insights/settings"));
    }

    /**
     * Test request without metric type parameter
     */
    public void testRequestWithoutMetricType() {
        Map<String, String> params = new HashMap<>();
        RestRequest restRequest = buildRestRequest(params, "/_insights/settings");
        assertNotNull(restRequest);
        assertEquals(RestRequest.Method.GET, restRequest.method());
    }

    /**
     * Test request with latency metric type
     */
    public void testRequestWithLatencyMetricType() {
        Map<String, String> params = new HashMap<>();
        params.put("metric_type", "latency");
        RestRequest restRequest = buildRestRequest(params, "/_insights/settings/latency");
        assertNotNull(restRequest);
        assertEquals("latency", restRequest.param("metric_type"));
    }

    /**
     * Test request with cpu metric type
     */
    public void testRequestWithCpuMetricType() {
        Map<String, String> params = new HashMap<>();
        params.put("metric_type", "cpu");
        RestRequest restRequest = buildRestRequest(params, "/_insights/settings/cpu");
        assertNotNull(restRequest);
        assertEquals("cpu", restRequest.param("metric_type"));
    }

    /**
     * Test request with memory metric type
     */
    public void testRequestWithMemoryMetricType() {
        Map<String, String> params = new HashMap<>();
        params.put("metric_type", "memory");
        RestRequest restRequest = buildRestRequest(params, "/_insights/settings/memory");
        assertNotNull(restRequest);
        assertEquals("memory", restRequest.param("metric_type"));
    }

    /**
     * Test request with invalid metric type (should still pass as validation happens in transport layer)
     */
    public void testRequestWithInvalidMetricType() {
        Map<String, String> params = new HashMap<>();
        params.put("metric_type", "invalid");
        RestRequest restRequest = buildRestRequest(params, "/_insights/settings/invalid");
        assertNotNull(restRequest);
        assertEquals("invalid", restRequest.param("metric_type"));
    }

    /**
     * Test request with empty metric type
     */
    public void testRequestWithEmptyMetricType() {
        Map<String, String> params = new HashMap<>();
        params.put("metric_type", "");
        RestRequest restRequest = buildRestRequest(params, "/_insights/settings/");
        assertNotNull(restRequest);
        assertEquals("", restRequest.param("metric_type"));
    }

    /**
     * Test multiple route methods are GET
     */
    public void testAllRoutesAreGet() {
        RestGetQueryInsightsSettingsAction action = new RestGetQueryInsightsSettingsAction();
        List<RestHandler.Route> routes = action.routes();
        for (RestHandler.Route route : routes) {
            assertEquals(RestRequest.Method.GET, route.getMethod());
        }
    }

    /**
     * Test prepareRequest creates request with null metric type when not provided
     */
    public void testPrepareRequestWithoutMetricType() {
        Map<String, String> params = new HashMap<>();
        RestRequest restRequest = buildRestRequest(params, "/_insights/settings");
        GetQueryInsightsSettingsRequest request = RestGetQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        assertNull(request.getMetricType());
    }

    /**
     * Test prepareRequest creates request with latency metric type
     */
    public void testPrepareRequestWithLatencyMetricType() {
        Map<String, String> params = new HashMap<>();
        params.put("metric_type", "latency");
        RestRequest restRequest = buildRestRequest(params, "/_insights/settings/latency");
        GetQueryInsightsSettingsRequest request = RestGetQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        assertEquals("latency", request.getMetricType());
    }

    /**
     * Test prepareRequest creates request with cpu metric type
     */
    public void testPrepareRequestWithCpuMetricType() {
        Map<String, String> params = new HashMap<>();
        params.put("metric_type", "cpu");
        RestRequest restRequest = buildRestRequest(params, "/_insights/settings/cpu");
        GetQueryInsightsSettingsRequest request = RestGetQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        assertEquals("cpu", request.getMetricType());
    }

    /**
     * Test prepareRequest creates request with memory metric type
     */
    public void testPrepareRequestWithMemoryMetricType() {
        Map<String, String> params = new HashMap<>();
        params.put("metric_type", "memory");
        RestRequest restRequest = buildRestRequest(params, "/_insights/settings/memory");
        GetQueryInsightsSettingsRequest request = RestGetQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        assertEquals("memory", request.getMetricType());
    }

    /**
     * Test prepareRequest handles random metric type (validation happens in transport layer)
     */
    public void testPrepareRequestWithRandomMetricType() {
        Map<String, String> params = new HashMap<>();
        String randomType = randomAlphaOfLengthBetween(3, 10);
        params.put("metric_type", randomType);
        RestRequest restRequest = buildRestRequest(params, "/_insights/settings/" + randomType);
        GetQueryInsightsSettingsRequest request = RestGetQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        assertEquals(randomType, request.getMetricType());
    }

    /**
     * Test prepareRequest handles empty metric type
     */
    public void testPrepareRequestWithEmptyMetricType() {
        Map<String, String> params = new HashMap<>();
        params.put("metric_type", "");
        RestRequest restRequest = buildRestRequest(params, "/_insights/settings/");
        GetQueryInsightsSettingsRequest request = RestGetQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        assertEquals("", request.getMetricType());
    }

    /**
     * Test route exact path for base settings endpoint
     */
    public void testRouteExactPathBaseEndpoint() {
        RestGetQueryInsightsSettingsAction action = new RestGetQueryInsightsSettingsAction();
        List<RestHandler.Route> routes = action.routes();
        assertEquals("/_insights/settings", routes.get(0).getPath());
    }

    /**
     * Test route exact path for metric type endpoint
     */
    public void testRouteExactPathMetricTypeEndpoint() {
        RestGetQueryInsightsSettingsAction action = new RestGetQueryInsightsSettingsAction();
        List<RestHandler.Route> routes = action.routes();
        assertEquals("/_insights/settings/{metric_type}", routes.get(1).getPath());
    }

    /**
     * Test buildRestResponse returns OK status with empty settings
     */
    public void testBuildRestResponseWithEmptySettings() throws Exception {
        FakeRestRequest restRequest = buildRestRequest(new HashMap<>(), "/_insights/settings");
        FakeRestChannel channel = new FakeRestChannel(restRequest, true, 1);
        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(new HashMap<>());

        RestResponse restResponse = RestGetQueryInsightsSettingsAction.buildRestResponse(channel, response);

        assertNotNull(restResponse);
        assertEquals(RestStatus.OK, restResponse.status());
    }

    /**
     * Test buildRestResponse returns OK status with latency settings
     */
    public void testBuildRestResponseWithLatencySettings() throws Exception {
        FakeRestRequest restRequest = buildRestRequest(new HashMap<>(), "/_insights/settings");
        FakeRestChannel channel = new FakeRestChannel(restRequest, true, 1);
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true);
        settings.put("latency.top_n_size", 10);
        settings.put("latency.window_size", "5m");
        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        RestResponse restResponse = RestGetQueryInsightsSettingsAction.buildRestResponse(channel, response);

        assertNotNull(restResponse);
        assertEquals(RestStatus.OK, restResponse.status());
    }

    /**
     * Test buildRestResponse returns OK status with all metric settings
     */
    public void testBuildRestResponseWithAllMetricSettings() throws Exception {
        FakeRestRequest restRequest = buildRestRequest(new HashMap<>(), "/_insights/settings");
        FakeRestChannel channel = new FakeRestChannel(restRequest, true, 1);
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true);
        settings.put("cpu.enabled", false);
        settings.put("memory.enabled", true);
        settings.put("grouping.group_by", "SIMILARITY");
        settings.put("exporter.type", "local_index");
        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        RestResponse restResponse = RestGetQueryInsightsSettingsAction.buildRestResponse(channel, response);

        assertNotNull(restResponse);
        assertEquals(RestStatus.OK, restResponse.status());
    }

    /**
     * Test getSettingsResponse returns a valid listener
     */
    public void testGetSettingsResponseReturnsListener() {
        RestGetQueryInsightsSettingsAction action = new RestGetQueryInsightsSettingsAction();
        FakeRestRequest restRequest = buildRestRequest(new HashMap<>(), "/_insights/settings");
        FakeRestChannel channel = new FakeRestChannel(restRequest, true, 1);

        RestResponseListener<GetQueryInsightsSettingsResponse> listener = action.getSettingsResponse(channel);

        assertNotNull(listener);
    }

    /**
     * Test getSettingsResponse listener builds response correctly
     */
    public void testGetSettingsResponseListenerBuildsResponse() throws Exception {
        RestGetQueryInsightsSettingsAction action = new RestGetQueryInsightsSettingsAction();
        FakeRestRequest restRequest = buildRestRequest(new HashMap<>(), "/_insights/settings");
        FakeRestChannel channel = new FakeRestChannel(restRequest, true, 1);
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true);
        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        RestResponseListener<GetQueryInsightsSettingsResponse> listener = action.getSettingsResponse(channel);
        RestResponse restResponse = listener.buildResponse(response);

        assertNotNull(restResponse);
        assertEquals(RestStatus.OK, restResponse.status());
    }

    /**
     * Helper method to build a FakeRestRequest
     */
    private FakeRestRequest buildRestRequest(Map<String, String> params, String path) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET).withPath(path).withParams(params).build();
    }
}
