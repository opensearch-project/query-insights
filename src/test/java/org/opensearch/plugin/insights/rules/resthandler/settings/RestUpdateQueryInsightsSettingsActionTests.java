/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.settings;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsRequest;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsResponse;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

/**
 * Unit tests for {@link RestUpdateQueryInsightsSettingsAction}.
 */
public class RestUpdateQueryInsightsSettingsActionTests extends OpenSearchTestCase {

    /**
     * Test getName returns correct action name
     */
    public void testGetName() {
        RestUpdateQueryInsightsSettingsAction action = new RestUpdateQueryInsightsSettingsAction();
        assertEquals("query_insights_update_settings_action", action.getName());
    }

    /**
     * Test routes returns correct number of routes
     */
    public void testGetRoutes() {
        RestUpdateQueryInsightsSettingsAction action = new RestUpdateQueryInsightsSettingsAction();
        List<RestHandler.Route> routes = action.routes();
        assertEquals(1, routes.size());
    }

    /**
     * Test route contains correct path and method
     */
    public void testRoutePathAndMethod() {
        RestUpdateQueryInsightsSettingsAction action = new RestUpdateQueryInsightsSettingsAction();
        List<RestHandler.Route> routes = action.routes();

        assertEquals(RestRequest.Method.PUT, routes.get(0).getMethod());
        assertTrue(routes.get(0).getPath().contains("/_insights/settings"));
    }

    /**
     * Test request with empty body
     */
    public void testRequestWithEmptyBody() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{}";
        RestRequest restRequest = buildRestRequest(params, content);
        assertNotNull(restRequest);
        assertEquals(RestRequest.Method.PUT, restRequest.method());
        assertTrue(restRequest.hasContent());
    }

    /**
     * Test request with latency settings
     */
    public void testRequestWithLatencySettings() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"latency\":{\"enabled\":true,\"top_n_size\":10}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        assertNotNull(restRequest);
        assertTrue(restRequest.hasContent());
    }

    /**
     * Test request with multiple metric settings
     */
    public void testRequestWithMultipleMetricSettings() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"latency\":{\"enabled\":true},\"cpu\":{\"enabled\":false},\"memory\":{\"enabled\":true}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        assertNotNull(restRequest);
        assertTrue(restRequest.hasContent());
    }

    /**
     * Test request with grouping settings
     */
    public void testRequestWithGroupingSettings() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"grouping\":{\"group_by\":\"SIMILARITY\"}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        assertNotNull(restRequest);
        assertTrue(restRequest.hasContent());
    }

    /**
     * Test request with exporter settings
     */
    public void testRequestWithExporterSettings() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"exporter\":{\"type\":\"local_index\",\"delete_after_days\":7}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        assertNotNull(restRequest);
        assertTrue(restRequest.hasContent());
    }

    /**
     * Test request with all settings
     */
    public void testRequestWithAllSettings() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"latency\":{\"enabled\":true,\"top_n_size\":10,\"window_size\":\"5m\"},"
            + "\"cpu\":{\"enabled\":false,\"top_n_size\":20},"
            + "\"memory\":{\"enabled\":true,\"window_size\":\"10m\"},"
            + "\"grouping\":{\"group_by\":\"NONE\"},"
            + "\"exporter\":{\"type\":\"debug\",\"delete_after_days\":3}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        assertNotNull(restRequest);
        assertTrue(restRequest.hasContent());
    }

    /**
     * Test route method is PUT
     */
    public void testRouteMethodIsPut() {
        RestUpdateQueryInsightsSettingsAction action = new RestUpdateQueryInsightsSettingsAction();
        List<RestHandler.Route> routes = action.routes();
        for (RestHandler.Route route : routes) {
            assertEquals(RestRequest.Method.PUT, route.getMethod());
        }
    }

    /**
     * Test request with nested settings structure
     */
    public void testRequestWithNestedSettings() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"latency\":{\"enabled\":true}}";
        RestRequest restRequest = buildRestRequest(params, content);
        assertNotNull(restRequest);
        assertTrue(restRequest.hasContent());
    }

    /**
     * Test request with boolean values
     */
    public void testRequestWithBooleanValues() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"latency\":{\"enabled\":true},\"cpu\":{\"enabled\":false}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        assertNotNull(restRequest);
        assertTrue(restRequest.hasContent());
    }

    /**
     * Test request with integer values
     */
    public void testRequestWithIntegerValues() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"latency\":{\"top_n_size\":100},\"exporter\":{\"delete_after_days\":30}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        assertNotNull(restRequest);
        assertTrue(restRequest.hasContent());
    }

    /**
     * Test request with string values
     */
    public void testRequestWithStringValues() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"latency\":{\"window_size\":\"30m\"},\"grouping\":{\"group_by\":\"SIMILARITY\"}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        assertNotNull(restRequest);
        assertTrue(restRequest.hasContent());
    }

    /**
     * Test prepareRequest parses empty body correctly
     */
    @SuppressWarnings("unchecked")
    public void testPrepareRequestWithEmptyBody() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{}";
        RestRequest restRequest = buildRestRequest(params, content);
        UpdateQueryInsightsSettingsRequest request = RestUpdateQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        assertTrue(request.getSettings().isEmpty());
    }

    /**
     * Test prepareRequest parses latency settings correctly
     */
    @SuppressWarnings("unchecked")
    public void testPrepareRequestWithLatencySettings() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"latency\":{\"enabled\":true,\"top_n_size\":10}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        UpdateQueryInsightsSettingsRequest request = RestUpdateQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        assertFalse(request.getSettings().isEmpty());
        assertTrue(request.getSettings().containsKey("persistent"));
        Map<String, Object> persistent = (Map<String, Object>) request.getSettings().get("persistent");
        assertTrue(persistent.containsKey("latency"));
    }

    /**
     * Test prepareRequest parses multiple metric settings correctly
     */
    @SuppressWarnings("unchecked")
    public void testPrepareRequestWithMultipleMetrics() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"latency\":{\"enabled\":true},\"cpu\":{\"enabled\":false},\"memory\":{\"enabled\":true}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        UpdateQueryInsightsSettingsRequest request = RestUpdateQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        Map<String, Object> persistent = (Map<String, Object>) request.getSettings().get("persistent");
        assertNotNull(persistent);
        assertTrue(persistent.containsKey("latency"));
        assertTrue(persistent.containsKey("cpu"));
        assertTrue(persistent.containsKey("memory"));
    }

    /**
     * Test prepareRequest parses grouping settings correctly
     */
    @SuppressWarnings("unchecked")
    public void testPrepareRequestWithGroupingSettings() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"grouping\":{\"group_by\":\"SIMILARITY\"}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        UpdateQueryInsightsSettingsRequest request = RestUpdateQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        Map<String, Object> persistent = (Map<String, Object>) request.getSettings().get("persistent");
        Map<String, Object> grouping = (Map<String, Object>) persistent.get("grouping");
        assertEquals("SIMILARITY", grouping.get("group_by"));
    }

    /**
     * Test prepareRequest parses exporter settings correctly
     */
    @SuppressWarnings("unchecked")
    public void testPrepareRequestWithExporterSettings() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"exporter\":{\"type\":\"local_index\",\"delete_after_days\":7}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        UpdateQueryInsightsSettingsRequest request = RestUpdateQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        Map<String, Object> persistent = (Map<String, Object>) request.getSettings().get("persistent");
        Map<String, Object> exporter = (Map<String, Object>) persistent.get("exporter");
        assertEquals("local_index", exporter.get("type"));
        assertEquals(7, exporter.get("delete_after_days"));
    }

    /**
     * Test prepareRequest parses flat structure settings correctly
     */
    @SuppressWarnings("unchecked")
    public void testPrepareRequestWithFlatSettings() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"latency\":{\"enabled\":true}}";
        RestRequest restRequest = buildRestRequest(params, content);
        UpdateQueryInsightsSettingsRequest request = RestUpdateQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        Map<String, Object> latency = (Map<String, Object>) request.getSettings().get("latency");
        assertNotNull(latency);
        assertTrue((Boolean) latency.get("enabled"));
    }

    /**
     * Test prepareRequest parses integer values correctly
     */
    @SuppressWarnings("unchecked")
    public void testPrepareRequestWithIntegerValues() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"latency\":{\"top_n_size\":100},\"exporter\":{\"delete_after_days\":30}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        UpdateQueryInsightsSettingsRequest request = RestUpdateQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        Map<String, Object> persistent = (Map<String, Object>) request.getSettings().get("persistent");
        Map<String, Object> latency = (Map<String, Object>) persistent.get("latency");
        assertEquals(100, latency.get("top_n_size"));
    }

    /**
     * Test prepareRequest parses boolean values correctly
     */
    @SuppressWarnings("unchecked")
    public void testPrepareRequestWithBooleanValues() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"latency\":{\"enabled\":true},\"cpu\":{\"enabled\":false}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        UpdateQueryInsightsSettingsRequest request = RestUpdateQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        Map<String, Object> persistent = (Map<String, Object>) request.getSettings().get("persistent");
        Map<String, Object> latency = (Map<String, Object>) persistent.get("latency");
        Map<String, Object> cpu = (Map<String, Object>) persistent.get("cpu");
        assertTrue((Boolean) latency.get("enabled"));
        assertFalse((Boolean) cpu.get("enabled"));
    }

    /**
     * Test prepareRequest parses string values correctly
     */
    @SuppressWarnings("unchecked")
    public void testPrepareRequestWithStringValues() throws IOException {
        Map<String, String> params = new HashMap<>();
        String content = "{\"persistent\":{\"latency\":{\"window_size\":\"30m\"},\"grouping\":{\"group_by\":\"SIMILARITY\"}}}";
        RestRequest restRequest = buildRestRequest(params, content);
        UpdateQueryInsightsSettingsRequest request = RestUpdateQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        Map<String, Object> persistent = (Map<String, Object>) request.getSettings().get("persistent");
        Map<String, Object> latency = (Map<String, Object>) persistent.get("latency");
        assertEquals("30m", latency.get("window_size"));
    }

    /**
     * Test route exact path
     */
    public void testRouteExactPath() {
        RestUpdateQueryInsightsSettingsAction action = new RestUpdateQueryInsightsSettingsAction();
        List<RestHandler.Route> routes = action.routes();
        assertEquals("/_insights/settings", routes.get(0).getPath());
    }

    /**
     * Test prepareRequest without content returns empty settings
     */
    public void testPrepareRequestWithoutContent() throws IOException {
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_insights/settings")
            .build();
        UpdateQueryInsightsSettingsRequest request = RestUpdateQueryInsightsSettingsAction.prepareRequest(restRequest);
        assertNotNull(request);
        assertTrue(request.getSettings().isEmpty());
    }

    /**
     * Test buildRestResponse returns OK status when acknowledged
     */
    public void testBuildRestResponseAcknowledged() throws Exception {
        FakeRestRequest restRequest = buildRestRequestWithoutContent();
        FakeRestChannel channel = new FakeRestChannel(restRequest, true, 1);
        UpdateQueryInsightsSettingsResponse response = new UpdateQueryInsightsSettingsResponse(true);

        RestResponse restResponse = RestUpdateQueryInsightsSettingsAction.buildRestResponse(channel, response);

        assertNotNull(restResponse);
        assertEquals(RestStatus.OK, restResponse.status());
    }

    /**
     * Test buildRestResponse returns OK status when not acknowledged
     */
    public void testBuildRestResponseNotAcknowledged() throws Exception {
        FakeRestRequest restRequest = buildRestRequestWithoutContent();
        FakeRestChannel channel = new FakeRestChannel(restRequest, true, 1);
        UpdateQueryInsightsSettingsResponse response = new UpdateQueryInsightsSettingsResponse(false);

        RestResponse restResponse = RestUpdateQueryInsightsSettingsAction.buildRestResponse(channel, response);

        assertNotNull(restResponse);
        assertEquals(RestStatus.OK, restResponse.status());
    }

    /**
     * Test updateSettingsResponse returns a valid listener
     */
    public void testUpdateSettingsResponseReturnsListener() {
        RestUpdateQueryInsightsSettingsAction action = new RestUpdateQueryInsightsSettingsAction();
        FakeRestRequest restRequest = buildRestRequestWithoutContent();
        FakeRestChannel channel = new FakeRestChannel(restRequest, true, 1);

        RestResponseListener<UpdateQueryInsightsSettingsResponse> listener = action.updateSettingsResponse(channel);

        assertNotNull(listener);
    }

    /**
     * Test updateSettingsResponse listener builds response correctly
     */
    public void testUpdateSettingsResponseListenerBuildsResponse() throws Exception {
        RestUpdateQueryInsightsSettingsAction action = new RestUpdateQueryInsightsSettingsAction();
        FakeRestRequest restRequest = buildRestRequestWithoutContent();
        FakeRestChannel channel = new FakeRestChannel(restRequest, true, 1);
        UpdateQueryInsightsSettingsResponse response = new UpdateQueryInsightsSettingsResponse(true);

        RestResponseListener<UpdateQueryInsightsSettingsResponse> listener = action.updateSettingsResponse(channel);
        RestResponse restResponse = listener.buildResponse(response);

        assertNotNull(restResponse);
        assertEquals(RestStatus.OK, restResponse.status());
    }

    /**
     * Helper method to build a FakeRestRequest with content
     */
    private FakeRestRequest buildRestRequest(Map<String, String> params, String content) throws IOException {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_insights/settings")
            .withParams(params)
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
    }

    /**
     * Helper method to build a FakeRestRequest without content
     */
    private FakeRestRequest buildRestRequestWithoutContent() {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT).withPath("/_insights/settings").build();
    }
}
