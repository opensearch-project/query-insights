/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.settings;

import java.io.IOException;
import java.util.Map;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/**
 * Integration tests for Query Insights Settings REST API
 */
public class QueryInsightsSettingsRestIT extends QueryInsightsRestTestCase {

    /**
     * Test GET settings endpoint without any settings configured
     */
    public void testGetSettingsWithDefaults() throws IOException {
        // Get all settings
        Request request = new Request("GET", "/_insights/settings");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = parseResponseMap(response);
        assertNotNull(responseMap);

        // Verify nested settings structure
        @SuppressWarnings("unchecked")
        Map<String, Object> persistentSettings = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull(persistentSettings);

        @SuppressWarnings("unchecked")
        Map<String, Object> latencySettings = (Map<String, Object>) persistentSettings.get("latency");
        assertNotNull(latencySettings);
        assertEquals(true, latencySettings.get("enabled")); // Default is true
        assertEquals(QueryInsightsSettings.DEFAULT_TOP_N_SIZE, latencySettings.get("top_n_size"));
    }

    /**
     * Test GET settings for specific metric type
     */
    public void testGetSettingsForLatencyMetric() throws IOException {
        // Set some latency settings first
        updateClusterSettings(
            Map.of(
                QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED.getKey(),
                true,
                QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE.getKey(),
                15,
                QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE.getKey(),
                "10m"
            )
        );

        // Get latency-specific settings
        Request request = new Request("GET", "/_insights/settings/latency");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = parseResponseMap(response);
        assertNotNull(responseMap);

        // Get persistent settings first
        @SuppressWarnings("unchecked")
        Map<String, Object> persistentSettings = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull(persistentSettings);

        // Then get latency settings
        @SuppressWarnings("unchecked")
        Map<String, Object> latencySettings = (Map<String, Object>) persistentSettings.get("latency");
        assertNotNull(latencySettings);

        assertEquals(true, latencySettings.get("enabled"));
        assertEquals(15, latencySettings.get("top_n_size"));
        assertEquals("10m", latencySettings.get("window_size"));
    }

    /**
     * Test PUT settings endpoint to update latency settings
     */
    public void testUpdateLatencySettings() throws IOException {
        Request request = new Request("PUT", "/_insights/settings");
        request.setJsonEntity(
            "{" + "\"latency\": {" + "  \"enabled\": true," + "  \"top_n_size\": 20," + "  \"window_size\": \"5m\"" + "}" + "}"
        );

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = parseResponseMap(response);
        assertEquals(true, responseMap.get("acknowledged"));

        // Verify settings were applied by reading from Query Insights API
        Request getRequest = new Request("GET", "/_insights/settings/latency");
        Response getResponse = client().performRequest(getRequest);
        Map<String, Object> getResponseMap = parseResponseMap(getResponse);

        @SuppressWarnings("unchecked")
        Map<String, Object> persistentSettings = (Map<String, Object>) getResponseMap.get("persistent");
        assertNotNull(persistentSettings);

        @SuppressWarnings("unchecked")
        Map<String, Object> latencySettings = (Map<String, Object>) persistentSettings.get("latency");
        assertNotNull(latencySettings);

        assertEquals(true, latencySettings.get("enabled"));
        assertEquals(20, latencySettings.get("top_n_size"));
        assertEquals("5m", latencySettings.get("window_size"));
    }

    /**
     * Test PUT settings endpoint to update CPU settings
     */
    public void testUpdateCpuSettings() throws IOException {
        Request request = new Request("PUT", "/_insights/settings");
        request.setJsonEntity("{" + "\"cpu\": {" + "  \"enabled\": true," + "  \"top_n_size\": 10" + "}" + "}");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Verify settings
        Request getRequest = new Request("GET", "/_insights/settings/cpu");
        Response getResponse = client().performRequest(getRequest);
        Map<String, Object> responseMap = parseResponseMap(getResponse);

        // Get persistent settings first
        @SuppressWarnings("unchecked")
        Map<String, Object> persistentSettings = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull(persistentSettings);

        // Then get CPU settings
        @SuppressWarnings("unchecked")
        Map<String, Object> cpuSettings = (Map<String, Object>) persistentSettings.get("cpu");
        assertNotNull(cpuSettings);

        assertEquals(true, cpuSettings.get("enabled"));
        assertEquals(10, cpuSettings.get("top_n_size"));
    }

    /**
     * Test PUT settings endpoint to update memory settings
     */
    public void testUpdateMemorySettings() throws IOException {
        Request request = new Request("PUT", "/_insights/settings");
        request.setJsonEntity("{" + "\"memory\": {" + "  \"enabled\": false," + "  \"window_size\": \"30m\"" + "}" + "}");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Verify settings
        Request getRequest = new Request("GET", "/_insights/settings/memory");
        Response getResponse = client().performRequest(getRequest);
        Map<String, Object> responseMap = parseResponseMap(getResponse);

        // Get persistent settings first
        @SuppressWarnings("unchecked")
        Map<String, Object> persistentSettings = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull(persistentSettings);

        // Then get memory settings
        @SuppressWarnings("unchecked")
        Map<String, Object> memorySettings = (Map<String, Object>) persistentSettings.get("memory");
        assertNotNull(memorySettings);

        assertEquals(false, memorySettings.get("enabled"));
        assertEquals("30m", memorySettings.get("window_size"));
    }

    /**
     * Test PUT settings with multiple metrics at once
     */
    public void testUpdateMultipleMetrics() throws IOException {
        Request request = new Request("PUT", "/_insights/settings");
        request.setJsonEntity(
            "{"
                + "\"latency\": {"
                + "  \"enabled\": true,"
                + "  \"top_n_size\": 25"
                + "},"
                + "\"cpu\": {"
                + "  \"enabled\": true,"
                + "  \"top_n_size\": 15"
                + "},"
                + "\"memory\": {"
                + "  \"enabled\": true,"
                + "  \"window_size\": \"1m\""
                + "}"
                + "}"
        );

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Verify all settings
        Request getRequest = new Request("GET", "/_insights/settings");
        Response getResponse = client().performRequest(getRequest);
        Map<String, Object> responseMap = parseResponseMap(getResponse);

        // Get persistent settings first
        @SuppressWarnings("unchecked")
        Map<String, Object> persistentSettings = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull(persistentSettings);

        // Get latency settings
        @SuppressWarnings("unchecked")
        Map<String, Object> latencySettings = (Map<String, Object>) persistentSettings.get("latency");
        assertNotNull(latencySettings);
        assertEquals(true, latencySettings.get("enabled"));
        assertEquals(25, latencySettings.get("top_n_size"));

        // Get CPU settings
        @SuppressWarnings("unchecked")
        Map<String, Object> cpuSettings = (Map<String, Object>) persistentSettings.get("cpu");
        assertNotNull(cpuSettings);
        assertEquals(true, cpuSettings.get("enabled"));
        assertEquals(15, cpuSettings.get("top_n_size"));

        // Get memory settings
        @SuppressWarnings("unchecked")
        Map<String, Object> memorySettings = (Map<String, Object>) persistentSettings.get("memory");
        assertNotNull(memorySettings);
        assertEquals(true, memorySettings.get("enabled"));
        assertEquals("1m", memorySettings.get("window_size"));
    }

    /**
     * Test PUT settings with grouping configuration
     */
    public void testUpdateGroupingSettings() throws IOException {
        Request request = new Request("PUT", "/_insights/settings");
        request.setJsonEntity("{" + "\"grouping\": {" + "  \"group_by\": \"similarity\"" + "}" + "}");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Verify settings were applied by reading from Query Insights API
        Request getRequest = new Request("GET", "/_insights/settings");
        Response getResponse = client().performRequest(getRequest);
        Map<String, Object> responseMap = parseResponseMap(getResponse);

        // Get persistent settings first
        @SuppressWarnings("unchecked")
        Map<String, Object> persistentSettings = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull(persistentSettings);

        // Get grouping settings
        @SuppressWarnings("unchecked")
        Map<String, Object> groupingSettings = (Map<String, Object>) persistentSettings.get("grouping");
        assertNotNull(groupingSettings);

        assertEquals("similarity", groupingSettings.get("group_by"));
    }

    /**
     * Test PUT settings with exporter configuration
     */
    public void testUpdateExporterSettings() throws IOException {
        Request request = new Request("PUT", "/_insights/settings");
        request.setJsonEntity("{" + "\"exporter\": {" + "  \"type\": \"local_index\"," + "  \"delete_after_days\": 30" + "}" + "}");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Verify settings were applied by reading from Query Insights API
        Request getRequest = new Request("GET", "/_insights/settings");
        Response getResponse = client().performRequest(getRequest);
        Map<String, Object> responseMap = parseResponseMap(getResponse);

        // Get persistent settings first
        @SuppressWarnings("unchecked")
        Map<String, Object> persistentSettings = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull(persistentSettings);

        // Get exporter settings
        @SuppressWarnings("unchecked")
        Map<String, Object> exporterSettings = (Map<String, Object>) persistentSettings.get("exporter");
        assertNotNull(exporterSettings);

        assertEquals("local_index", exporterSettings.get("type"));
        assertEquals(30, exporterSettings.get("delete_after_days"));
    }

    /**
     * Test settings priority: transient > persistent
     */
    public void testSettingsPriority() throws IOException {
        // Set persistent setting
        Request persistentRequest = new Request("PUT", "/_cluster/settings");
        persistentRequest.setJsonEntity(
            "{" + "\"persistent\": {" + "\"" + QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED.getKey() + "\": true" + "}" + "}"
        );
        client().performRequest(persistentRequest);

        // Set transient setting that overrides
        Request transientRequest = new Request("PUT", "/_cluster/settings");
        transientRequest.setJsonEntity(
            "{" + "\"transient\": {" + "\"" + QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED.getKey() + "\": false" + "}" + "}"
        );
        client().performRequest(transientRequest);

        // Get settings - should show transient value
        Request request = new Request("GET", "/_insights/settings/latency");
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = parseResponseMap(response);

        // Get persistent settings first
        @SuppressWarnings("unchecked")
        Map<String, Object> persistentSettings = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull(persistentSettings);

        // Get latency settings
        @SuppressWarnings("unchecked")
        Map<String, Object> latencySettings = (Map<String, Object>) persistentSettings.get("latency");
        assertNotNull(latencySettings);

        assertEquals(false, latencySettings.get("enabled"));
    }

    /**
     * Test invalid metric type (currently no validation, returns empty for unknown metrics)
     */
    public void testGetSettingsWithInvalidMetricType() throws IOException {
        Request request = new Request("GET", "/_insights/settings/invalid_metric");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Invalid metric types don't cause errors, they just don't match any settings
        Map<String, Object> responseMap = parseResponseMap(response);
        assertNotNull(responseMap);
    }

    /**
     * Test invalid settings values
     */
    public void testUpdateSettingsWithInvalidValues() throws IOException {
        Request request = new Request("PUT", "/_insights/settings");
        request.setJsonEntity(
            "{"
                + "\"latency\": {"
                + "  \"top_n_size\": -1"  // Invalid: negative value
                + "}"
                + "}"
        );

        try {
            client().performRequest(request);
            fail("Expected ResponseException for invalid top_n_size");
        } catch (ResponseException e) {
            assertTrue(e.getResponse().getStatusLine().getStatusCode() >= 400);
        }
    }

    /**
     * Test updating settings multiple times
     */
    public void testUpdateSettingsMultipleTimes() throws IOException {
        // First update
        Request request1 = new Request("PUT", "/_insights/settings");
        request1.setJsonEntity("{\"latency\": {\"enabled\": true}}");
        client().performRequest(request1);

        // Second update
        Request request2 = new Request("PUT", "/_insights/settings");
        request2.setJsonEntity("{\"latency\": {\"top_n_size\": 50}}");
        Response response = client().performRequest(request2);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Verify both settings are applied
        Request getRequest = new Request("GET", "/_insights/settings/latency");
        Response getResponse = client().performRequest(getRequest);
        Map<String, Object> responseMap = parseResponseMap(getResponse);

        // Get persistent settings first
        @SuppressWarnings("unchecked")
        Map<String, Object> persistentSettings = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull(persistentSettings);

        // Get latency settings
        @SuppressWarnings("unchecked")
        Map<String, Object> latencySettings = (Map<String, Object>) persistentSettings.get("latency");
        assertNotNull(latencySettings);

        assertEquals(true, latencySettings.get("enabled"));
        assertEquals(50, latencySettings.get("top_n_size"));
    }

    /**
     * Test resetting settings to defaults
     */
    public void testResetSettings() throws IOException {
        // Set some custom values
        Request updateRequest = new Request("PUT", "/_insights/settings");
        updateRequest.setJsonEntity("{" + "\"latency\": {" + "  \"enabled\": true," + "  \"top_n_size\": 50" + "}" + "}");
        client().performRequest(updateRequest);

        // Reset by setting to null
        Request resetRequest = new Request("PUT", "/_cluster/settings");
        resetRequest.setJsonEntity(
            "{"
                + "\"persistent\": {"
                + "\""
                + QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED.getKey()
                + "\": null,"
                + "\""
                + QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE.getKey()
                + "\": null"
                + "}"
                + "}"
        );
        client().performRequest(resetRequest);

        // Verify reset to defaults
        Request getRequest = new Request("GET", "/_insights/settings/latency");
        Response getResponse = client().performRequest(getRequest);
        Map<String, Object> responseMap = parseResponseMap(getResponse);

        // Get persistent settings first
        @SuppressWarnings("unchecked")
        Map<String, Object> persistentSettings = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull(persistentSettings);

        // Get latency settings
        @SuppressWarnings("unchecked")
        Map<String, Object> latencySettings = (Map<String, Object>) persistentSettings.get("latency");
        assertNotNull(latencySettings);

        assertEquals(true, latencySettings.get("enabled")); // Defaults to node settings (true)
        assertEquals(QueryInsightsSettings.DEFAULT_TOP_N_SIZE, latencySettings.get("top_n_size"));
    }

    // Helper methods

    private Map<String, Object> parseResponseMap(Response response) throws IOException {
        return JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getPersistentSettings(Response response) throws IOException {
        Map<String, Object> responseMap = parseResponseMap(response);
        return (Map<String, Object>) responseMap.get("persistent");
    }

    private Map<String, Object> getClusterSettings() throws IOException {
        Request request = new Request("GET", "/_cluster/settings");
        request.addParameter("include_defaults", "true");
        Response response = client().performRequest(request);
        return parseResponseMap(response);
    }

    private void updateClusterSettings(Map<String, Object> settings) throws IOException {
        StringBuilder json = new StringBuilder("{\"persistent\":{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            if (!first) {
                json.append(",");
            }
            first = false;
            json.append("\"").append(entry.getKey()).append("\":");
            if (entry.getValue() instanceof String) {
                json.append("\"").append(entry.getValue()).append("\"");
            } else {
                json.append(entry.getValue());
            }
        }
        json.append("}}");

        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(json.toString());
        client().performRequest(request);
    }
}
