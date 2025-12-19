/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.settings;

/*
 * NOTE: Testing the actual settings update flow requires mocking client.admin().cluster().updateSettings()
 * with async callbacks, which is problematic with Mockito. These tests focus on exception handling
 * and constructor validation. The settings mapping logic is tested in integration tests with real cluster clients.
 */

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsRequest;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;

/**
 * Unit tests for {@link TransportUpdateQueryInsightsSettingsAction}
 */
public class TransportUpdateQueryInsightsSettingsActionTests extends OpenSearchTestCase {

    private TransportUpdateQueryInsightsSettingsAction action;
    private TransportService transportService;
    private ActionFilters actionFilters;
    private Client client;
    private ClusterAdminClient clusterAdminClient;

    @Before
    public void setup() {
        transportService = mock(TransportService.class);
        actionFilters = mock(ActionFilters.class);
        client = mock(Client.class);

        AdminClient adminClient = mock(AdminClient.class);
        clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        action = new TransportUpdateQueryInsightsSettingsAction(transportService, actionFilters, client);
    }

    public void testConstructor() {
        assertNotNull(action);
    }

    public void testUpdateSettingsWithException() {
        UpdateQueryInsightsSettingsRequest request = mock(UpdateQueryInsightsSettingsRequest.class);
        when(request.getSettings()).thenThrow(new RuntimeException("Request processing failed"));

        @SuppressWarnings("unchecked")
        ActionListener<UpdateQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(null, request, listener);

        verify(listener).onFailure(any(RuntimeException.class));
    }

    public void testUpdateSettingsWithNullPointerException() {
        UpdateQueryInsightsSettingsRequest request = mock(UpdateQueryInsightsSettingsRequest.class);
        when(request.getSettings()).thenThrow(new NullPointerException("Null value encountered"));

        @SuppressWarnings("unchecked")
        ActionListener<UpdateQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(null, request, listener);

        verify(listener).onFailure(any(NullPointerException.class));
    }

    public void testUpdateSettingsWithIllegalArgumentException() {
        UpdateQueryInsightsSettingsRequest request = mock(UpdateQueryInsightsSettingsRequest.class);
        when(request.getSettings()).thenThrow(new IllegalArgumentException("Invalid setting value"));

        @SuppressWarnings("unchecked")
        ActionListener<UpdateQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(null, request, listener);

        verify(listener).onFailure(any(IllegalArgumentException.class));
    }

    /**
     * Test that mapSettings properly handles latency metric settings.
     */
    public void testMapSettingsWithLatencyMetric() {
        Map<String, Object> settings = new HashMap<>();
        Map<String, Object> latencySettings = new HashMap<>();
        latencySettings.put("enabled", true);
        latencySettings.put("top_n_size", 10);
        latencySettings.put("window_size", "5m");
        settings.put("latency", latencySettings);

        Settings.Builder builder = Settings.builder();

        action.mapSettings(settings, builder);

        Settings result = builder.build();

        // Verify mapped settings
        assertEquals("true", result.get("search.insights.top_queries.latency.enabled"));
        assertEquals("10", result.get("search.insights.top_queries.latency.top_n_size"));
        assertEquals("5m", result.get("search.insights.top_queries.latency.window_size"));
    }

    /**
     * Test that mapSettings properly handles cpu and memory metric settings.
     */
    public void testMapSettingsWithCpuAndMemoryMetrics() {
        Map<String, Object> settings = new HashMap<>();

        Map<String, Object> cpuSettings = new HashMap<>();
        cpuSettings.put("enabled", false);
        cpuSettings.put("top_n_size", 20);
        settings.put("cpu", cpuSettings);

        Map<String, Object> memorySettings = new HashMap<>();
        memorySettings.put("enabled", true);
        memorySettings.put("window_size", "10m");
        settings.put("memory", memorySettings);

        Settings.Builder builder = Settings.builder();

        action.mapSettings(settings, builder);

        Settings result = builder.build();

        // Verify mapped settings
        assertEquals("false", result.get("search.insights.top_queries.cpu.enabled"));
        assertEquals("20", result.get("search.insights.top_queries.cpu.top_n_size"));
        assertEquals("true", result.get("search.insights.top_queries.memory.enabled"));
        assertEquals("10m", result.get("search.insights.top_queries.memory.window_size"));
    }

    /**
     * Test that mapSettings properly handles grouping settings.
     */
    public void testMapSettingsWithGroupingSettings() {
        Map<String, Object> settings = new HashMap<>();
        Map<String, Object> groupingSettings = new HashMap<>();
        groupingSettings.put("group_by", "similarity");
        settings.put("grouping", groupingSettings);

        Settings.Builder builder = Settings.builder();

        action.mapSettings(settings, builder);

        Settings result = builder.build();

        assertEquals("similarity", result.get("search.insights.top_queries.grouping.group_by"));
    }

    /**
     * Test that mapSettings properly handles exporter settings.
     */
    public void testMapSettingsWithExporterSettings() {
        Map<String, Object> settings = new HashMap<>();
        Map<String, Object> exporterSettings = new HashMap<>();
        exporterSettings.put("type", "local_index");
        exporterSettings.put("delete_after_days", 30);
        settings.put("exporter", exporterSettings);

        Settings.Builder builder = Settings.builder();

        action.mapSettings(settings, builder);

        Settings result = builder.build();

        assertEquals("local_index", result.get("search.insights.top_queries.exporter.type"));
        assertEquals("30", result.get("search.insights.top_queries.exporter.delete_after_days"));
    }

    /**
     * Test that mapSettings properly handles nested persistent settings structure.
     */
    public void testMapSettingsWithNestedPersistentStructure() {
        Map<String, Object> settings = new HashMap<>();
        Map<String, Object> persistent = new HashMap<>();

        Map<String, Object> latencySettings = new HashMap<>();
        latencySettings.put("enabled", true);
        latencySettings.put("top_n_size", 15);
        persistent.put("latency", latencySettings);

        settings.put("persistent", persistent);

        Settings.Builder builder = Settings.builder();

        action.mapSettings(settings, builder);

        Settings result = builder.build();

        // Should correctly extract from nested structure
        assertEquals("true", result.get("search.insights.top_queries.latency.enabled"));
        assertEquals("15", result.get("search.insights.top_queries.latency.top_n_size"));
    }

    /**
     * Test that mapSettings handles all three metric types correctly.
     */
    public void testMapSettingsWithAllMetricTypes() {
        Map<String, Object> settings = new HashMap<>();

        Map<String, Object> latencySettings = new HashMap<>();
        latencySettings.put("enabled", true);
        latencySettings.put("top_n_size", 5);
        latencySettings.put("window_size", "1m");
        settings.put("latency", latencySettings);

        Map<String, Object> cpuSettings = new HashMap<>();
        cpuSettings.put("enabled", false);
        cpuSettings.put("top_n_size", 10);
        cpuSettings.put("window_size", "5m");
        settings.put("cpu", cpuSettings);

        Map<String, Object> memorySettings = new HashMap<>();
        memorySettings.put("enabled", true);
        memorySettings.put("top_n_size", 15);
        memorySettings.put("window_size", "10m");
        settings.put("memory", memorySettings);

        Settings.Builder builder = Settings.builder();

        action.mapSettings(settings, builder);

        Settings result = builder.build();

        // Verify all three metrics are mapped correctly
        assertEquals("true", result.get("search.insights.top_queries.latency.enabled"));
        assertEquals("5", result.get("search.insights.top_queries.latency.top_n_size"));
        assertEquals("1m", result.get("search.insights.top_queries.latency.window_size"));

        assertEquals("false", result.get("search.insights.top_queries.cpu.enabled"));
        assertEquals("10", result.get("search.insights.top_queries.cpu.top_n_size"));
        assertEquals("5m", result.get("search.insights.top_queries.cpu.window_size"));

        assertEquals("true", result.get("search.insights.top_queries.memory.enabled"));
        assertEquals("15", result.get("search.insights.top_queries.memory.top_n_size"));
        assertEquals("10m", result.get("search.insights.top_queries.memory.window_size"));
    }

    /**
     * Test that mapSettings handles mixed settings (metrics, grouping, exporter).
     */
    public void testMapSettingsWithComplexMixedSettings() {
        Map<String, Object> settings = new HashMap<>();

        // Metric settings
        Map<String, Object> latencySettings = new HashMap<>();
        latencySettings.put("enabled", true);
        latencySettings.put("top_n_size", 25);
        settings.put("latency", latencySettings);

        // Grouping settings
        Map<String, Object> groupingSettings = new HashMap<>();
        groupingSettings.put("group_by", "none");
        settings.put("grouping", groupingSettings);

        // Exporter settings
        Map<String, Object> exporterSettings = new HashMap<>();
        exporterSettings.put("type", "debug");
        exporterSettings.put("delete_after_days", 7);
        settings.put("exporter", exporterSettings);

        Settings.Builder builder = Settings.builder();

        action.mapSettings(settings, builder);

        Settings result = builder.build();

        // Verify all settings categories are mapped correctly
        assertEquals("true", result.get("search.insights.top_queries.latency.enabled"));
        assertEquals("25", result.get("search.insights.top_queries.latency.top_n_size"));
        assertEquals("none", result.get("search.insights.top_queries.grouping.group_by"));
        assertEquals("debug", result.get("search.insights.top_queries.exporter.type"));
        assertEquals("7", result.get("search.insights.top_queries.exporter.delete_after_days"));
    }

    /**
     * Test that processMetricSettings handles partial field sets correctly.
     */
    public void testProcessMetricSettingsWithPartialFields() {
        Settings.Builder builder = Settings.builder();

        // Test with only enabled field
        Map<String, Object> latencySettings = new HashMap<>();
        latencySettings.put("enabled", true);

        action.processMetricSettings("latency", latencySettings, builder);

        Settings result = builder.build();
        assertEquals("true", result.get("search.insights.top_queries.latency.enabled"));
        assertNull(result.get("search.insights.top_queries.latency.top_n_size"));
        assertNull(result.get("search.insights.top_queries.latency.window_size"));
    }

    /**
     * Test that processMetricSettings handles all fields correctly.
     */
    public void testProcessMetricSettingsWithAllFields() {
        Settings.Builder builder = Settings.builder();

        Map<String, Object> cpuSettings = new HashMap<>();
        cpuSettings.put("enabled", false);
        cpuSettings.put("top_n_size", 50);
        cpuSettings.put("window_size", "30m");

        action.processMetricSettings("cpu", cpuSettings, builder);

        Settings result = builder.build();
        assertEquals("false", result.get("search.insights.top_queries.cpu.enabled"));
        assertEquals("50", result.get("search.insights.top_queries.cpu.top_n_size"));
        assertEquals("30m", result.get("search.insights.top_queries.cpu.window_size"));
    }

    /**
     * Test that processSettings correctly iterates through all metric types.
     */
    public void testProcessSettingsIteratesThroughAllMetrics() {
        Map<String, Object> settings = new HashMap<>();

        Map<String, Object> latencySettings = new HashMap<>();
        latencySettings.put("enabled", true);
        settings.put("latency", latencySettings);

        Map<String, Object> cpuSettings = new HashMap<>();
        cpuSettings.put("top_n_size", 20);
        settings.put("cpu", cpuSettings);

        Map<String, Object> memorySettings = new HashMap<>();
        memorySettings.put("window_size", "15m");
        settings.put("memory", memorySettings);

        Settings.Builder builder = Settings.builder();

        action.processSettings(settings, builder);

        Settings result = builder.build();

        // Verify all three metrics were processed
        assertEquals("true", result.get("search.insights.top_queries.latency.enabled"));
        assertEquals("20", result.get("search.insights.top_queries.cpu.top_n_size"));
        assertEquals("15m", result.get("search.insights.top_queries.memory.window_size"));
    }

    /**
     * Test that processSettings handles grouping and exporter settings.
     */
    public void testProcessSettingsWithGroupingAndExporter() {
        Map<String, Object> settings = new HashMap<>();

        Map<String, Object> groupingSettings = new HashMap<>();
        groupingSettings.put("group_by", "none");
        settings.put("grouping", groupingSettings);

        Map<String, Object> exporterSettings = new HashMap<>();
        exporterSettings.put("type", "local_index");
        exporterSettings.put("delete_after_days", 14);
        settings.put("exporter", exporterSettings);

        Settings.Builder builder = Settings.builder();

        action.processSettings(settings, builder);

        Settings result = builder.build();

        assertEquals("none", result.get("search.insights.top_queries.grouping.group_by"));
        assertEquals("local_index", result.get("search.insights.top_queries.exporter.type"));
        assertEquals("14", result.get("search.insights.top_queries.exporter.delete_after_days"));
    }

    /**
     * Test doExecute creates proper ClusterUpdateSettingsRequest.
     * Verifies the request is built and sent to cluster admin client.
     */
    public void testDoExecuteBuildsClusterRequest() {
        Map<String, Object> settings = new HashMap<>();
        Map<String, Object> latencySettings = new HashMap<>();
        latencySettings.put("enabled", true);
        settings.put("latency", latencySettings);

        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest(settings);

        @SuppressWarnings("unchecked")
        ActionListener<UpdateQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(null, request, listener);

        // Verify that updateSettings was called on clusterAdminClient
        verify(clusterAdminClient).updateSettings(any(ClusterUpdateSettingsRequest.class), any());
    }
}
