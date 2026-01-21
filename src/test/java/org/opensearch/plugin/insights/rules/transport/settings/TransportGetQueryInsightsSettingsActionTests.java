/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.settings;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.rules.action.settings.GetQueryInsightsSettingsAction;
import org.opensearch.plugin.insights.rules.action.settings.GetQueryInsightsSettingsRequest;
import org.opensearch.plugin.insights.rules.action.settings.GetQueryInsightsSettingsResponse;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

/**
 * Unit tests for {@link TransportGetQueryInsightsSettingsAction}.
 */
public class TransportGetQueryInsightsSettingsActionTests extends OpenSearchTestCase {

    private TransportService transportService;
    private ActionFilters actionFilters;
    private ClusterService clusterService;
    private TransportGetQueryInsightsSettingsAction action;
    private Task task;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        transportService = mock(TransportService.class);
        actionFilters = mock(ActionFilters.class);
        clusterService = mock(ClusterService.class);
        task = mock(Task.class);

        // Setup cluster settings
        Settings nodeSettings = Settings.builder()
            .put(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED.getKey(), true)
            .put(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE.getKey(), 10)
            .put(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE.getKey(), "5m")
            .put(QueryInsightsSettings.TOP_N_CPU_QUERIES_ENABLED.getKey(), false)
            .put(QueryInsightsSettings.TOP_N_CPU_QUERIES_SIZE.getKey(), 20)
            .put(QueryInsightsSettings.TOP_N_MEMORY_QUERIES_ENABLED.getKey(), true)
            .build();

        when(clusterService.getSettings()).thenReturn(nodeSettings);

        // Setup cluster state with metadata
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.persistentSettings()).thenReturn(Settings.EMPTY);
        when(metadata.transientSettings()).thenReturn(Settings.EMPTY);

        action = new TransportGetQueryInsightsSettingsAction(transportService, actionFilters, clusterService);
    }

    /**
     * Test doExecute with null metric type returns all settings
     */
    @SuppressWarnings("unchecked")
    public void testDoExecuteWithNullMetricType() {
        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest((String) null);
        ActionListener<GetQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(task, request, listener);

        ArgumentCaptor<GetQueryInsightsSettingsResponse> captor = ArgumentCaptor.forClass(GetQueryInsightsSettingsResponse.class);
        verify(listener).onResponse(captor.capture());

        GetQueryInsightsSettingsResponse response = captor.getValue();
        Map<String, Object> settings = response.getSettings();

        // Should include all metric settings
        assertTrue(settings.containsKey("latency.enabled"));
        assertTrue(settings.containsKey("cpu.enabled"));
        assertTrue(settings.containsKey("memory.enabled"));

        // Should include grouping and exporter settings
        assertTrue(settings.containsKey("grouping.group_by"));
        assertTrue(settings.containsKey("exporter.type"));
        assertTrue(settings.containsKey("exporter.delete_after_days"));
    }

    /**
     * Test doExecute with latency metric type returns only latency settings
     */
    @SuppressWarnings("unchecked")
    public void testDoExecuteWithLatencyMetricType() {
        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest("latency");
        ActionListener<GetQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(task, request, listener);

        ArgumentCaptor<GetQueryInsightsSettingsResponse> captor = ArgumentCaptor.forClass(GetQueryInsightsSettingsResponse.class);
        verify(listener).onResponse(captor.capture());

        GetQueryInsightsSettingsResponse response = captor.getValue();
        Map<String, Object> settings = response.getSettings();

        // Should only include latency settings
        assertTrue(settings.containsKey("latency.enabled"));
        assertTrue(settings.containsKey("latency.top_n_size"));
        assertTrue(settings.containsKey("latency.window_size"));

        // Should not include other metrics
        assertFalse(settings.containsKey("cpu.enabled"));
        assertFalse(settings.containsKey("memory.enabled"));

        // Should not include grouping/exporter settings
        assertFalse(settings.containsKey("grouping.group_by"));
        assertFalse(settings.containsKey("exporter.type"));
    }

    /**
     * Test doExecute with CPU metric type
     */
    @SuppressWarnings("unchecked")
    public void testDoExecuteWithCpuMetricType() {
        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest("cpu");
        ActionListener<GetQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(task, request, listener);

        ArgumentCaptor<GetQueryInsightsSettingsResponse> captor = ArgumentCaptor.forClass(GetQueryInsightsSettingsResponse.class);
        verify(listener).onResponse(captor.capture());

        GetQueryInsightsSettingsResponse response = captor.getValue();
        Map<String, Object> settings = response.getSettings();

        // Should only include CPU settings
        assertTrue(settings.containsKey("cpu.enabled"));
        assertTrue(settings.containsKey("cpu.top_n_size"));
        assertTrue(settings.containsKey("cpu.window_size"));

        // Should not include other metrics
        assertFalse(settings.containsKey("latency.enabled"));
        assertFalse(settings.containsKey("memory.enabled"));
    }

    /**
     * Test doExecute with memory metric type
     */
    @SuppressWarnings("unchecked")
    public void testDoExecuteWithMemoryMetricType() {
        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest("memory");
        ActionListener<GetQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(task, request, listener);

        ArgumentCaptor<GetQueryInsightsSettingsResponse> captor = ArgumentCaptor.forClass(GetQueryInsightsSettingsResponse.class);
        verify(listener).onResponse(captor.capture());

        GetQueryInsightsSettingsResponse response = captor.getValue();
        Map<String, Object> settings = response.getSettings();

        // Should only include memory settings
        assertTrue(settings.containsKey("memory.enabled"));
        assertTrue(settings.containsKey("memory.top_n_size"));
        assertTrue(settings.containsKey("memory.window_size"));

        // Should not include other metrics
        assertFalse(settings.containsKey("latency.enabled"));
        assertFalse(settings.containsKey("cpu.enabled"));
    }

    /**
     * Test settings values are correctly retrieved
     */
    @SuppressWarnings("unchecked")
    public void testDoExecuteReturnsCorrectValues() {
        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest((String) null);
        ActionListener<GetQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(task, request, listener);

        ArgumentCaptor<GetQueryInsightsSettingsResponse> captor = ArgumentCaptor.forClass(GetQueryInsightsSettingsResponse.class);
        verify(listener).onResponse(captor.capture());

        GetQueryInsightsSettingsResponse response = captor.getValue();
        Map<String, Object> settings = response.getSettings();

        // Verify specific values from node settings
        assertEquals(true, settings.get("latency.enabled"));
        assertEquals(10, settings.get("latency.top_n_size"));
        assertEquals("5m", settings.get("latency.window_size").toString());
        assertEquals(false, settings.get("cpu.enabled"));
        assertEquals(20, settings.get("cpu.top_n_size"));
        assertEquals(true, settings.get("memory.enabled"));
    }

    /**
     * Test with persistent settings override
     */
    @SuppressWarnings("unchecked")
    public void testDoExecuteWithPersistentSettings() {
        Settings persistentSettings = Settings.builder()
            .put(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED.getKey(), false)
            .put(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE.getKey(), 50)
            .build();

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.persistentSettings()).thenReturn(persistentSettings);
        when(metadata.transientSettings()).thenReturn(Settings.EMPTY);

        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest((String) null);
        ActionListener<GetQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(task, request, listener);

        ArgumentCaptor<GetQueryInsightsSettingsResponse> captor = ArgumentCaptor.forClass(GetQueryInsightsSettingsResponse.class);
        verify(listener).onResponse(captor.capture());

        GetQueryInsightsSettingsResponse response = captor.getValue();
        Map<String, Object> settings = response.getSettings();

        // Persistent settings should override node settings
        assertEquals(false, settings.get("latency.enabled"));
        assertEquals(50, settings.get("latency.top_n_size"));
    }

    /**
     * Test with transient settings override
     */
    @SuppressWarnings("unchecked")
    public void testDoExecuteWithTransientSettings() {
        Settings transientSettings = Settings.builder()
            .put(QueryInsightsSettings.TOP_N_CPU_QUERIES_ENABLED.getKey(), true)
            .put(QueryInsightsSettings.TOP_N_CPU_QUERIES_SIZE.getKey(), 100)
            .build();

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.persistentSettings()).thenReturn(Settings.EMPTY);
        when(metadata.transientSettings()).thenReturn(transientSettings);

        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest((String) null);
        ActionListener<GetQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(task, request, listener);

        ArgumentCaptor<GetQueryInsightsSettingsResponse> captor = ArgumentCaptor.forClass(GetQueryInsightsSettingsResponse.class);
        verify(listener).onResponse(captor.capture());

        GetQueryInsightsSettingsResponse response = captor.getValue();
        Map<String, Object> settings = response.getSettings();

        // Transient settings should override node settings
        assertEquals(true, settings.get("cpu.enabled"));
        assertEquals(100, settings.get("cpu.top_n_size"));
    }

    /**
     * Test case insensitive metric type filtering
     */
    @SuppressWarnings("unchecked")
    public void testDoExecuteWithCaseInsensitiveMetricType() {
        GetQueryInsightsSettingsRequest requestUpper = new GetQueryInsightsSettingsRequest("LATENCY");
        ActionListener<GetQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(task, requestUpper, listener);

        ArgumentCaptor<GetQueryInsightsSettingsResponse> captor = ArgumentCaptor.forClass(GetQueryInsightsSettingsResponse.class);
        verify(listener).onResponse(captor.capture());

        GetQueryInsightsSettingsResponse response = captor.getValue();
        Map<String, Object> settings = response.getSettings();

        // Should work with uppercase
        assertTrue(settings.containsKey("latency.enabled"));
        assertFalse(settings.containsKey("cpu.enabled"));
    }

    /**
     * Test with empty cluster state
     */
    @SuppressWarnings("unchecked")
    public void testDoExecuteWithEmptyClusterState() {
        Settings emptyNodeSettings = Settings.EMPTY;
        when(clusterService.getSettings()).thenReturn(emptyNodeSettings);

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.persistentSettings()).thenReturn(Settings.EMPTY);
        when(metadata.transientSettings()).thenReturn(Settings.EMPTY);

        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest((String) null);
        ActionListener<GetQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(task, request, listener);

        ArgumentCaptor<GetQueryInsightsSettingsResponse> captor = ArgumentCaptor.forClass(GetQueryInsightsSettingsResponse.class);
        verify(listener).onResponse(captor.capture());

        GetQueryInsightsSettingsResponse response = captor.getValue();
        Map<String, Object> settings = response.getSettings();

        // Should return default values
        assertNotNull(settings);
        assertTrue(settings.containsKey("latency.enabled"));
        assertTrue(settings.containsKey("cpu.enabled"));
        assertTrue(settings.containsKey("memory.enabled"));
    }

    /**
     * Test action name
     */
    public void testActionName() {
        assertEquals(GetQueryInsightsSettingsAction.NAME, action.actionName);
    }

    /**
     * Test constructor initializes properly
     */
    public void testConstructor() {
        TransportGetQueryInsightsSettingsAction newAction = new TransportGetQueryInsightsSettingsAction(
            transportService,
            actionFilters,
            clusterService
        );
        assertNotNull(newAction);
    }

    /**
     * Test exception handling in doExecute
     */
    @SuppressWarnings("unchecked")
    public void testDoExecuteWithException() {
        when(clusterService.state()).thenThrow(new RuntimeException("Test exception"));

        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest((String) null);
        ActionListener<GetQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(task, request, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());

        Exception exception = exceptionCaptor.getValue();
        assertTrue(exception instanceof RuntimeException);
        assertEquals("Test exception", exception.getMessage());
    }

    /**
     * Test priority: transient > persistent > node settings
     */
    @SuppressWarnings("unchecked")
    public void testSettingsPriority() {
        // Node settings: enabled = true
        Settings nodeSettings = Settings.builder().put(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED.getKey(), true).build();

        // Persistent settings: enabled = false (should override node)
        Settings persistentSettings = Settings.builder().put(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED.getKey(), false).build();

        // Transient settings: enabled = true (should override persistent)
        Settings transientSettings = Settings.builder().put(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED.getKey(), true).build();

        when(clusterService.getSettings()).thenReturn(nodeSettings);

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.persistentSettings()).thenReturn(persistentSettings);
        when(metadata.transientSettings()).thenReturn(transientSettings);

        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest((String) null);
        ActionListener<GetQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(task, request, listener);

        ArgumentCaptor<GetQueryInsightsSettingsResponse> captor = ArgumentCaptor.forClass(GetQueryInsightsSettingsResponse.class);
        verify(listener).onResponse(captor.capture());

        GetQueryInsightsSettingsResponse response = captor.getValue();
        Map<String, Object> settings = response.getSettings();

        // Transient settings should win
        assertEquals(true, settings.get("latency.enabled"));
    }
}
