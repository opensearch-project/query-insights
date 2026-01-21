/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.settings;

import java.util.HashMap;
import java.util.Map;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.rules.action.settings.GetQueryInsightsSettingsAction;
import org.opensearch.plugin.insights.rules.action.settings.GetQueryInsightsSettingsRequest;
import org.opensearch.plugin.insights.rules.action.settings.GetQueryInsightsSettingsResponse;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to get Query Insights settings from the cluster.
 */
public class TransportGetQueryInsightsSettingsAction extends HandledTransportAction<
    GetQueryInsightsSettingsRequest,
    GetQueryInsightsSettingsResponse> {

    private final ClusterService clusterService;

    /**
     * Constructor for TransportGetQueryInsightsSettingsAction
     *
     * @param transportService The TransportService
     * @param actionFilters The ActionFilters
     * @param clusterService The ClusterService to read settings from
     */
    @Inject
    public TransportGetQueryInsightsSettingsAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final ClusterService clusterService
    ) {
        super(GetQueryInsightsSettingsAction.NAME, transportService, actionFilters, GetQueryInsightsSettingsRequest::new);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(
        final Task task,
        final GetQueryInsightsSettingsRequest request,
        final ActionListener<GetQueryInsightsSettingsResponse> listener
    ) {
        try {
            // Read dynamic cluster settings (persistent + transient merged with node settings)
            final Settings persistentSettings = clusterService.state().metadata().persistentSettings();
            final Settings transientSettings = clusterService.state().metadata().transientSettings();
            final Settings nodeSettings = clusterService.getSettings();

            // Merge settings: transient > persistent > node defaults
            final Settings mergedSettings = Settings.builder().put(nodeSettings).put(persistentSettings).put(transientSettings).build();

            final Map<String, Object> queryInsightsSettings = extractQueryInsightsSettings(mergedSettings, request.getMetricType());
            listener.onResponse(new GetQueryInsightsSettingsResponse(queryInsightsSettings));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Extract Query Insights settings from cluster settings
     *
     * @param settings The cluster settings
     * @param metricType Optional metric type filter
     * @return Map of Query Insights settings
     */
    private Map<String, Object> extractQueryInsightsSettings(final Settings settings, final String metricType) {
        final Map<String, Object> queryInsightsSettings = new HashMap<>();

        // If metricType is specified, only return settings for that metric
        if (metricType == null || "latency".equalsIgnoreCase(metricType)) {
            queryInsightsSettings.put("latency.enabled", QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED.get(settings));
            queryInsightsSettings.put("latency.top_n_size", QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE.get(settings));
            queryInsightsSettings.put("latency.window_size", QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE.get(settings));
        }

        if (metricType == null || "cpu".equalsIgnoreCase(metricType)) {
            queryInsightsSettings.put("cpu.enabled", QueryInsightsSettings.TOP_N_CPU_QUERIES_ENABLED.get(settings));
            queryInsightsSettings.put("cpu.top_n_size", QueryInsightsSettings.TOP_N_CPU_QUERIES_SIZE.get(settings));
            queryInsightsSettings.put("cpu.window_size", QueryInsightsSettings.TOP_N_CPU_QUERIES_WINDOW_SIZE.get(settings));
        }

        if (metricType == null || "memory".equalsIgnoreCase(metricType)) {
            queryInsightsSettings.put("memory.enabled", QueryInsightsSettings.TOP_N_MEMORY_QUERIES_ENABLED.get(settings));
            queryInsightsSettings.put("memory.top_n_size", QueryInsightsSettings.TOP_N_MEMORY_QUERIES_SIZE.get(settings));
            queryInsightsSettings.put("memory.window_size", QueryInsightsSettings.TOP_N_MEMORY_QUERIES_WINDOW_SIZE.get(settings));
        }

        // Always include grouping and exporter settings if no specific metric is requested
        if (metricType == null) {
            queryInsightsSettings.put("grouping.group_by", QueryInsightsSettings.TOP_N_QUERIES_GROUP_BY.get(settings));
            queryInsightsSettings.put("exporter.type", QueryInsightsSettings.TOP_N_EXPORTER_TYPE.get(settings));
            queryInsightsSettings.put("exporter.delete_after_days", QueryInsightsSettings.TOP_N_EXPORTER_DELETE_AFTER.get(settings));
        }

        return queryInsightsSettings;
    }
}
