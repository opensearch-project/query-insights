/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.settings;

import java.util.Map;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsAction;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsRequest;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Transport action to update Query Insights settings in the cluster.
 */
public class TransportUpdateQueryInsightsSettingsAction extends HandledTransportAction<
    UpdateQueryInsightsSettingsRequest,
    UpdateQueryInsightsSettingsResponse> {

    private final Client client;

    /**
     * Constructor for TransportUpdateQueryInsightsSettingsAction
     *
     * @param transportService The TransportService
     * @param actionFilters The ActionFilters
     * @param client The Client to update cluster settings
     */
    @Inject
    public TransportUpdateQueryInsightsSettingsAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final Client client
    ) {
        super(UpdateQueryInsightsSettingsAction.NAME, transportService, actionFilters, UpdateQueryInsightsSettingsRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(
        final Task task,
        final UpdateQueryInsightsSettingsRequest request,
        final ActionListener<UpdateQueryInsightsSettingsResponse> listener
    ) {
        try {
            // Build cluster settings update request with ONLY Query Insights settings
            final Settings.Builder settingsBuilder = Settings.builder();
            final Map<String, Object> requestedSettings = request.getSettings();

            // Map the simplified keys to actual setting keys
            processSettings(requestedSettings, settingsBuilder);

            // Write to both persistent and transient settings
            // Persistent: survive cluster restart
            // Transient: take immediate effect (highest precedence)
            final Settings newSettings = settingsBuilder.build();
            final ClusterUpdateSettingsRequest clusterRequest = new ClusterUpdateSettingsRequest().persistentSettings(newSettings)
                .transientSettings(newSettings);

            client.admin()
                .cluster()
                .updateSettings(
                    clusterRequest,
                    ActionListener.wrap(
                        (ClusterUpdateSettingsResponse response) -> listener.onResponse(
                            new UpdateQueryInsightsSettingsResponse(response.isAcknowledged())
                        ),
                        listener::onFailure
                    )
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Process settings and map them to actual setting keys
     */
    @SuppressWarnings("unchecked")
    void processSettings(final Map<String, Object> settings, final Settings.Builder builder) {
        // Process metric-specific settings
        for (String metricType : new String[] { "latency", "cpu", "memory" }) {
            if (settings.containsKey(metricType)) {
                Map<String, Object> metricSettings = (Map<String, Object>) settings.get(metricType);
                processMetricSettings(metricType, metricSettings, builder);
            }
        }

        // Process grouping settings
        if (settings.containsKey("grouping")) {
            Map<String, Object> groupingSettings = (Map<String, Object>) settings.get("grouping");
            if (groupingSettings.containsKey("group_by")) {
                builder.put("search.insights.top_queries.grouping.group_by", groupingSettings.get("group_by").toString());
            }
        }

        // Process exporter settings
        if (settings.containsKey("exporter")) {
            Map<String, Object> exporterSettings = (Map<String, Object>) settings.get("exporter");
            if (exporterSettings.containsKey("type")) {
                builder.put("search.insights.top_queries.exporter.type", exporterSettings.get("type").toString());
            }
            if (exporterSettings.containsKey("delete_after_days")) {
                builder.put("search.insights.top_queries.exporter.delete_after_days", exporterSettings.get("delete_after_days").toString());
            }
        }
    }

    /**
     * Process settings for a specific metric type
     */
    void processMetricSettings(final String metricType, final Map<String, Object> metricSettings, final Settings.Builder builder) {
        if (metricSettings.containsKey("enabled")) {
            builder.put("search.insights.top_queries." + metricType + ".enabled", metricSettings.get("enabled").toString());
        }
        if (metricSettings.containsKey("top_n_size")) {
            builder.put("search.insights.top_queries." + metricType + ".top_n_size", metricSettings.get("top_n_size").toString());
        }
        if (metricSettings.containsKey("window_size")) {
            builder.put("search.insights.top_queries." + metricType + ".window_size", metricSettings.get("window_size").toString());
        }
    }
}
