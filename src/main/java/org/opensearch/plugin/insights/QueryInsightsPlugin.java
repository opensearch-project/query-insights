/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterFactory;
import org.opensearch.plugin.insights.core.listener.FinishedQueriesListener;
import org.opensearch.plugin.insights.core.listener.QueryInsightsListener;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReaderFactory;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.health_stats.HealthStatsAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesAction;
import org.opensearch.plugin.insights.rules.action.settings.GetQueryInsightsSettingsAction;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsAction;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesAction;
import org.opensearch.plugin.insights.rules.resthandler.health_stats.RestHealthStatsAction;
import org.opensearch.plugin.insights.rules.resthandler.live_queries.RestLiveQueriesAction;
import org.opensearch.plugin.insights.rules.resthandler.settings.RestGetQueryInsightsSettingsAction;
import org.opensearch.plugin.insights.rules.resthandler.settings.RestUpdateQueryInsightsSettingsAction;
import org.opensearch.plugin.insights.rules.resthandler.top_queries.RestTopQueriesAction;
import org.opensearch.plugin.insights.rules.transport.health_stats.TransportHealthStatsAction;
import org.opensearch.plugin.insights.rules.transport.live_queries.TransportLiveQueriesAction;
import org.opensearch.plugin.insights.rules.transport.settings.TransportGetQueryInsightsSettingsAction;
import org.opensearch.plugin.insights.rules.transport.settings.TransportUpdateQueryInsightsSettingsAction;
import org.opensearch.plugin.insights.rules.transport.top_queries.TransportTopQueriesAction;
import org.opensearch.plugin.insights.settings.QueryCategorizationSettings;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryAwarePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

/**
 * Plugin class for Query Insights.
 */
public class QueryInsightsPlugin extends Plugin implements ActionPlugin, TelemetryAwarePlugin {
    /**
     * Default constructor
     */
    public QueryInsightsPlugin() {}

    @Override
    public Collection<Object> createComponents(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry namedWriteableRegistry,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<RepositoriesService> repositoriesServiceSupplier,
        final Tracer tracer,
        final MetricsRegistry metricsRegistry
    ) {
        // initialize operational metrics counters
        OperationalMetricsCounter.initialize(clusterService.getClusterName().toString(), metricsRegistry);
        // create top n queries service
        final QueryInsightsService queryInsightsService = new QueryInsightsService(
            clusterService,
            threadPool,
            client,
            metricsRegistry,
            xContentRegistry,
            new QueryInsightsExporterFactory(client, clusterService),
            new QueryInsightsReaderFactory(client)
        );
        QueryInsightsListener queryInsightsListener = new QueryInsightsListener(clusterService, queryInsightsService, threadPool);
        FinishedQueriesListener finishedQueriesListener = new FinishedQueriesListener(clusterService, queryInsightsService, threadPool);
        queryInsightsService.setQueryInsightsListener(queryInsightsListener);
        queryInsightsService.setFinishedQueriesListener(finishedQueriesListener);
        return List.of(queryInsightsService, queryInsightsListener, finishedQueriesListener);
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
        return List.of(
            new ScalingExecutorBuilder(
                QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR,
                1,
                Math.min((OpenSearchExecutors.allocatedProcessors(settings) + 1) / 2, QueryInsightsSettings.MAX_THREAD_COUNT),
                TimeValue.timeValueMinutes(5)
            )
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        final Settings settings,
        final RestController restController,
        final ClusterSettings clusterSettings,
        final IndexScopedSettings indexScopedSettings,
        final SettingsFilter settingsFilter,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(
            new RestTopQueriesAction(),
            new RestHealthStatsAction(),
            new RestLiveQueriesAction(),
            new RestGetQueryInsightsSettingsAction(),
            new RestUpdateQueryInsightsSettingsAction()
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionPlugin.ActionHandler<>(TopQueriesAction.INSTANCE, TransportTopQueriesAction.class),
            new ActionPlugin.ActionHandler<>(HealthStatsAction.INSTANCE, TransportHealthStatsAction.class),
            new ActionPlugin.ActionHandler<>(LiveQueriesAction.INSTANCE, TransportLiveQueriesAction.class),
            new ActionPlugin.ActionHandler<>(GetQueryInsightsSettingsAction.INSTANCE, TransportGetQueryInsightsSettingsAction.class),
            new ActionPlugin.ActionHandler<>(UpdateQueryInsightsSettingsAction.INSTANCE, TransportUpdateQueryInsightsSettingsAction.class)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            // Settings for top N queries
            QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED,
            QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE,
            QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE,
            QueryInsightsSettings.TOP_N_CPU_QUERIES_ENABLED,
            QueryInsightsSettings.TOP_N_CPU_QUERIES_SIZE,
            QueryInsightsSettings.TOP_N_CPU_QUERIES_WINDOW_SIZE,
            QueryInsightsSettings.TOP_N_MEMORY_QUERIES_ENABLED,
            QueryInsightsSettings.TOP_N_MEMORY_QUERIES_SIZE,
            QueryInsightsSettings.TOP_N_MEMORY_QUERIES_WINDOW_SIZE,
            QueryInsightsSettings.TOP_N_QUERIES_GROUP_BY,
            QueryInsightsSettings.TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N,
            QueryInsightsSettings.TOP_N_QUERIES_GROUPING_FIELD_NAME,
            QueryInsightsSettings.TOP_N_QUERIES_GROUPING_FIELD_TYPE,
            QueryCategorizationSettings.SEARCH_QUERY_METRICS_ENABLED_SETTING,
            QueryInsightsSettings.TOP_N_EXPORTER_DELETE_AFTER,
            QueryInsightsSettings.TOP_N_EXPORTER_TYPE,
            QueryInsightsSettings.TOP_N_QUERIES_EXCLUDED_INDICES,
            QueryInsightsSettings.TOP_N_QUERIES_MAX_SOURCE_LENGTH,
            QueryInsightsSettings.FINISHED_QUERIES_CACHE_ENABLED,
            QueryInsightsSettings.FINISHED_QUERIES_RETENTION_PERIOD,
            QueryCategorizationSettings.SEARCH_QUERY_FIELD_TYPE_CACHE_SIZE_KEY
        );
    }
}
