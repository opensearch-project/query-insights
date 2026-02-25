/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugin.insights.core.listener.QueryInsightsListener;
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
import org.opensearch.plugin.insights.rules.transport.live_queries.TransportLiveQueriesAction;
import org.opensearch.plugin.insights.settings.QueryCategorizationSettings;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestHandler;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;

public class QueryInsightsPluginTests extends OpenSearchTestCase {

    private QueryInsightsPlugin queryInsightsPlugin;

    private final Client client = mock(Client.class, org.mockito.Answers.RETURNS_DEEP_STUBS);
    private ClusterService clusterService;
    private final ThreadPool threadPool = new TestThreadPool(
        "QueryInsightsPluginTests",
        new ScalingExecutorBuilder(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR, 1, 5, TimeValue.timeValueMinutes(5))
    );
    private final MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);

    @Before
    public void setup() {
        queryInsightsPlugin = new QueryInsightsPlugin();
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        clusterService = ClusterServiceUtils.createClusterService(settings, clusterSettings, threadPool);

        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );

        // Mock the client administrative calls
        AdminClient adminClient = mock(AdminClient.class);
        ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testGetSettings() {
        assertEquals(
            Arrays.asList(
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
                QueryCategorizationSettings.SEARCH_QUERY_FIELD_TYPE_CACHE_SIZE_KEY,
                QueryInsightsSettings.REMOTE_EXPORTER_REPOSITORY,
                QueryInsightsSettings.REMOTE_EXPORTER_PATH,
                QueryInsightsSettings.REMOTE_EXPORTER_ENABLED,
                QueryInsightsSettings.LIVE_QUERIES_CACHE_IDLE_TIMEOUT
            ),
            queryInsightsPlugin.getSettings()
        );
    }

    public void testCreateComponent() {
        RepositoriesService repositoriesService = mock(RepositoriesService.class);
        List<Object> components = (List<Object>) queryInsightsPlugin.createComponents(
            client,
            clusterService,
            threadPool,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            () -> repositoriesService,
            null,
            metricsRegistry
        );
        assertEquals(2, components.size());
        assertTrue(components.get(0) instanceof QueryInsightsService);
        assertTrue(components.get(1) instanceof QueryInsightsListener);
    }

    public void testGetExecutorBuilders() {
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        List<ExecutorBuilder<?>> executorBuilders = queryInsightsPlugin.getExecutorBuilders(settings);
        assertEquals(1, executorBuilders.size());
        assertTrue(executorBuilders.get(0) instanceof ScalingExecutorBuilder);
    }

    public void testGetRestHandlers() {
        List<RestHandler> components = queryInsightsPlugin.getRestHandlers(Settings.EMPTY, null, null, null, null, null, null);
        assertEquals(5, components.size());
        assertTrue(components.get(0) instanceof RestTopQueriesAction);
        assertTrue(components.get(1) instanceof RestHealthStatsAction);
        assertTrue(components.get(2) instanceof RestLiveQueriesAction);
        assertTrue(components.get(3) instanceof RestGetQueryInsightsSettingsAction);
        assertTrue(components.get(4) instanceof RestUpdateQueryInsightsSettingsAction);
    }

    public void testGetActions() {
        List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> components = queryInsightsPlugin.getActions();
        assertEquals(5, components.size());
        assertTrue(components.get(0).getAction() instanceof TopQueriesAction);
        assertTrue(components.get(1).getAction() instanceof HealthStatsAction);
        assertTrue(components.get(2).getAction() instanceof LiveQueriesAction);
        assertTrue(components.get(3).getAction() instanceof GetQueryInsightsSettingsAction);
        assertTrue(components.get(4).getAction() instanceof UpdateQueryInsightsSettingsAction);
    }

    public void testLiveQueriesActionRegistration() {
        List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = queryInsightsPlugin.getActions();
        boolean hasLiveQueriesAction = actions.stream().anyMatch(handler -> handler.getAction().name().equals(LiveQueriesAction.NAME));
        assertTrue("Plugin should register LiveQueriesAction", hasLiveQueriesAction);

        boolean hasLiveQueriesTransport = actions.stream()
            .filter(handler -> handler.getAction().name().equals(LiveQueriesAction.NAME))
            .anyMatch(handler -> handler.getTransportAction().equals(TransportLiveQueriesAction.class));
        assertTrue("Plugin should register TransportLiveQueriesAction", hasLiveQueriesTransport);
    }

    public void testLiveQueriesRestActionRegistration() {
        List<RestHandler> restHandlers = queryInsightsPlugin.getRestHandlers(Settings.EMPTY, null, null, null, null, null, null);
        boolean hasLiveQueriesRestHandler = restHandlers.stream().anyMatch(handler -> handler instanceof RestLiveQueriesAction);
        assertTrue("Plugin should register RestLiveQueriesAction", hasLiveQueriesRestHandler);
    }

}
