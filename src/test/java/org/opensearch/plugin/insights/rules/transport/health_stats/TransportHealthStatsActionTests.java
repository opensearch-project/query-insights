/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.health_stats;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.List;
import org.junit.Before;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.health_stats.HealthStatsRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesResponse;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.transport.top_queries.TransportTopQueriesAction;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class TransportHealthStatsActionTests extends OpenSearchTestCase {

    private final ThreadPool threadPool = mock(ThreadPool.class);

    private final Settings.Builder settingsBuilder = Settings.builder();
    private final Settings settings = settingsBuilder.build();
    private final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final ClusterService clusterService = ClusterServiceUtils.createClusterService(settings, clusterSettings, threadPool);
    private final TransportService transportService = mock(TransportService.class);
    private final QueryInsightsService queryInsightsService = mock(QueryInsightsService.class);
    private final ActionFilters actionFilters = mock(ActionFilters.class);
    private final TransportHealthStatsAction transportHealthStatsAction = new TransportHealthStatsAction(
        threadPool,
        clusterService,
        transportService,
        queryInsightsService,
        actionFilters
    );
    private final DummyParentAction dummyParentAction = new DummyParentAction(
        threadPool,
        clusterService,
        transportService,
        queryInsightsService,
        actionFilters
    );

    class DummyParentAction extends TransportTopQueriesAction {
        public DummyParentAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            QueryInsightsService topQueriesByLatencyService,
            ActionFilters actionFilters
        ) {
            super(threadPool, clusterService, transportService, topQueriesByLatencyService, actionFilters);
        }

        public TopQueriesResponse createNewResponse() {
            TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY, null, null);
            return newResponse(request, List.of(), List.of());
        }
    }

    @Before
    public void setup() {
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);
    }

    public void testNewResponse() {
        TopQueriesResponse response = dummyParentAction.createNewResponse();
        assertNotNull(response);
    }

    public void testNewNodeRequest() {
        HealthStatsRequest request = new HealthStatsRequest();
        TransportHealthStatsAction.NodeRequest nodeRequest = transportHealthStatsAction.newNodeRequest(request);
        assertEquals(request, nodeRequest.request);
    }

    public void testNodeRequestConstructor() throws IOException {
        HealthStatsRequest request = new HealthStatsRequest();
        TransportHealthStatsAction.NodeRequest nodeRequest = new TransportHealthStatsAction.NodeRequest(request);
        BytesStreamOutput out = new BytesStreamOutput();
        nodeRequest.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        TransportHealthStatsAction.NodeRequest deserializedRequest = new TransportHealthStatsAction.NodeRequest(in);
        assertEquals(request.nodesIds().length, deserializedRequest.request.nodesIds().length);
    }

}
