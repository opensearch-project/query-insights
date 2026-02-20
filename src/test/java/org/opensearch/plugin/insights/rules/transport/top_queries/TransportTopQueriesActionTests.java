/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.top_queries;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext.UserPrincipalInfo;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.core.service.TopQueriesService;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueries;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesResponse;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.FilterByMode;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class TransportTopQueriesActionTests extends OpenSearchTestCase {

    private final ThreadPool threadPool = new TestThreadPool("QueryInsightsThreadPool");
    private ClusterService clusterService;
    private TransportService transportService;
    private QueryInsightsService queryInsightsService;
    private ActionFilters actionFilters;
    private ClusterSettings clusterSettings;
    private DiscoveryNode node1;
    private DiscoveryNode node2;

    private TransportTopQueriesAction actionToTest;
    private DummyParentAction dummyParentAction;

    // Mocks for doExecute tests
    private Task mockTask;
    @SuppressWarnings("rawtypes")
    private ActionListener mockFinalListener;
    private TopQueriesService mockTopQueriesService;

    class DummyParentAction extends TransportTopQueriesAction {
        public DummyParentAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            QueryInsightsService queryInsightsService,
            ActionFilters actionFilters
        ) {
            super(threadPool, clusterService, transportService, queryInsightsService, actionFilters);
        }

        @SuppressWarnings("unchecked")
        public TopQueriesResponse createNewResponse() {
            TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY, null, null, null, null);
            return newResponse(request, Collections.emptyList(), Collections.emptyList());
        }
    }

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        clusterService = mock(ClusterService.class);
        transportService = mock(TransportService.class);
        queryInsightsService = mock(QueryInsightsService.class);
        actionFilters = mock(ActionFilters.class);
        Settings settings = Settings.builder().put("node.name", "test_node").put("cluster.name", "test_cluster_name").build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), VersionUtils.randomVersion(random()));
        node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), VersionUtils.randomVersion(random()));

        when(clusterService.localNode()).thenReturn(node1);
        final ClusterName clusterNameInstance = new ClusterName("test_cluster_name");
        when(clusterService.getClusterName()).thenReturn(clusterNameInstance);

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(node1).add(node2).localNodeId(node1.getId()).build();
        ClusterState clusterState = ClusterState.builder(clusterNameInstance).nodes(discoveryNodes).build();
        when(clusterService.state()).thenReturn(clusterState);

        mockTask = mock(Task.class);
        mockFinalListener = mock(ActionListener.class);
        mockTopQueriesService = mock(TopQueriesService.class);

        when(queryInsightsService.getTopQueriesService(any(MetricType.class))).thenReturn(mockTopQueriesService);

        actionToTest = new TransportTopQueriesAction(threadPool, clusterService, transportService, queryInsightsService, actionFilters) {
            @Override
            protected void doExecute(Task task, TopQueriesRequest request, ActionListener<TopQueriesResponse> finalListener) {}
        };

        dummyParentAction = new DummyParentAction(threadPool, clusterService, transportService, queryInsightsService, actionFilters);

        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_CPU_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_MEMORY_QUERIES_ENABLED);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(clusterService);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testNewResponse() {
        TopQueriesResponse response = dummyParentAction.createNewResponse();
        assertNotNull(response);
        assertEquals(MetricType.LATENCY, response.getMetricType());
        assertEquals("test_cluster_name", response.getClusterName().value());
    }

    @SuppressWarnings("unchecked")
    public void testHandleInMemoryDataResponse_noHistoricalData() {
        TopQueriesRequest request = new TopQueriesRequest(MetricType.CPU, null, null, null, false);
        List<SearchQueryRecord> inMemoryRecords = Collections.singletonList(
            new SearchQueryRecord(
                1L,
                Map.of(MetricType.CPU, new Measurement(1.0D, AggregationType.SUM)),
                Map.of(Attribute.NODE_ID, node1.getId()),
                new SearchSourceBuilder(),
                new UserPrincipalContext(threadPool),
                "live_only"
            )
        );
        TopQueries inMemoryTq = new TopQueries(node1, inMemoryRecords);
        TopQueriesResponse inMemoryResponse = new TopQueriesResponse(
            clusterService.getClusterName(),
            Collections.singletonList(inMemoryTq),
            Collections.emptyList(),
            request.getMetricType()
        );
        ActionListener<TopQueriesResponse> finalListener = mock(ActionListener.class);

        actionToTest.handleInMemoryDataResponse(request, inMemoryResponse, finalListener);

        verify(finalListener).onResponse(inMemoryResponse);
    }

    @SuppressWarnings("unchecked")
    public void testHandleInMemoryDataResponse_withHistoricalData_invokesFetch() {
        TopQueriesRequest request = new TopQueriesRequest(MetricType.CPU, "from", "to", "id", false);
        List<TopQueries> inMemoryTopQueries = Collections.singletonList(new TopQueries(node1, Collections.emptyList()));
        List<FailedNodeException> failures = Collections.emptyList();
        ActionListener<TopQueriesResponse> finalListener = mock(ActionListener.class);
        TransportTopQueriesAction spyAction = spy(actionToTest);

        spyAction.handleInMemoryDataResponse(
            request,
            new TopQueriesResponse(clusterService.getClusterName(), inMemoryTopQueries, failures, request.getMetricType()),
            finalListener
        );

        verify(spyAction).fetchHistoricalData(eq(request), eq(inMemoryTopQueries), eq(failures), eq(finalListener));
    }

    @SuppressWarnings("unchecked")
    public void testOnHistoricalDataResponse_combinesDataCorrectly() {
        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY, "from", "to", "id", true);
        List<SearchQueryRecord> inMemoryRecords = Collections.singletonList(
            new SearchQueryRecord(
                1L,
                Map.of(MetricType.LATENCY, new Measurement(5.0D, AggregationType.AVERAGE)),
                Map.of(Attribute.NODE_ID, node1.getId()),
                new SearchSourceBuilder(),
                new UserPrincipalContext(threadPool),
                "live_entry"
            )
        );
        TopQueries inMemoryTq = new TopQueries(node1, inMemoryRecords);
        List<TopQueries> inMemoryTopQueries = Collections.singletonList(inMemoryTq);
        List<FailedNodeException> failures = Collections.singletonList(new FailedNodeException(node1.getId(), "fail", null));
        List<SearchQueryRecord> histRecords = Collections.singletonList(
            new SearchQueryRecord(
                2L,
                Map.of(MetricType.LATENCY, new Measurement(10.0D, AggregationType.AVERAGE)),
                Map.of(Attribute.NODE_ID, node1.getId()),
                null,
                null,
                "hist_entry"
            )
        );
        ActionListener<TopQueriesResponse> finalListener = mock(ActionListener.class);

        actionToTest.onHistoricalDataResponse(request, inMemoryTopQueries, failures, histRecords, finalListener);

        ArgumentCaptor<TopQueriesResponse> responseCaptor = ArgumentCaptor.forClass(TopQueriesResponse.class);
        verify(finalListener).onResponse(responseCaptor.capture());

        TopQueriesResponse response = responseCaptor.getValue();
        assertEquals(2, response.getNodes().size());
        assertEquals(inMemoryRecords, response.getNodes().get(0).getTopQueriesRecord());
        assertEquals(histRecords, response.getNodes().get(1).getTopQueriesRecord());
    }

    @SuppressWarnings("unchecked")
    public void testOnHistoricalDataFailure_usesInMemoryData() {
        TopQueriesRequest request = new TopQueriesRequest(MetricType.CPU, "from", "to", "id", false);
        List<SearchQueryRecord> inMemoryRecords = Collections.singletonList(
            new SearchQueryRecord(
                3L,
                Map.of(MetricType.CPU, new Measurement(2.0D, AggregationType.SUM)),
                Map.of(Attribute.NODE_ID, node1.getId()),
                new SearchSourceBuilder(),
                new UserPrincipalContext(threadPool),
                "live_fail"
            )
        );
        TopQueries inMemoryTq = new TopQueries(node1, inMemoryRecords);
        List<TopQueries> inMemoryTopQueries = Collections.singletonList(inMemoryTq);
        List<FailedNodeException> failures = Collections.singletonList(new FailedNodeException(node1.getId(), "fail", null));
        Exception error = new RuntimeException("error");
        ActionListener<TopQueriesResponse> finalListener = mock(ActionListener.class);

        actionToTest.onHistoricalDataFailure(request, inMemoryTopQueries, failures, error, finalListener);

        ArgumentCaptor<TopQueriesResponse> responseCaptor = ArgumentCaptor.forClass(TopQueriesResponse.class);
        verify(finalListener).onResponse(responseCaptor.capture());

        TopQueriesResponse response = responseCaptor.getValue();
        assertEquals(1, response.getNodes().size());
        assertEquals(inMemoryRecords, response.getNodes().get(0).getTopQueriesRecord());
        assertEquals(failures, response.failures());
    }

    @SuppressWarnings("unchecked")
    public void testOnHistoricalDataResponse_removesDuplicates() {
        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY, "from", "to", "id", true);

        // in-memory record that's unique
        SearchQueryRecord uniqueInMemoryRecord = new SearchQueryRecord(
            1L,
            Map.of(MetricType.LATENCY, new Measurement(3.0D, AggregationType.AVERAGE)),
            Map.of(Attribute.NODE_ID, node1.getId()),
            new SearchSourceBuilder(),
            new UserPrincipalContext(threadPool),
            "unique_in_memory"
        );

        // in-memory record with ID that will also appear in historical data
        SearchQueryRecord inMemoryDuplicateRecord = new SearchQueryRecord(
            2L,
            Map.of(MetricType.LATENCY, new Measurement(5.0D, AggregationType.AVERAGE)),
            Map.of(Attribute.NODE_ID, node1.getId()),
            new SearchSourceBuilder(),
            new UserPrincipalContext(threadPool),
            "duplicate_entry"
        );

        // historical record with same ID but different content (should be filtered out)
        SearchQueryRecord historicalDuplicateRecord = new SearchQueryRecord(
            3L, // Different timestamp
            Map.of(MetricType.LATENCY, new Measurement(8.0D, AggregationType.AVERAGE)), // Different measurement
            Map.of(Attribute.NODE_ID, node1.getId()),
            null,
            null,
            "duplicate_entry" // Same ID - this is what matters for deduplication
        );

        // historical record that's unique
        SearchQueryRecord uniqueHistoricalRecord = new SearchQueryRecord(
            4L,
            Map.of(MetricType.LATENCY, new Measurement(7.0D, AggregationType.AVERAGE)),
            Map.of(Attribute.NODE_ID, node1.getId()),
            null,
            null,
            "unique_historical"
        );

        List<SearchQueryRecord> inMemoryRecords = List.of(uniqueInMemoryRecord, inMemoryDuplicateRecord);
        TopQueries inMemoryTq = new TopQueries(node1, inMemoryRecords);
        List<TopQueries> inMemoryTopQueries = Collections.singletonList(inMemoryTq);

        List<SearchQueryRecord> histRecords = List.of(historicalDuplicateRecord, uniqueHistoricalRecord);

        List<FailedNodeException> failures = Collections.emptyList();
        ActionListener<TopQueriesResponse> finalListener = mock(ActionListener.class);

        actionToTest.onHistoricalDataResponse(request, inMemoryTopQueries, failures, histRecords, finalListener);

        ArgumentCaptor<TopQueriesResponse> responseCaptor = ArgumentCaptor.forClass(TopQueriesResponse.class);
        verify(finalListener).onResponse(responseCaptor.capture());

        TopQueriesResponse response = responseCaptor.getValue();
        assertEquals(2, response.getNodes().size());

        // First node should have the original in-memory records
        assertEquals(inMemoryRecords, response.getNodes().get(0).getTopQueriesRecord());

        // Second node should have only the unique historical record (duplicate should be removed)
        List<SearchQueryRecord> deduplicatedHistoricalRecords = response.getNodes().get(1).getTopQueriesRecord();
        assertEquals(1, deduplicatedHistoricalRecords.size());
        assertEquals(uniqueHistoricalRecord, deduplicatedHistoricalRecords.get(0));

        // Verify the duplicate record (by ID) is not in the historical results
        boolean containsDuplicateId = deduplicatedHistoricalRecords.stream().anyMatch(record -> record.getId().equals("duplicate_entry"));
        assertFalse(containsDuplicateId);
    }

    @SuppressWarnings("unchecked")
    public void testWrapWithRbacFilter_noneModePassesThrough() {
        ActionListener<TopQueriesResponse> finalListener = mock(ActionListener.class);

        ActionListener<TopQueriesResponse> wrapped = actionToTest.wrapWithRbacFilter(finalListener, FilterByMode.NONE, null);

        // When mode is NONE, the delegate should be returned directly
        assertSame(finalListener, wrapped);
    }

    @SuppressWarnings("unchecked")
    public void testWrapWithRbacFilter_usernameMode_filtersRecords() {
        ActionListener<TopQueriesResponse> finalListener = mock(ActionListener.class);
        UserPrincipalInfo userInfo = new UserPrincipalInfo("user1", List.of("br1"), List.of("role1"));

        ActionListener<TopQueriesResponse> wrapped = actionToTest.wrapWithRbacFilter(finalListener, FilterByMode.USERNAME, userInfo);

        // Create records — one for user1 and one for user2
        Map<Attribute, Object> attrs1 = new HashMap<>();
        attrs1.put(Attribute.USERNAME, "user1");
        attrs1.put(Attribute.NODE_ID, node1.getId());
        SearchQueryRecord record1 = new SearchQueryRecord(
            1L,
            Map.of(MetricType.LATENCY, new Measurement(1.0D, AggregationType.AVERAGE)),
            attrs1,
            "rec1"
        );

        Map<Attribute, Object> attrs2 = new HashMap<>();
        attrs2.put(Attribute.USERNAME, "user2");
        attrs2.put(Attribute.NODE_ID, node1.getId());
        SearchQueryRecord record2 = new SearchQueryRecord(
            2L,
            Map.of(MetricType.LATENCY, new Measurement(2.0D, AggregationType.AVERAGE)),
            attrs2,
            "rec2"
        );

        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(record1);
        records.add(record2);
        TopQueries topQueries = new TopQueries(node1, records);
        TopQueriesResponse response = new TopQueriesResponse(
            clusterService.getClusterName(),
            Collections.singletonList(topQueries),
            Collections.emptyList(),
            MetricType.LATENCY
        );

        wrapped.onResponse(response);

        ArgumentCaptor<TopQueriesResponse> responseCaptor = ArgumentCaptor.forClass(TopQueriesResponse.class);
        verify(finalListener).onResponse(responseCaptor.capture());

        TopQueriesResponse filteredResponse = responseCaptor.getValue();
        assertEquals(1, filteredResponse.getNodes().size());
        // Only record1 (user1) should remain
        assertEquals(1, filteredResponse.getNodes().get(0).getTopQueriesRecord().size());
        assertEquals("rec1", filteredResponse.getNodes().get(0).getTopQueriesRecord().get(0).getId());
    }

    @SuppressWarnings("unchecked")
    public void testWrapWithRbacFilter_adminBypass() {
        ActionListener<TopQueriesResponse> finalListener = mock(ActionListener.class);
        UserPrincipalInfo adminInfo = new UserPrincipalInfo("admin_user", List.of("admin_br"), List.of("all_access"));

        ActionListener<TopQueriesResponse> wrapped = actionToTest.wrapWithRbacFilter(finalListener, FilterByMode.USERNAME, adminInfo);

        Map<Attribute, Object> attrs1 = new HashMap<>();
        attrs1.put(Attribute.USERNAME, "user1");
        attrs1.put(Attribute.NODE_ID, node1.getId());
        SearchQueryRecord record1 = new SearchQueryRecord(
            1L,
            Map.of(MetricType.LATENCY, new Measurement(1.0D, AggregationType.AVERAGE)),
            attrs1,
            "rec1"
        );

        Map<Attribute, Object> attrs2 = new HashMap<>();
        attrs2.put(Attribute.USERNAME, "user2");
        attrs2.put(Attribute.NODE_ID, node1.getId());
        SearchQueryRecord record2 = new SearchQueryRecord(
            2L,
            Map.of(MetricType.LATENCY, new Measurement(2.0D, AggregationType.AVERAGE)),
            attrs2,
            "rec2"
        );

        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(record1);
        records.add(record2);
        TopQueries topQueries = new TopQueries(node1, records);
        TopQueriesResponse response = new TopQueriesResponse(
            clusterService.getClusterName(),
            Collections.singletonList(topQueries),
            Collections.emptyList(),
            MetricType.LATENCY
        );

        wrapped.onResponse(response);

        ArgumentCaptor<TopQueriesResponse> responseCaptor = ArgumentCaptor.forClass(TopQueriesResponse.class);
        verify(finalListener).onResponse(responseCaptor.capture());

        TopQueriesResponse filteredResponse = responseCaptor.getValue();
        // Admin sees all records
        assertEquals(2, filteredResponse.getNodes().get(0).getTopQueriesRecord().size());
    }

    @SuppressWarnings("unchecked")
    public void testWrapWithRbacFilter_backendRolesMode_filtersRecords() {
        ActionListener<TopQueriesResponse> finalListener = mock(ActionListener.class);
        UserPrincipalInfo userInfo = new UserPrincipalInfo("user1", List.of("team_a"), List.of("role1"));

        ActionListener<TopQueriesResponse> wrapped = actionToTest.wrapWithRbacFilter(finalListener, FilterByMode.BACKEND_ROLES, userInfo);

        Map<Attribute, Object> attrs1 = new HashMap<>();
        attrs1.put(Attribute.BACKEND_ROLES, new String[] { "team_a", "team_b" });
        attrs1.put(Attribute.NODE_ID, node1.getId());
        SearchQueryRecord record1 = new SearchQueryRecord(
            1L,
            Map.of(MetricType.LATENCY, new Measurement(1.0D, AggregationType.AVERAGE)),
            attrs1,
            "rec1"
        );

        Map<Attribute, Object> attrs2 = new HashMap<>();
        attrs2.put(Attribute.BACKEND_ROLES, new String[] { "team_c" });
        attrs2.put(Attribute.NODE_ID, node1.getId());
        SearchQueryRecord record2 = new SearchQueryRecord(
            2L,
            Map.of(MetricType.LATENCY, new Measurement(2.0D, AggregationType.AVERAGE)),
            attrs2,
            "rec2"
        );

        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(record1);
        records.add(record2);
        TopQueries topQueries = new TopQueries(node1, records);
        TopQueriesResponse response = new TopQueriesResponse(
            clusterService.getClusterName(),
            Collections.singletonList(topQueries),
            Collections.emptyList(),
            MetricType.LATENCY
        );

        wrapped.onResponse(response);

        ArgumentCaptor<TopQueriesResponse> responseCaptor = ArgumentCaptor.forClass(TopQueriesResponse.class);
        verify(finalListener).onResponse(responseCaptor.capture());

        TopQueriesResponse filteredResponse = responseCaptor.getValue();
        assertEquals(1, filteredResponse.getNodes().size());
        assertEquals(1, filteredResponse.getNodes().get(0).getTopQueriesRecord().size());
        assertEquals("rec1", filteredResponse.getNodes().get(0).getTopQueriesRecord().get(0).getId());
    }

    @SuppressWarnings("unchecked")
    public void testWrapWithRbacFilter_onFailure_propagates() {
        ActionListener<TopQueriesResponse> finalListener = mock(ActionListener.class);
        UserPrincipalInfo userInfo = new UserPrincipalInfo("user1", List.of("br1"), List.of("role1"));

        ActionListener<TopQueriesResponse> wrapped = actionToTest.wrapWithRbacFilter(finalListener, FilterByMode.USERNAME, userInfo);

        RuntimeException error = new RuntimeException("test error");
        wrapped.onFailure(error);

        verify(finalListener).onFailure(error);
    }

    @SuppressWarnings("unchecked")
    public void testWrapWithRbacFilter_nullUserInfo_returnsEmpty() {
        ActionListener<TopQueriesResponse> finalListener = mock(ActionListener.class);

        ActionListener<TopQueriesResponse> wrapped = actionToTest.wrapWithRbacFilter(finalListener, FilterByMode.USERNAME, null);

        Map<Attribute, Object> attrs1 = new HashMap<>();
        attrs1.put(Attribute.USERNAME, "user1");
        attrs1.put(Attribute.NODE_ID, node1.getId());
        SearchQueryRecord record1 = new SearchQueryRecord(
            1L,
            Map.of(MetricType.LATENCY, new Measurement(1.0D, AggregationType.AVERAGE)),
            attrs1,
            "rec1"
        );

        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(record1);
        TopQueries topQueries = new TopQueries(node1, records);
        TopQueriesResponse response = new TopQueriesResponse(
            clusterService.getClusterName(),
            Collections.singletonList(topQueries),
            Collections.emptyList(),
            MetricType.LATENCY
        );

        wrapped.onResponse(response);

        ArgumentCaptor<TopQueriesResponse> responseCaptor = ArgumentCaptor.forClass(TopQueriesResponse.class);
        verify(finalListener).onResponse(responseCaptor.capture());

        TopQueriesResponse filteredResponse = responseCaptor.getValue();
        assertEquals(1, filteredResponse.getNodes().size());
        assertEquals(0, filteredResponse.getNodes().get(0).getTopQueriesRecord().size());
    }
}
