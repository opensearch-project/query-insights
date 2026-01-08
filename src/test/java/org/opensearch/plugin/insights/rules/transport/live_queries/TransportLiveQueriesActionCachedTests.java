/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.tasks.resourcetracker.TaskResourceStats;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.core.tasks.resourcetracker.TaskThreadUsage;
import org.opensearch.plugin.insights.core.service.FinishedQueriesCache;
import org.opensearch.plugin.insights.core.service.LiveQueriesCache;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.test.tasks.MockTaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;

/**
 * Unit tests for cached and include_finished functionality in {@link TransportLiveQueriesAction}.
 */
@SuppressWarnings("unchecked")
public class TransportLiveQueriesActionCachedTests extends OpenSearchTestCase {

    private TransportLiveQueriesAction transportLiveQueriesAction;
    private Client client;
    private ClusterService clusterService;
    private DiscoveryNode node1;
    private ThreadPool threadPool;
    private TransportService transportService;
    private ActionFilters actionFilters;
    private AdminClient adminClient;
    private ClusterAdminClient clusterAdminClient;
    private QueryInsightsService queryInsightsService;
    private FinishedQueriesCache finishedQueriesCache;
    private LiveQueriesCache liveQueriesCache;

    @Before
    public void setup() {
        threadPool = mock(ThreadPool.class);
        clusterService = mock(ClusterService.class);
        transportService = mock(TransportService.class);
        MockTaskManager taskManager = new MockTaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        when(transportService.getTaskManager()).thenReturn(taskManager);
        client = mock(Client.class, org.mockito.Answers.RETURNS_DEEP_STUBS);
        actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new org.opensearch.action.support.ActionFilter[0]);

        node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), VersionUtils.randomVersion(random()));

        when(clusterService.localNode()).thenReturn(node1);
        when(clusterService.getClusterName()).thenReturn(new ClusterName("test-cluster"));
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(node1).localNodeId(node1.getId()).build();
        ClusterState clusterState = ClusterState.builder(clusterService.getClusterName()).nodes(discoveryNodes).build();
        when(clusterService.state()).thenReturn(clusterState);

        when(transportService.getLocalNode()).thenReturn(node1);

        adminClient = mock(AdminClient.class);
        clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        queryInsightsService = mock(QueryInsightsService.class);
        finishedQueriesCache = mock(FinishedQueriesCache.class);
        liveQueriesCache = mock(LiveQueriesCache.class);
        when(queryInsightsService.getFinishedQueriesCache()).thenReturn(finishedQueriesCache);
        when(queryInsightsService.getLiveQueriesCache()).thenReturn(liveQueriesCache);

        transportLiveQueriesAction = new TransportLiveQueriesAction(transportService, client, actionFilters, queryInsightsService);
    }

    private TaskInfo createTaskInfo(
        DiscoveryNode node,
        String action,
        long startTime,
        long runningTimeNanos,
        String description,
        long cpuNanos,
        long memoryBytes
    ) throws IOException {
        TaskId taskId = new TaskId(node.getId(), randomLong());
        Map<String, TaskResourceUsage> resourceUsageInfoMap = new HashMap<>();
        TaskResourceUsage totalUsage = new TaskResourceUsage(cpuNanos, memoryBytes);
        resourceUsageInfoMap.put("total", totalUsage);
        TaskResourceStats resourceStats = new TaskResourceStats(resourceUsageInfoMap, new TaskThreadUsage(0, 0));

        return new TaskInfo(
            taskId,
            "test_type",
            action,
            description,
            null,
            startTime,
            runningTimeNanos,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.singletonMap("foo", "bar"),
            resourceStats
        );
    }

    private SearchQueryRecord createFinishedQuery(String id, long latency) {
        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(latency));
        measurements.put(MetricType.CPU, new Measurement(randomLongBetween(100, 1000)));
        measurements.put(MetricType.MEMORY, new Measurement(randomLongBetween(1024, 10240)));

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.SEARCH_TYPE, "query_then_fetch");
        attributes.put(Attribute.TOTAL_SHARDS, 1);

        return new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, id);
    }

    private SearchQueryRecord createFinishedLiveQuery(String id, long latency) {
        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(latency));
        measurements.put(MetricType.CPU, new Measurement(randomLongBetween(100, 1000)));
        measurements.put(MetricType.MEMORY, new Measurement(randomLongBetween(1024, 10240)));
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.TASK_ID, "node-1:123");
        attributes.put(Attribute.NODE_ID, "node-1");
        attributes.put(Attribute.DESCRIPTION, "test query");
        attributes.put(Attribute.IS_CANCELLED, false);
        return new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, id);
    }

    public void testRequestWithCachedTrue() throws IOException {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.LATENCY, 10, new String[0], null, null, true, false);

        // Mock live cache to return empty list
        when(liveQueriesCache.getCurrentQueries()).thenReturn(Collections.emptyList());

        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        LiveQueriesResponse response = futureResponse.actionGet();

        assertEquals(0, response.getLiveQueries().size());
        assertTrue(request.isCached());
        assertFalse(request.isIncludeFinished());
    }

    public void testRequestWithIncludeFinishedTrue() throws IOException {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.LATENCY, 10, new String[0], null, null, false, true);
        TaskInfo task1 = createTaskInfo(node1, "indices:data/read/search", System.currentTimeMillis(), 1000000L, "desc1", 500L, 1024L);
        List<TaskInfo> tasks = List.of(task1);
        ListTasksResponse listTasksResponse = new ListTasksResponse(tasks, emptyList(), emptyList());

        // Mock finished queries cache
        List<SearchQueryRecord> finishedQueries = List.of(
            createFinishedLiveQuery("finished1", 100L),
            createFinishedLiveQuery("finished2", 200L)
        );
        when(finishedQueriesCache.getFinishedQueries(false)).thenReturn(finishedQueries);
        when(liveQueriesCache.getCurrentQueries()).thenReturn(Collections.emptyList());

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        LiveQueriesResponse response = futureResponse.actionGet();

        assertEquals(1, response.getLiveQueries().size());
        assertFalse(request.isCached());
        assertTrue(request.isIncludeFinished());
    }

    public void testRequestWithBothCachedAndIncludeFinished() throws IOException {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.LATENCY, 10, new String[0], null, null, true, true);

        // Mock finished queries cache
        List<SearchQueryRecord> finishedQueries = List.of(createFinishedLiveQuery("finished1", 100L));
        when(finishedQueriesCache.getFinishedQueries(true)).thenReturn(finishedQueries);
        when(liveQueriesCache.getCurrentQueries()).thenReturn(Collections.emptyList());

        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        LiveQueriesResponse response = futureResponse.actionGet();

        assertEquals(0, response.getLiveQueries().size());
        assertTrue(request.isCached());
        assertTrue(request.isIncludeFinished());
    }

    public void testRequestWithWlmGroupId() throws IOException {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.LATENCY, 10, new String[0], "TEST_GROUP", null, false, false);
        TaskInfo task1 = createTaskInfo(node1, "indices:data/read/search", System.currentTimeMillis(), 1000000L, "desc1", 500L, 1024L);
        List<TaskInfo> tasks = List.of(task1);
        ListTasksResponse listTasksResponse = new ListTasksResponse(tasks, emptyList(), emptyList());

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        LiveQueriesResponse response = futureResponse.actionGet();

        // The task will be filtered out because it doesn't have the matching WLM group
        assertEquals(0, response.getLiveQueries().size());
        assertEquals("TEST_GROUP", request.getWlmGroupId());
    }

    public void testRequestWithAllNewParameters() throws IOException {
        LiveQueriesRequest request = new LiveQueriesRequest(
            false,
            MetricType.CPU,
            5,
            new String[] { "node1" },
            "CUSTOM_GROUP",
            null,
            true,
            true
        );
        TaskInfo task1 = createTaskInfo(node1, "indices:data/read/search", System.currentTimeMillis(), 1000000L, "desc1", 500L, 1024L);
        List<TaskInfo> tasks = List.of(task1);
        ListTasksResponse listTasksResponse = new ListTasksResponse(tasks, emptyList(), emptyList());

        // Mock finished queries cache
        List<SearchQueryRecord> finishedQueries = List.of(createFinishedLiveQuery("finished1", 100L));
        when(finishedQueriesCache.getFinishedQueries(false)).thenReturn(finishedQueries);
        when(liveQueriesCache.getCurrentQueries()).thenReturn(Collections.emptyList());

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        LiveQueriesResponse response = futureResponse.actionGet();

        // The task will be filtered out because it doesn't have the matching WLM group
        assertEquals(0, response.getLiveQueries().size());
        assertFalse(request.isVerbose());
        assertEquals(MetricType.CPU, request.getSortBy());
        assertEquals(5, request.getSize());
        assertArrayEquals(new String[] { "node1" }, request.nodesIds());
        assertEquals("CUSTOM_GROUP", request.getWlmGroupId());
        assertTrue(request.isCached());
        assertTrue(request.isIncludeFinished());
    }

    public void testDefaultParameterValues() throws IOException {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        TaskInfo task1 = createTaskInfo(node1, "indices:data/read/search", System.currentTimeMillis(), 1000000L, "desc1", 500L, 1024L);
        List<TaskInfo> tasks = List.of(task1);
        ListTasksResponse listTasksResponse = new ListTasksResponse(tasks, emptyList(), emptyList());

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        LiveQueriesResponse response = futureResponse.actionGet();

        assertEquals(1, response.getLiveQueries().size());
        assertFalse(request.isCached()); // Default should be false
        assertFalse(request.isIncludeFinished()); // Default should be false
        assertNull(request.getWlmGroupId()); // Default should be null
    }
}
