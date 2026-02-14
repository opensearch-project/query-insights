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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.TaskGroup;
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
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.LiveQueryRecord;
import org.opensearch.plugin.insights.rules.model.MetricType;
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
 * Unit tests for the {@link TransportLiveQueriesAction} class.
 */
@SuppressWarnings("unchecked")
public class TransportLiveQueriesActionTests extends OpenSearchTestCase {

    private TransportLiveQueriesAction transportLiveQueriesAction;
    private Client client;
    private ClusterService clusterService;
    private DiscoveryNode node1;
    private DiscoveryNode node2;
    private ThreadPool threadPool;
    private TransportService transportService;
    private ActionFilters actionFilters;
    private AdminClient adminClient;
    private ClusterAdminClient clusterAdminClient;

    @Before
    public void setup() {
        threadPool = mock(ThreadPool.class);
        clusterService = mock(ClusterService.class);
        transportService = mock(TransportService.class);
        MockTaskManager taskManager = new MockTaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        when(transportService.getTaskManager()).thenReturn(taskManager);
        client = mock(Client.class, org.mockito.Answers.RETURNS_DEEP_STUBS); // Use deep stubs for client.admin().cluster()
        actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new org.opensearch.action.support.ActionFilter[0]);

        node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), VersionUtils.randomVersion(random()));
        node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), VersionUtils.randomVersion(random()));

        when(clusterService.localNode()).thenReturn(node1); // Assume node1 is local for simplicity
        when(clusterService.getClusterName()).thenReturn(new ClusterName("test-cluster"));
        // Stub cluster state to include the nodes
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(node1).add(node2).localNodeId(node1.getId()).build();
        ClusterState clusterState = ClusterState.builder(clusterService.getClusterName()).nodes(discoveryNodes).build();
        when(clusterService.state()).thenReturn(clusterState);

        // Mock the transport service to return the local node
        when(transportService.getLocalNode()).thenReturn(node1);

        // Mock the client administrative calls

        adminClient = mock(AdminClient.class);
        clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        transportLiveQueriesAction = new TransportLiveQueriesAction(transportService, client, actionFilters);
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

    public void testNodeOperationVerbose_AllNodes() throws IOException {
        // Request all nodes (no specific nodeIds)
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        TaskInfo task1 = createTaskInfo(node1, "indices:data/read/search", System.currentTimeMillis(), 1000000L, "desc1", 500L, 1024L);
        TaskInfo task2 = createTaskInfo(node2, "indices:data/read/search", System.currentTimeMillis(), 2000000L, "desc2", 600L, 2048L);
        TaskInfo nonSearchTask1 = createTaskInfo(
            node1,
            "cluster:monitor/nodes/tasks/list",
            System.currentTimeMillis(),
            3000000L,
            "desc3",
            700L,
            4096L
        );
        TaskInfo nonSearchTask2 = createTaskInfo(
            node1,
            "indices:data/read/search[phase/query]",
            System.currentTimeMillis(),
            3000000L,
            "desc3",
            700L,
            4096L
        );
        List<TaskInfo> tasks = List.of(task1, task2, nonSearchTask1, nonSearchTask2);
        ListTasksResponse listTasksResponse = new ListTasksResponse(tasks, emptyList(), emptyList());

        // Mock the listTasks asynchronous call
        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        // Execute
        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        LiveQueriesResponse clusterResponse = futureResponse.actionGet();
        List<LiveQueryRecord> records = clusterResponse.getLiveQueries();

        // Verify listTasks was called and DID NOT have node filter set
        ArgumentCaptor<ListTasksRequest> captor = ArgumentCaptor.forClass(ListTasksRequest.class);
        verify(clusterAdminClient).listTasks(captor.capture(), any(ActionListener.class));
        assertEquals(0, captor.getValue().getNodes().length);

        // Verify results - should have 2 search tasks
        assertEquals(2, records.size());
        assertTrue(records.stream().anyMatch(r -> r.getQueryId().equals(task1.getTaskId().toString())));
        assertTrue(records.stream().anyMatch(r -> r.getQueryId().equals(task2.getTaskId().toString())));
    }

    public void testNodeFiltering_SpecificNode() throws IOException {
        // Request only node2
        LiveQueriesRequest request = new LiveQueriesRequest(true, "node2");
        // Prepare response as if listTasks only returned tasks for node2
        TaskInfo task2 = createTaskInfo(node2, "indices:data/read/search", System.currentTimeMillis(), 2000000L, "desc2", 600L, 2048L);
        List<TaskInfo> tasks = List.of(task2);
        ListTasksResponse listTasksResponse = new ListTasksResponse(tasks, emptyList(), emptyList());

        // Mock the listTasks asynchronous call
        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        // Execute
        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        LiveQueriesResponse clusterResponse = futureResponse.actionGet();
        List<LiveQueryRecord> records = clusterResponse.getLiveQueries();

        // Verify listTasks was called and HAD node filter set
        ArgumentCaptor<ListTasksRequest> captor = ArgumentCaptor.forClass(ListTasksRequest.class);
        verify(clusterAdminClient).listTasks(captor.capture(), any(ActionListener.class));
        assertArrayEquals(new String[] { "node2" }, captor.getValue().getNodes());

        // Verify results - should only have task2 from node2
        assertEquals(1, records.size());
        LiveQueryRecord record2 = records.get(0);
        assertEquals(task2.getTaskId().toString(), record2.getQueryId());
    }

    public void testNodeOperationNonVerbose() throws IOException {
        // Request non-verbose for node1
        LiveQueriesRequest request = new LiveQueriesRequest(false, "node1");
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
        LiveQueriesResponse clusterResponse = futureResponse.actionGet();
        List<LiveQueryRecord> records = clusterResponse.getLiveQueries();

        // Verify listTasks was called and HAD node filter set
        ArgumentCaptor<ListTasksRequest> captor = ArgumentCaptor.forClass(ListTasksRequest.class);
        verify(clusterAdminClient).listTasks(captor.capture(), any(ActionListener.class));
        assertArrayEquals(new String[] { "node1" }, captor.getValue().getNodes());

        assertEquals(1, records.size());
    }

    public void testNodeOperationHandlesException() {
        LiveQueriesRequest request = new LiveQueriesRequest(true);

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("Simulated task fetch error"));
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        expectThrows(RuntimeException.class, futureResponse::actionGet);
    }

    public void testResponseConstructor() {
        List<LiveQueryRecord> records = Collections.emptyList();
        LiveQueriesResponse response = new LiveQueriesResponse(records);
        assertNotNull(response);
        assertEquals(records, response.getLiveQueries());
    }

    public void testTransportActionSortsByCpuAndLimitsSize() throws IOException {
        // Prepare a request to sort by CPU and limit to 1 result
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.CPU, 1, new String[0], null);
        // Create tasks with different CPU values
        TaskInfo lowCpu = createTaskInfo(node1, "indices:data/read/search", System.currentTimeMillis(), 1000L, "low", 100L, 100L);
        TaskInfo highCpu = createTaskInfo(node1, "indices:data/read/search", System.currentTimeMillis(), 2000L, "high", 200L, 200L);
        TaskInfo midCpu = createTaskInfo(node1, "indices:data/read/search", System.currentTimeMillis(), 1500L, "mid", 150L, 150L);
        List<TaskInfo> tasks = List.of(lowCpu, highCpu, midCpu);
        ListTasksResponse listTasksResponse = new ListTasksResponse(tasks, emptyList(), emptyList());
        // Mock the async call
        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        // Execute transport action
        PlainActionFuture<LiveQueriesResponse> future = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, future);
        LiveQueriesResponse response = future.actionGet();
        List<LiveQueryRecord> records = response.getLiveQueries();

        // Should only have the highCpu task

        assertEquals(1, records.size());
        assertEquals(highCpu.getTaskId().toString(), records.get(0).getQueryId());
    }

    // Safeguard tests: missing or incomplete resource stats should default CPU/memory to 0
    public void testNullResourceStatsDefaultsToZero() throws Exception {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        // Create a TaskInfo with null resourceStats
        TaskInfo info = new TaskInfo(
            new TaskId(node1.getId(), randomLong()),
            "test_type",
            "indices:data/read/search",
            "desc",
            null,
            123L,
            456L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            null // resourceStats is null
        );
        ListTasksResponse listTasksResponse = new ListTasksResponse(List.of(info), emptyList(), emptyList());
        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> future = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, future);
        LiveQueriesResponse response = future.actionGet();
        LiveQueryRecord rec = response.getLiveQueries().get(0);
        assertEquals(0L, rec.getTotalCpu());
        assertEquals(0L, rec.getTotalMemory());
    }

    public void testEmptyUsageInfoDefaultsToZero() throws Exception {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        // Create TaskResourceStats with empty usageInfo map
        TaskResourceStats stats = new TaskResourceStats(Collections.emptyMap(), new TaskThreadUsage(0, 0));
        TaskInfo info = new TaskInfo(
            new TaskId(node1.getId(), randomLong()),
            "test_type",
            "indices:data/read/search",
            "desc",
            null,
            123L,
            456L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            stats
        );
        ListTasksResponse listTasksResponse = new ListTasksResponse(List.of(info), emptyList(), emptyList());
        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> future = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, future);
        LiveQueriesResponse response = future.actionGet();
        LiveQueryRecord rec = response.getLiveQueries().get(0);
        assertEquals(0L, rec.getTotalCpu());
        assertEquals(0L, rec.getTotalMemory());
    }

    public void testMissingTotalUsageDefaultsToZero() throws Exception {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        // Create usageInfo map without 'total' key
        Map<String, TaskResourceUsage> usageInfo = Map.of("other", new TaskResourceUsage(111L, 222L));
        TaskResourceStats stats = new TaskResourceStats(usageInfo, new TaskThreadUsage(0, 0));
        TaskInfo info = new TaskInfo(
            new TaskId(node1.getId(), randomLong()),
            "test_type",
            "indices:data/read/search",
            "desc",
            null,
            123L,
            456L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            stats
        );
        ListTasksResponse listTasksResponse = new ListTasksResponse(List.of(info), emptyList(), emptyList());
        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> future = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, future);
        LiveQueriesResponse response = future.actionGet();
        LiveQueryRecord rec = response.getLiveQueries().get(0);
        assertEquals(0L, rec.getTotalCpu());
        assertEquals(0L, rec.getTotalMemory());
    }

    public void testTaskGroupAggregation() throws Exception {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        TaskInfo coord = createTaskInfo(node1, "indices:data/read/search", 100L, 200L, "coord", 100L, 200L);
        TaskInfo shard1 = createTaskInfo(node1, "indices:data/read/search[phase/query]", 100L, 200L, "shard1", 300L, 400L);
        TaskInfo shard2 = createTaskInfo(node2, "indices:data/read/search[phase/query]", 100L, 200L, "shard2", 500L, 600L);

        TaskGroup group = new TaskGroup(coord, List.of(new TaskGroup(shard1, emptyList()), new TaskGroup(shard2, emptyList())));
        ListTasksResponse mockResponse = mock(ListTasksResponse.class);
        when(mockResponse.getTaskGroups()).thenReturn(List.of(group));

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> future = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, future);
        LiveQueriesResponse response = future.actionGet();
        LiveQueryRecord rec = response.getLiveQueries().get(0);

        assertEquals(900L, rec.getTotalCpu());
        assertEquals(1200L, rec.getTotalMemory());
        assertEquals(2, rec.getShardTasks().size());
    }

    public void testMultipleQueriesMultipleNodes() throws Exception {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        TaskInfo coord1 = createTaskInfo(node1, "indices:data/read/search", 100L, 200L, "q1", 100L, 200L);
        TaskInfo shard1 = createTaskInfo(node1, "indices:data/read/search[phase/query]", 100L, 200L, "s1", 300L, 400L);
        TaskInfo coord2 = createTaskInfo(node2, "indices:data/read/search", 100L, 200L, "q2", 500L, 600L);
        TaskInfo shard2 = createTaskInfo(node2, "indices:data/read/search[phase/query]", 100L, 200L, "s2", 700L, 800L);

        TaskGroup group1 = new TaskGroup(coord1, List.of(new TaskGroup(shard1, emptyList())));
        TaskGroup group2 = new TaskGroup(coord2, List.of(new TaskGroup(shard2, emptyList())));
        ListTasksResponse mockResponse = mock(ListTasksResponse.class);
        when(mockResponse.getTaskGroups()).thenReturn(List.of(group1, group2));

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> future = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, future);
        LiveQueriesResponse response = future.actionGet();

        assertEquals(2, response.getLiveQueries().size());
        assertEquals(400L, response.getLiveQueries().get(0).getTotalCpu());
        assertEquals(1200L, response.getLiveQueries().get(1).getTotalCpu());
    }

    public void testEmptyTaskGroups() throws Exception {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        ListTasksResponse mockResponse = mock(ListTasksResponse.class);
        when(mockResponse.getTaskGroups()).thenReturn(emptyList());

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> future = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, future);
        LiveQueriesResponse response = future.actionGet();

        assertEquals(0, response.getLiveQueries().size());
    }

    public void testCoordinatorWithNoShards() throws Exception {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        TaskInfo coord = createTaskInfo(node1, "indices:data/read/search", 100L, 200L, "coord", 100L, 200L);
        TaskGroup group = new TaskGroup(coord, emptyList());
        ListTasksResponse mockResponse = mock(ListTasksResponse.class);
        when(mockResponse.getTaskGroups()).thenReturn(List.of(group));

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> future = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, future);
        LiveQueriesResponse response = future.actionGet();
        LiveQueryRecord rec = response.getLiveQueries().get(0);

        assertEquals(100L, rec.getTotalCpu());
        assertEquals(200L, rec.getTotalMemory());
        assertEquals(0, rec.getShardTasks().size());
    }

    public void testNegativeSize() throws Exception {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.LATENCY, -1, new String[0], null);
        TaskInfo task = createTaskInfo(node1, "indices:data/read/search", 100L, 200L, "task", 100L, 200L);
        ListTasksResponse listTasksResponse = new ListTasksResponse(List.of(task), emptyList(), emptyList());

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> future = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, future);
        LiveQueriesResponse response = future.actionGet();

        assertEquals(1, response.getLiveQueries().size());
    }

    public void testShardsMappedToCorrectParent() throws Exception {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        TaskInfo coord1 = createTaskInfo(
            node1,
            "indices:data/read/search",
            1000L,
            2000L,
            "indices[index1], search_type[QUERY_THEN_FETCH]",
            100L,
            200L
        );
        TaskInfo shard1a = createTaskInfo(node1, "indices:data/read/search[phase/query]", 1000L, 2000L, "shardId[[index1][0]]", 300L, 400L);
        TaskInfo shard1b = createTaskInfo(node2, "indices:data/read/search[phase/query]", 1000L, 2000L, "shardId[[index1][1]]", 500L, 600L);
        TaskInfo coord2 = createTaskInfo(
            node2,
            "indices:data/read/search",
            1000L,
            2000L,
            "indices[index2], search_type[QUERY_THEN_FETCH]",
            700L,
            800L
        );
        TaskInfo shard2a = createTaskInfo(
            node1,
            "indices:data/read/search[phase/query]",
            1000L,
            2000L,
            "shardId[[index2][0]]",
            900L,
            1000L
        );
        TaskInfo nonSearch = createTaskInfo(node1, "cluster:monitor/nodes/stats", 1000L, 2000L, "monitoring", 50L, 100L);

        TaskGroup group1 = new TaskGroup(coord1, List.of(new TaskGroup(shard1a, emptyList()), new TaskGroup(shard1b, emptyList())));
        TaskGroup group2 = new TaskGroup(coord2, List.of(new TaskGroup(shard2a, emptyList())));
        TaskGroup nonSearchGroup = new TaskGroup(nonSearch, emptyList());
        ListTasksResponse mockResponse = mock(ListTasksResponse.class);
        when(mockResponse.getTaskGroups()).thenReturn(List.of(group1, group2, nonSearchGroup));

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> future = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, future);
        LiveQueriesResponse response = future.actionGet();

        assertEquals(2, response.getLiveQueries().size());
        LiveQueryRecord query1 = response.getLiveQueries().get(0);
        LiveQueryRecord query2 = response.getLiveQueries().get(1);

        assertEquals(coord1.getTaskId().toString(), query1.getQueryId());
        assertEquals(2, query1.getShardTasks().size());
        assertEquals(900L, query1.getTotalCpu());

        assertEquals(coord2.getTaskId().toString(), query2.getQueryId());
        assertEquals(1, query2.getShardTasks().size());
        assertEquals(1600L, query2.getTotalCpu());
    }

    public void testCoordinatorAndShardResourceAggregation() throws Exception {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        TaskInfo coord = createTaskInfo(
            node1,
            "indices:data/read/search",
            1770841884644L,
            229069667L,
            "indices[test-index], search_type[QUERY_THEN_FETCH]",
            11084000L,
            1719784L
        );
        TaskInfo shard1 = createTaskInfo(
            node2,
            "indices:data/read/search[phase/fetch/id]",
            1770841884865L,
            8365583L,
            "id[[uvkUqd8wRGqJtO0PCg2FYw][5]], size[354]",
            6418000L,
            856072L
        );
        TaskInfo shard2 = createTaskInfo(
            node2,
            "indices:data/read/search[phase/fetch/id]",
            1770841884865L,
            8494708L,
            "id[[uvkUqd8wRGqJtO0PCg2FYw][3]], size[330]",
            4564000L,
            316328L
        );
        TaskInfo shard3 = createTaskInfo(
            node2,
            "indices:data/read/search[phase/fetch/id]",
            1770841884865L,
            8421125L,
            "id[[uvkUqd8wRGqJtO0PCg2FYw][4]], size[316]",
            4564000L,
            351600L
        );

        TaskGroup group = new TaskGroup(
            coord,
            List.of(new TaskGroup(shard1, emptyList()), new TaskGroup(shard2, emptyList()), new TaskGroup(shard3, emptyList()))
        );
        ListTasksResponse mockResponse = mock(ListTasksResponse.class);
        when(mockResponse.getTaskGroups()).thenReturn(List.of(group));

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> future = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, future);
        LiveQueriesResponse response = future.actionGet();
        LiveQueryRecord rec = response.getLiveQueries().get(0);

        assertEquals(26630000L, rec.getTotalCpu());
        assertEquals(3243784L, rec.getTotalMemory());
        assertEquals(3, rec.getShardTasks().size());
        assertEquals(
            11084000L,
            rec.getCoordinatorTask().getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(coord.getTaskId().toString(), rec.getQueryId());
        assertEquals(coord.getTaskId().toString(), rec.getCoordinatorTask().getTaskInfo().getTaskId().toString());
        assertEquals(shard1.getTaskId().toString(), rec.getShardTasks().get(0).getTaskInfo().getTaskId().toString());
        assertEquals(shard2.getTaskId().toString(), rec.getShardTasks().get(1).getTaskInfo().getTaskId().toString());
        assertEquals(shard3.getTaskId().toString(), rec.getShardTasks().get(2).getTaskInfo().getTaskId().toString());
    }
}
