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
        long startTime1 = System.currentTimeMillis();
        long startTime2 = System.currentTimeMillis() + 100;
        TaskInfo task1 = createTaskInfo(node1, "indices:data/read/search", startTime1, 1000000L, "desc1", 500L, 1024L);
        TaskInfo shard1 = createTaskInfo(node1, "indices:data/read/search[phase/query]", startTime1, 500000L, "shard1", 200L, 512L);
        TaskInfo task2 = createTaskInfo(node2, "indices:data/read/search", startTime2, 2000000L, "desc2", 600L, 2048L);
        TaskInfo shard2 = createTaskInfo(node2, "indices:data/read/search[phase/fetch]", startTime2, 800000L, "shard2", 300L, 1024L);
        TaskInfo nonSearchTask1 = createTaskInfo(
            node1,
            "cluster:monitor/nodes/tasks/list",
            System.currentTimeMillis(),
            3000000L,
            "desc3",
            700L,
            4096L
        );

        TaskGroup group1 = new TaskGroup(task1, List.of(new TaskGroup(shard1, emptyList())));
        TaskGroup group2 = new TaskGroup(task2, List.of(new TaskGroup(shard2, emptyList())));
        TaskGroup nonSearchGroup = new TaskGroup(nonSearchTask1, emptyList());
        ListTasksResponse mockResponse = mock(ListTasksResponse.class);
        when(mockResponse.getTaskGroups()).thenReturn(List.of(group1, group2, nonSearchGroup));

        // Mock the listTasks asynchronous call
        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockResponse);
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
        assertTrue(captor.getValue().getDetailed());

        // Verify results - should have 2 search tasks, non-search tasks filtered out
        assertEquals(2, records.size());
        assertFalse(records.stream().anyMatch(r -> r.getQueryId().contains(nonSearchTask1.getTaskId().toString())));

        LiveQueryRecord record1 = records.stream()
            .filter(r -> r.getQueryId().equals(task1.getTaskId().toString()))
            .findFirst()
            .orElse(null);
        assertNotNull(record1);
        assertEquals(task1.getTaskId().toString(), record1.getQueryId());
        assertEquals("running", record1.getStatus());
        assertEquals(startTime1, record1.getStartTime());
        assertEquals(1L, record1.getTotalLatency());
        assertEquals(700L, record1.getTotalCpu()); // 500 + 200
        assertEquals(1536L, record1.getTotalMemory()); // 1024 + 512
        assertNotNull(record1.getCoordinatorTask());
        assertEquals(1, record1.getShardTasks().size());
        assertEquals("running", record1.getShardTasks().get(0).getStatus());
        assertEquals(
            200L,
            record1.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            512L,
            record1.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );

        LiveQueryRecord record2 = records.stream()
            .filter(r -> r.getQueryId().equals(task2.getTaskId().toString()))
            .findFirst()
            .orElse(null);
        assertNotNull(record2);
        assertEquals(task2.getTaskId().toString(), record2.getQueryId());
        assertEquals("running", record2.getStatus());
        assertEquals(startTime2, record2.getStartTime());
        assertEquals(2L, record2.getTotalLatency());
        assertEquals(900L, record2.getTotalCpu()); // 600 + 300
        assertEquals(3072L, record2.getTotalMemory()); // 2048 + 1024
        assertNotNull(record2.getCoordinatorTask());
        assertEquals(1, record2.getShardTasks().size());
        assertEquals("running", record2.getShardTasks().get(0).getStatus());
        assertEquals(
            300L,
            record2.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            1024L,
            record2.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
    }

    public void testNodeFiltering_SpecificNode() throws IOException {
        // Request only node2
        LiveQueriesRequest request = new LiveQueriesRequest(true, "node2");
        // Prepare response as if listTasks only returned tasks for node2
        TaskInfo task2 = createTaskInfo(node2, "indices:data/read/search", System.currentTimeMillis(), 2000000L, "desc2", 600L, 2048L);
        TaskInfo shard2 = createTaskInfo(
            node2,
            "indices:data/read/search[phase/query]",
            System.currentTimeMillis(),
            1000000L,
            "shard2",
            250L,
            1024L
        );
        TaskInfo nonSearchTask = createTaskInfo(
            node2,
            "cluster:monitor/nodes/stats",
            System.currentTimeMillis(),
            1000000L,
            "stats",
            100L,
            512L
        );

        TaskGroup group2 = new TaskGroup(task2, List.of(new TaskGroup(shard2, emptyList())));
        TaskGroup nonSearchGroup = new TaskGroup(nonSearchTask, emptyList());
        ListTasksResponse mockResponse = mock(ListTasksResponse.class);
        when(mockResponse.getTaskGroups()).thenReturn(List.of(group2, nonSearchGroup));

        // Mock the listTasks asynchronous call
        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockResponse);
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

        // Verify results - should only have task2 from node2, non-search task filtered out
        assertEquals(1, records.size());
        assertFalse(records.stream().anyMatch(r -> r.getQueryId().contains(nonSearchTask.getTaskId().toString())));
        LiveQueryRecord record2 = records.get(0);
        assertEquals(task2.getTaskId().toString(), record2.getQueryId());
        assertEquals("running", record2.getStatus());
        assertEquals(2L, record2.getTotalLatency());
        assertEquals(850L, record2.getTotalCpu()); // 600 + 250
        assertEquals(3072L, record2.getTotalMemory()); // 2048 + 1024
        assertNotNull(record2.getCoordinatorTask());
        assertEquals(1, record2.getShardTasks().size());
        assertEquals("running", record2.getShardTasks().get(0).getStatus());
        assertEquals(
            250L,
            record2.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            1024L,
            record2.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
    }

    public void testNodeOperationNonVerbose() throws IOException {
        // Request non-verbose for node1
        LiveQueriesRequest request = new LiveQueriesRequest(false, "node1");
        TaskInfo task1 = createTaskInfo(node1, "indices:data/read/search", System.currentTimeMillis(), 1000000L, "desc1", 500L, 1024L);
        TaskInfo shard1 = createTaskInfo(
            node1,
            "indices:data/read/search[phase/query]",
            System.currentTimeMillis(),
            500000L,
            "shard1",
            150L,
            512L
        );
        TaskInfo nonSearchTask = createTaskInfo(node1, "indices:admin/refresh", System.currentTimeMillis(), 500000L, "refresh", 50L, 256L);

        TaskGroup group1 = new TaskGroup(task1, List.of(new TaskGroup(shard1, emptyList())));
        TaskGroup nonSearchGroup = new TaskGroup(nonSearchTask, emptyList());
        ListTasksResponse mockResponse = mock(ListTasksResponse.class);
        when(mockResponse.getTaskGroups()).thenReturn(List.of(group1, nonSearchGroup));

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockResponse);
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
        assertFalse(records.stream().anyMatch(r -> r.getQueryId().contains(nonSearchTask.getTaskId().toString())));
        LiveQueryRecord record = records.get(0);
        assertEquals(task1.getTaskId().toString(), record.getQueryId());
        assertEquals("running", record.getStatus());
        assertEquals(1L, record.getTotalLatency());
        assertEquals(650L, record.getTotalCpu()); // 500 + 150
        assertEquals(1536L, record.getTotalMemory()); // 1024 + 512
        assertNotNull(record.getCoordinatorTask());
        assertEquals(1, record.getShardTasks().size());
        assertEquals("running", record.getShardTasks().get(0).getStatus());
        assertEquals(
            150L,
            record.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            512L,
            record.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
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
        TaskInfo lowShard = createTaskInfo(
            node1,
            "indices:data/read/search[phase/query]",
            System.currentTimeMillis(),
            500L,
            "lowShard",
            50L,
            50L
        );
        TaskInfo highCpu = createTaskInfo(node1, "indices:data/read/search", System.currentTimeMillis(), 2000L, "high", 200L, 200L);
        TaskInfo highShard = createTaskInfo(
            node1,
            "indices:data/read/search[phase/query]",
            System.currentTimeMillis(),
            1000L,
            "highShard",
            100L,
            100L
        );
        TaskInfo midCpu = createTaskInfo(node1, "indices:data/read/search", System.currentTimeMillis(), 1500L, "mid", 150L, 150L);
        TaskInfo midShard = createTaskInfo(
            node1,
            "indices:data/read/search[phase/query]",
            System.currentTimeMillis(),
            750L,
            "midShard",
            75L,
            75L
        );
        TaskInfo nonSearchTask = createTaskInfo(node1, "indices:data/write/bulk", System.currentTimeMillis(), 5000L, "bulk", 500L, 500L);

        TaskGroup lowGroup = new TaskGroup(lowCpu, List.of(new TaskGroup(lowShard, emptyList())));
        TaskGroup highGroup = new TaskGroup(highCpu, List.of(new TaskGroup(highShard, emptyList())));
        TaskGroup midGroup = new TaskGroup(midCpu, List.of(new TaskGroup(midShard, emptyList())));
        TaskGroup nonSearchGroup = new TaskGroup(nonSearchTask, emptyList());
        ListTasksResponse mockResponse = mock(ListTasksResponse.class);
        when(mockResponse.getTaskGroups()).thenReturn(List.of(lowGroup, highGroup, midGroup, nonSearchGroup));

        // Mock the async call
        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        // Execute transport action
        PlainActionFuture<LiveQueriesResponse> future = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, future);
        LiveQueriesResponse response = future.actionGet();
        List<LiveQueryRecord> records = response.getLiveQueries();

        // Should only have the highCpu task, non-search task filtered out
        assertEquals(1, records.size());
        assertFalse(records.stream().anyMatch(r -> r.getQueryId().contains(nonSearchTask.getTaskId().toString())));
        LiveQueryRecord record = records.get(0);
        assertEquals(highCpu.getTaskId().toString(), record.getQueryId());
        assertEquals("running", record.getStatus());
        assertEquals(0L, record.getTotalLatency());
        assertEquals(300L, record.getTotalCpu()); // 200 + 100
        assertEquals(300L, record.getTotalMemory()); // 200 + 100
        assertNotNull(record.getCoordinatorTask());
        assertEquals(1, record.getShardTasks().size());
        assertEquals("running", record.getShardTasks().get(0).getStatus());
        assertEquals(
            100L,
            record.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            100L,
            record.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
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
        assertEquals("running", rec.getStatus());
        assertEquals(0L, rec.getTotalCpu());
        assertEquals(0L, rec.getTotalMemory());
        assertEquals(0L, rec.getTotalLatency());
        assertNotNull(rec.getCoordinatorTask());
        assertEquals(0, rec.getShardTasks().size());
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
        assertEquals("running", rec.getStatus());
        assertEquals(0L, rec.getTotalCpu());
        assertEquals(0L, rec.getTotalMemory());
        assertEquals(0L, rec.getTotalLatency());
        assertNotNull(rec.getCoordinatorTask());
        assertEquals(0, rec.getShardTasks().size());
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
        assertEquals("running", rec.getStatus());
        assertEquals(0L, rec.getTotalCpu());
        assertEquals(0L, rec.getTotalMemory());
        assertEquals(0L, rec.getTotalLatency());
        assertNotNull(rec.getCoordinatorTask());
        assertEquals(0, rec.getShardTasks().size());
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

        assertEquals("running", rec.getStatus());
        assertEquals(0L, rec.getTotalLatency());
        assertEquals(900L, rec.getTotalCpu()); // 100 + 300 + 500
        assertEquals(1200L, rec.getTotalMemory()); // 200 + 400 + 600
        assertEquals(2, rec.getShardTasks().size());
        assertNotNull(rec.getCoordinatorTask());
        assertEquals(
            100L,
            rec.getCoordinatorTask().getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            200L,
            rec.getCoordinatorTask().getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
        assertEquals("running", rec.getShardTasks().get(0).getStatus());
        assertEquals(
            300L,
            rec.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            400L,
            rec.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
        assertEquals("running", rec.getShardTasks().get(1).getStatus());
        assertEquals(
            500L,
            rec.getShardTasks().get(1).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            600L,
            rec.getShardTasks().get(1).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
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
        LiveQueryRecord query1 = response.getLiveQueries().get(0);
        LiveQueryRecord query2 = response.getLiveQueries().get(1);
        assertEquals("running", query1.getStatus());
        assertEquals(0L, query1.getTotalLatency());
        assertEquals(400L, query1.getTotalCpu()); // 100 + 300
        assertEquals(600L, query1.getTotalMemory()); // 200 + 400
        assertEquals(1, query1.getShardTasks().size());
        assertEquals("running", query1.getShardTasks().get(0).getStatus());
        assertEquals(
            300L,
            query1.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            400L,
            query1.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
        assertEquals("running", query2.getStatus());
        assertEquals(0L, query2.getTotalLatency());
        assertEquals(1200L, query2.getTotalCpu()); // 500 + 700
        assertEquals(1400L, query2.getTotalMemory()); // 600 + 800
        assertEquals(1, query2.getShardTasks().size());
        assertEquals("running", query2.getShardTasks().get(0).getStatus());
        assertEquals(
            700L,
            query2.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            800L,
            query2.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
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

        assertEquals("running", rec.getStatus());
        assertEquals(0L, rec.getTotalLatency());
        assertEquals(100L, rec.getTotalCpu());
        assertEquals(200L, rec.getTotalMemory());
        assertEquals(0, rec.getShardTasks().size());
        assertNotNull(rec.getCoordinatorTask());
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
        LiveQueryRecord rec = response.getLiveQueries().get(0);
        assertEquals("running", rec.getStatus());
        assertEquals(0L, rec.getTotalLatency());
        assertEquals(100L, rec.getTotalCpu());
        assertEquals(200L, rec.getTotalMemory());
        assertNotNull(rec.getCoordinatorTask());
        assertEquals(0, rec.getShardTasks().size());
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
        assertEquals("running", query1.getStatus());
        assertEquals(0L, query1.getTotalLatency());
        assertEquals(2, query1.getShardTasks().size());
        assertEquals(900L, query1.getTotalCpu()); // 100 + 300 + 500
        assertEquals(1200L, query1.getTotalMemory()); // 200 + 400 + 600
        assertNotNull(query1.getCoordinatorTask());
        assertEquals("running", query1.getShardTasks().get(0).getStatus());
        assertEquals(
            300L,
            query1.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            400L,
            query1.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
        assertEquals("running", query1.getShardTasks().get(1).getStatus());
        assertEquals(
            500L,
            query1.getShardTasks().get(1).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            600L,
            query1.getShardTasks().get(1).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );

        assertEquals(coord2.getTaskId().toString(), query2.getQueryId());
        assertEquals("running", query2.getStatus());
        assertEquals(0L, query2.getTotalLatency());
        assertEquals(1, query2.getShardTasks().size());
        assertEquals(1600L, query2.getTotalCpu()); // 700 + 900
        assertEquals(1800L, query2.getTotalMemory()); // 800 + 1000
        assertNotNull(query2.getCoordinatorTask());
        assertEquals("running", query2.getShardTasks().get(0).getStatus());
        assertEquals(
            900L,
            query2.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            1000L,
            query2.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
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

        assertEquals("running", rec.getStatus());
        assertEquals(229L, rec.getTotalLatency());
        assertEquals(26630000L, rec.getTotalCpu()); // 11084000 + 6418000 + 4564000 + 4564000
        assertEquals(3243784L, rec.getTotalMemory()); // 1719784 + 856072 + 316328 + 351600
        assertEquals(3, rec.getShardTasks().size());
        assertNotNull(rec.getCoordinatorTask());
        assertEquals(
            11084000L,
            rec.getCoordinatorTask().getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            1719784L,
            rec.getCoordinatorTask().getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
        assertEquals(coord.getTaskId().toString(), rec.getQueryId());
        assertEquals(coord.getTaskId().toString(), rec.getCoordinatorTask().getTaskInfo().getTaskId().toString());
        assertEquals(shard1.getTaskId().toString(), rec.getShardTasks().get(0).getTaskInfo().getTaskId().toString());
        assertEquals("running", rec.getShardTasks().get(0).getStatus());
        assertEquals(
            6418000L,
            rec.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            856072L,
            rec.getShardTasks().get(0).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
        assertEquals(shard2.getTaskId().toString(), rec.getShardTasks().get(1).getTaskInfo().getTaskId().toString());
        assertEquals("running", rec.getShardTasks().get(1).getStatus());
        assertEquals(
            4564000L,
            rec.getShardTasks().get(1).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            316328L,
            rec.getShardTasks().get(1).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
        assertEquals(shard3.getTaskId().toString(), rec.getShardTasks().get(2).getTaskInfo().getTaskId().toString());
        assertEquals("running", rec.getShardTasks().get(2).getStatus());
        assertEquals(
            4564000L,
            rec.getShardTasks().get(2).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getCpuTimeInNanos()
        );
        assertEquals(
            351600L,
            rec.getShardTasks().get(2).getTaskInfo().getResourceStats().getResourceUsageInfo().get("total").getMemoryInBytes()
        );
    }
}
