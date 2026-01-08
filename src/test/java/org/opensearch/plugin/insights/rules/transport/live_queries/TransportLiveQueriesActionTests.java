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
import java.util.stream.Collectors;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
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
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.Attribute;
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
    private QueryInsightsService queryInsightsService;

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

        queryInsightsService = mock(QueryInsightsService.class);
        FinishedQueriesCache finishedQueriesCache = mock(FinishedQueriesCache.class);
        when(queryInsightsService.getFinishedQueriesCache()).thenReturn(finishedQueriesCache);

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
        List<SearchQueryRecord> records = clusterResponse.getLiveQueries();

        // Verify listTasks was called and DID NOT have node filter set
        ArgumentCaptor<ListTasksRequest> captor = ArgumentCaptor.forClass(ListTasksRequest.class);
        verify(clusterAdminClient).listTasks(captor.capture(), any(ActionListener.class));
        assertEquals(0, captor.getValue().getNodes().length);

        // Verify results - should have 2 search tasks in the flat list
        assertEquals(2, records.size());
        Map<String, SearchQueryRecord> resultsById = records.stream().collect(Collectors.toMap(SearchQueryRecord::getId, r -> r));

        assertTrue(resultsById.containsKey(task1.getTaskId().toString()));
        SearchQueryRecord record1 = resultsById.get(task1.getTaskId().toString());
        assertEquals(task1.getStartTime(), record1.getTimestamp());
        assertEquals("desc1", record1.getAttributes().get(Attribute.DESCRIPTION));
        assertEquals(1000000L, record1.getMeasurements().get(MetricType.LATENCY).getMeasurement().longValue());
        assertEquals(500L, record1.getMeasurements().get(MetricType.CPU).getMeasurement().longValue());
        assertEquals(1024L, record1.getMeasurements().get(MetricType.MEMORY).getMeasurement().longValue());

        assertTrue(resultsById.containsKey(task2.getTaskId().toString()));
        SearchQueryRecord record2 = resultsById.get(task2.getTaskId().toString());
        assertEquals(task2.getStartTime(), record2.getTimestamp());
        assertEquals("desc2", record2.getAttributes().get(Attribute.DESCRIPTION));
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
            ListTasksRequest listRequest = invocation.getArgument(0);
            // Simulate upstream filtering: only return tasks if node matches
            if (listRequest.getNodes() != null && listRequest.getNodes().length == 1 && listRequest.getNodes()[0].equals("node2")) {
                ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
                listener.onResponse(listTasksResponse);
            } else {
                ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
                // Return empty if node doesn't match
                listener.onResponse(new ListTasksResponse(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
            }
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        // Execute
        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        LiveQueriesResponse clusterResponse = futureResponse.actionGet();
        List<SearchQueryRecord> records = clusterResponse.getLiveQueries();

        // Verify listTasks was called and HAD node filter set
        ArgumentCaptor<ListTasksRequest> captor = ArgumentCaptor.forClass(ListTasksRequest.class);
        verify(clusterAdminClient).listTasks(captor.capture(), any(ActionListener.class));
        assertArrayEquals(new String[] { "node2" }, captor.getValue().getNodes());

        // Verify results - should only have task2 from node2
        assertEquals(1, records.size());
        SearchQueryRecord record2 = records.get(0);
        assertEquals(task2.getTaskId().toString(), record2.getId());
        assertEquals("desc2", record2.getAttributes().get(Attribute.DESCRIPTION));
    }

    public void testNodeOperationNonVerbose() throws IOException {
        // Request non-verbose for node1
        LiveQueriesRequest request = new LiveQueriesRequest(false, "node1");
        TaskInfo task1 = createTaskInfo(node1, "indices:data/read/search", System.currentTimeMillis(), 1000000L, "desc1", 500L, 1024L);
        List<TaskInfo> tasks = List.of(task1);
        ListTasksResponse listTasksResponse = new ListTasksResponse(tasks, emptyList(), emptyList());

        doAnswer(invocation -> {
            ListTasksRequest listRequest = invocation.getArgument(0);
            // Simulate upstream filtering: only return tasks if node matches
            if (listRequest.getNodes() != null && listRequest.getNodes().length == 1 && listRequest.getNodes()[0].equals("node1")) {
                ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
                listener.onResponse(listTasksResponse);
            } else {
                ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
                // Return empty if node doesn't match
                listener.onResponse(new ListTasksResponse(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
            }
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        LiveQueriesResponse clusterResponse = futureResponse.actionGet();
        List<SearchQueryRecord> records = clusterResponse.getLiveQueries();

        // Verify listTasks was called and HAD node filter set
        ArgumentCaptor<ListTasksRequest> captor = ArgumentCaptor.forClass(ListTasksRequest.class);
        verify(clusterAdminClient).listTasks(captor.capture(), any(ActionListener.class));
        assertArrayEquals(new String[] { "node1" }, captor.getValue().getNodes());

        assertEquals(1, records.size());
        SearchQueryRecord record = records.get(0);
        assertEquals(task1.getTaskId().toString(), record.getId());
        assertNull(record.getAttributes().get(Attribute.DESCRIPTION)); // Description should be null
        assertEquals(1000000L, record.getMeasurements().get(MetricType.LATENCY).getMeasurement().longValue());
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
        List<SearchQueryRecord> records = Collections.emptyList();
        LiveQueriesResponse response = new LiveQueriesResponse(records);
        assertNotNull(response);
        assertEquals(records, response.getLiveQueries());
    }

    public void testTransportActionSortsByCpuAndLimitsSize() throws IOException {
        // Prepare a request to sort by CPU and limit to 1 result
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.CPU, 1, new String[0], null, null, false, false);
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
        List<SearchQueryRecord> records = response.getLiveQueries();

        // Should only have the highCpu task
        assertEquals(1, records.size());
        assertEquals(highCpu.getTaskId().toString(), records.get(0).getId());
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
        SearchQueryRecord rec = response.getLiveQueries().get(0);
        assertEquals(0L, rec.getMeasurements().get(MetricType.CPU).getMeasurement().longValue());
        assertEquals(0L, rec.getMeasurements().get(MetricType.MEMORY).getMeasurement().longValue());
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
        SearchQueryRecord rec = response.getLiveQueries().get(0);
        assertEquals(0L, rec.getMeasurements().get(MetricType.CPU).getMeasurement().longValue());
        assertEquals(0L, rec.getMeasurements().get(MetricType.MEMORY).getMeasurement().longValue());
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
        SearchQueryRecord rec = response.getLiveQueries().get(0);
        assertEquals(0L, rec.getMeasurements().get(MetricType.CPU).getMeasurement().longValue());
        assertEquals(0L, rec.getMeasurements().get(MetricType.MEMORY).getMeasurement().longValue());
    }
}
