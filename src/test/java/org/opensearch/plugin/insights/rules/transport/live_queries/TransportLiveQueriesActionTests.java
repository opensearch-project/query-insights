/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesNodeRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesNodeResponse;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.LiveQueryRecord;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.TaskRecord;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class TransportLiveQueriesActionTests extends OpenSearchTestCase {

    private TransportLiveQueriesAction action;
    private ThreadPool threadPool;
    private TransportService transportService;
    private ClusterService clusterService;
    private ActionFilters actionFilters;
    private TaskManager taskManager;
    private DiscoveryNode localNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("test");
        transportService = mock(TransportService.class);
        clusterService = mock(ClusterService.class);
        actionFilters = mock(ActionFilters.class);
        taskManager = mock(TaskManager.class);
        localNode = mock(DiscoveryNode.class);

        when(clusterService.getClusterName()).thenReturn(new ClusterName("test-cluster"));
        when(transportService.getTaskManager()).thenReturn(taskManager);
        when(transportService.getLocalNode()).thenReturn(localNode);
        when(localNode.getId()).thenReturn("node1");

        action = new TransportLiveQueriesAction(threadPool, transportService, clusterService, actionFilters);
    }

    @Override
    public void tearDown() throws Exception {
        threadPool.shutdown();
        super.tearDown();
    }

    public void testNewResponseWithCoordinatorAndShardTasks() {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.LATENCY, 10, new String[0], null, null);

        TaskRecord coordinatorTask = new TaskRecord(
            "1",
            null,
            "node1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            100L,
            50L,
            1024L,
            "search query",
            "group1"
        );

        TaskRecord shardTask = new TaskRecord(
            "2",
            "node1:1",
            "node1",
            "RUNNING",
            "indices:data/read/search[phase/query]",
            1000L,
            50L,
            25L,
            512L,
            "shard query",
            "group1"
        );

        List<TaskRecord> tasks = List.of(coordinatorTask, shardTask);
        LiveQueriesNodeResponse nodeResponse = new LiveQueriesNodeResponse(localNode, tasks);
        List<LiveQueriesNodeResponse> responses = List.of(nodeResponse);
        List<FailedNodeException> failures = new ArrayList<>();

        LiveQueriesResponse response = action.newResponse(request, responses, failures);

        assertNotNull(response);
        assertEquals("test-cluster", response.getClusterName().value());

        List<LiveQueryRecord> records = response.getLiveQueries();
        assertEquals(1, records.size());

        LiveQueryRecord record = records.get(0);
        assertEquals("1", record.getQueryId());
        assertEquals("RUNNING", record.getStatus());
        assertEquals(1000L, record.getStartTime());
        assertEquals(100L, record.getTotalLatency());
        assertEquals(75L, record.getTotalCpu());
        assertEquals(1536L, record.getTotalMemory());
        assertEquals("group1", record.getWlmGroupId());
        assertNotNull(record.getCoordinatorTask());
        assertEquals(1, record.getShardTasks().size());
    }

    public void testNewResponseWithMultipleQueries() {
        LiveQueriesRequest request = new LiveQueriesRequest(false, new String[0]);

        TaskRecord query1Coordinator = new TaskRecord(
            "1",
            null,
            "node1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            200L,
            100L,
            2048L,
            "query 1",
            null
        );

        TaskRecord query2Coordinator = new TaskRecord(
            "3",
            null,
            "node2",
            "RUNNING",
            "indices:data/read/search",
            2000L,
            150L,
            75L,
            1536L,
            "query 2",
            "group2"
        );

        List<TaskRecord> tasks = List.of(query1Coordinator, query2Coordinator);
        LiveQueriesNodeResponse nodeResponse = new LiveQueriesNodeResponse(localNode, tasks);
        List<LiveQueriesNodeResponse> responses = List.of(nodeResponse);

        LiveQueriesResponse response = action.newResponse(request, responses, new ArrayList<>());

        List<LiveQueryRecord> records = response.getLiveQueries();
        assertEquals(2, records.size());

        LiveQueryRecord record1 = records.stream().filter(r -> "1".equals(r.getQueryId())).findFirst().orElse(null);
        assertNotNull(record1);
        assertEquals(200L, record1.getTotalLatency());
        assertNull(record1.getWlmGroupId());

        LiveQueryRecord record2 = records.stream().filter(r -> "3".equals(r.getQueryId())).findFirst().orElse(null);
        assertNotNull(record2);
        assertEquals(150L, record2.getTotalLatency());
        assertEquals("group2", record2.getWlmGroupId());
    }

    public void testNewResponseWithFailures() {
        LiveQueriesRequest request = new LiveQueriesRequest(false, new String[0]);
        List<LiveQueriesNodeResponse> responses = new ArrayList<>();
        List<FailedNodeException> failures = List.of(mock(FailedNodeException.class));

        LiveQueriesResponse response = action.newResponse(request, responses, failures);

        assertNotNull(response);
        assertTrue(response.getLiveQueries().isEmpty());
        assertEquals(1, response.failures().size());
    }

    public void testNewResponseWithSortingByCpu() {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.CPU, 10, new String[0], null, null);

        TaskRecord task1 = new TaskRecord(
            "1",
            null,
            "node1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            100L,
            200L,
            1024L,
            "query 1",
            null
        );
        TaskRecord task2 = new TaskRecord(
            "2",
            null,
            "node1",
            "RUNNING",
            "indices:data/read/search",
            2000L,
            150L,
            100L,
            2048L,
            "query 2",
            null
        );

        List<TaskRecord> tasks = List.of(task1, task2);
        LiveQueriesNodeResponse nodeResponse = new LiveQueriesNodeResponse(localNode, tasks);

        LiveQueriesResponse response = action.newResponse(request, List.of(nodeResponse), new ArrayList<>());
        List<LiveQueryRecord> records = response.getLiveQueries();

        assertEquals(2, records.size());
        assertEquals("1", records.get(0).getQueryId()); // Higher CPU first
        assertEquals("2", records.get(1).getQueryId());
    }

    public void testNewResponseWithSizeLimit() {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.LATENCY, 1, new String[0], null, null);

        TaskRecord task1 = new TaskRecord(
            "1",
            null,
            "node1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            200L,
            100L,
            1024L,
            "query 1",
            null
        );
        TaskRecord task2 = new TaskRecord(
            "2",
            null,
            "node1",
            "RUNNING",
            "indices:data/read/search",
            2000L,
            100L,
            50L,
            512L,
            "query 2",
            null
        );

        List<TaskRecord> tasks = List.of(task1, task2);
        LiveQueriesNodeResponse nodeResponse = new LiveQueriesNodeResponse(localNode, tasks);

        LiveQueriesResponse response = action.newResponse(request, List.of(nodeResponse), new ArrayList<>());
        List<LiveQueryRecord> records = response.getLiveQueries();

        assertEquals(1, records.size()); // Limited to 1
        assertEquals("1", records.get(0).getQueryId()); // Highest latency
    }

    public void testNewResponseWithTaskIdFilter() {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.LATENCY, 10, new String[0], null, "2");

        TaskRecord task1 = new TaskRecord(
            "1",
            null,
            "node1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            200L,
            100L,
            1024L,
            "query 1",
            null
        );
        TaskRecord task2 = new TaskRecord(
            "2",
            null,
            "node1",
            "RUNNING",
            "indices:data/read/search",
            2000L,
            100L,
            50L,
            512L,
            "query 2",
            null
        );

        List<TaskRecord> tasks = List.of(task1, task2);
        LiveQueriesNodeResponse nodeResponse = new LiveQueriesNodeResponse(localNode, tasks);

        LiveQueriesResponse response = action.newResponse(request, List.of(nodeResponse), new ArrayList<>());
        List<LiveQueryRecord> records = response.getLiveQueries();

        assertEquals(1, records.size());
        assertEquals("2", records.get(0).getQueryId()); // Only task 2
    }

    public void testNewResponseWithNodeIdFilter() {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.LATENCY, 10, new String[] { "node2" }, null, null);

        TaskRecord task1 = new TaskRecord(
            "1",
            null,
            "node1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            200L,
            100L,
            1024L,
            "query 1",
            null
        );
        TaskRecord task2 = new TaskRecord(
            "2",
            null,
            "node2",
            "RUNNING",
            "indices:data/read/search",
            2000L,
            100L,
            50L,
            512L,
            "query 2",
            null
        );

        List<TaskRecord> tasks = List.of(task1, task2);
        LiveQueriesNodeResponse nodeResponse = new LiveQueriesNodeResponse(localNode, tasks);

        LiveQueriesResponse response = action.newResponse(request, List.of(nodeResponse), new ArrayList<>());
        List<LiveQueryRecord> records = response.getLiveQueries();

        assertEquals(1, records.size());
        assertEquals("2", records.get(0).getQueryId()); // Only task on node2
    }

    public void testNewNodeRequest() {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.CPU, 5, new String[] { "node1" }, "group1", "task123");
        LiveQueriesNodeRequest nodeRequest = action.newNodeRequest(request);

        assertNotNull(nodeRequest);
        assertEquals(request, nodeRequest.getRequest());
        assertTrue(nodeRequest.getRequest().isVerbose());
        assertEquals(MetricType.CPU, nodeRequest.getRequest().getSortBy());
        assertEquals(5, nodeRequest.getRequest().getSize());
        assertEquals("group1", nodeRequest.getRequest().getWlmGroupId());
        assertEquals("task123", nodeRequest.getRequest().getTaskId());
    }

    public void testNodeOperationWithSearchTasks() {
        LiveQueriesRequest request = new LiveQueriesRequest(false, new String[0]);
        LiveQueriesNodeRequest nodeRequest = new LiveQueriesNodeRequest(request);

        Task searchTask = createMockTask(1L, null, "indices:data/read/search", 1000L);
        Task shardTask = createMockTask(2L, null, "indices:data/read/search[phase/query]", 2000L);
        Task nonSearchTask = createMockTask(3L, null, "cluster:monitor/nodes/info", 3000L);

        Map<Long, Task> tasks = Map.of(1L, searchTask, 2L, shardTask, 3L, nonSearchTask);
        when(taskManager.getTasks()).thenReturn(tasks);

        LiveQueriesNodeResponse response = action.nodeOperation(nodeRequest);

        assertNotNull(response);
        assertEquals(2, response.getTasks().size()); // Only search tasks

        List<TaskRecord> taskRecords = response.getTasks();
        assertTrue(
            taskRecords.stream()
                .allMatch(
                    t -> t.getAction().equals("indices:data/read/search") || t.getAction().startsWith("indices:data/read/search[phase/")
                )
        );
    }

    public void testNodeOperationWithWlmGroupFiltering() {
        LiveQueriesRequest request = new LiveQueriesRequest(false, MetricType.LATENCY, 10, new String[0], "target-group", null);
        LiveQueriesNodeRequest nodeRequest = new LiveQueriesNodeRequest(request);

        Task matchingTask = createMockTask(1L, null, "indices:data/read/search", 1000L);
        Task nonMatchingTask = createMockTask(2L, null, "indices:data/read/search", 2000L);

        Map<Long, Task> tasks = Map.of(1L, matchingTask, 2L, nonMatchingTask);
        when(taskManager.getTasks()).thenReturn(tasks);

        LiveQueriesNodeResponse response = action.nodeOperation(nodeRequest);

        assertNotNull(response);
        // Should filter based on WLM group (mocked tasks don't have WLM groups, so none match)
        assertEquals(0, response.getTasks().size());
    }

    public void testNodeOperationWithEmptyTasks() {
        LiveQueriesRequest request = new LiveQueriesRequest(false, new String[0]);
        LiveQueriesNodeRequest nodeRequest = new LiveQueriesNodeRequest(request);

        when(taskManager.getTasks()).thenReturn(new HashMap<>());

        LiveQueriesNodeResponse response = action.nodeOperation(nodeRequest);

        assertNotNull(response);
        assertTrue(response.getTasks().isEmpty());
    }

    private Task createMockTask(Long id, String parentId, String action, long startTime) {
        Task task = mock(Task.class);
        when(task.getId()).thenReturn(id);
        when(task.getAction()).thenReturn(action);
        when(task.getStartTime()).thenReturn(startTime);
        when(task.getStartTimeNanos()).thenReturn(System.nanoTime());
        when(task.getDescription()).thenReturn("test task " + id);
        when(task.getParentTaskId()).thenReturn(null);
        when(task.getTotalResourceStats()).thenReturn(null);

        return task;
    }
}
