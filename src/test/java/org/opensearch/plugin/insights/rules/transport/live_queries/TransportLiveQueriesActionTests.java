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
import org.mockito.Answers;
import org.opensearch.action.FailedNodeException;
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
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueries;
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
    private DiscoveryNode localNode;
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
        client = mock(Client.class, Answers.RETURNS_DEEP_STUBS); // Use deep stubs for client.admin().cluster()
        actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new org.opensearch.action.support.ActionFilter[0]);

        localNode = new DiscoveryNode(
            "local_node_id",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        when(clusterService.localNode()).thenReturn(localNode);
        when(clusterService.getClusterName()).thenReturn(new ClusterName("test-cluster"));
        // Stub cluster state to include the local node
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build();
        ClusterState clusterState = ClusterState.builder(clusterService.getClusterName()).nodes(discoveryNodes).build();
        when(clusterService.state()).thenReturn(clusterState);

        // Mock the client administrative calls
        adminClient = mock(AdminClient.class);
        clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        transportLiveQueriesAction = new TransportLiveQueriesAction(threadPool, clusterService, transportService, client, actionFilters);
    }

    private TaskInfo createTaskInfo(
        String action,
        long startTime,
        long runningTimeNanos,
        String description,
        long cpuNanos,
        long memoryBytes
    ) throws IOException {
        TaskId taskId = new TaskId(localNode.getId(), randomLong());

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

    public void testNodeOperationVerbose() throws IOException {
        LiveQueriesRequest request = new LiveQueriesRequest(true, localNode.getId());
        TaskInfo searchTask = createTaskInfo("indices:data/read/search", System.currentTimeMillis(), 1000000L, "desc1", 500L, 1024L);
        TaskInfo nonSearchTask = createTaskInfo(
            "cluster:monitor/nodes/tasks/list",
            System.currentTimeMillis(),
            2000000L,
            "desc2",
            600L,
            2048L
        );
        List<TaskInfo> tasks = List.of(searchTask, nonSearchTask);
        ListTasksResponse listTasksResponse = new ListTasksResponse(tasks, emptyList(), emptyList());

        // Mock the listTasks asynchronous call
        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        // Execute via execute()
        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        LiveQueriesResponse clusterResponse = futureResponse.actionGet();
        List<LiveQueries> nodes = clusterResponse.getNodes();
        assertEquals(1, nodes.size());
        LiveQueries liveQueries = nodes.get(0);

        // Verify listTasks invoked asynchronously
        verify(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        // Verify results
        assertEquals(1, liveQueries.getLiveQueries().size());
        SearchQueryRecord record = liveQueries.getLiveQueries().get(0);
        assertEquals(searchTask.getStartTime(), record.getTimestamp());
        assertEquals(localNode.getId(), record.getAttributes().get(Attribute.NODE_ID));
        assertEquals("desc1", record.getAttributes().get(Attribute.DESCRIPTION));
        assertEquals(1000000L, record.getMeasurements().get(MetricType.LATENCY).getMeasurement().longValue());
        assertEquals(500L, record.getMeasurements().get(MetricType.CPU).getMeasurement().longValue());
        assertEquals(1024L, record.getMeasurements().get(MetricType.MEMORY).getMeasurement().longValue());
        assertEquals(searchTask.getTaskId().toString(), record.getId());
        assertEquals(localNode, liveQueries.getNode());
    }

    public void testNodeOperationNonVerbose() throws IOException {
        LiveQueriesRequest request = new LiveQueriesRequest(false, localNode.getId());
        TaskInfo searchTask = createTaskInfo("indices:data/read/search", System.currentTimeMillis(), 1000000L, "desc1", 500L, 1024L);
        List<TaskInfo> tasks = List.of(searchTask);
        ListTasksResponse listTasksResponse = new ListTasksResponse(tasks, emptyList(), emptyList());

        doAnswer(invocation -> {
            ActionListener<ListTasksResponse> listener = invocation.getArgument(1);
            listener.onResponse(listTasksResponse);
            return null;
        }).when(clusterAdminClient).listTasks(any(ListTasksRequest.class), any(ActionListener.class));

        PlainActionFuture<LiveQueriesResponse> futureResponse = PlainActionFuture.newFuture();
        transportLiveQueriesAction.execute(request, futureResponse);
        LiveQueriesResponse clusterResponse = futureResponse.actionGet();
        LiveQueries liveQueries = clusterResponse.getNodes().get(0);

        assertEquals(1, liveQueries.getLiveQueries().size());
        SearchQueryRecord record = liveQueries.getLiveQueries().get(0);
        assertNull(record.getAttributes().get(Attribute.DESCRIPTION)); // Description should be null
        assertEquals(localNode.getId(), record.getAttributes().get(Attribute.NODE_ID));
        assertEquals(1000000L, record.getMeasurements().get(MetricType.LATENCY).getMeasurement().longValue());
    }

    public void testNodeOperationHandlesException() {
        LiveQueriesRequest request = new LiveQueriesRequest(true, localNode.getId());

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
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        LiveQueries nodeResponse = new LiveQueries(localNode, Collections.emptyList());
        List<LiveQueries> responses = List.of(nodeResponse);
        List<FailedNodeException> failures = Collections.emptyList();

        LiveQueriesResponse clusterResponse = new LiveQueriesResponse(
            clusterService.getClusterName(),
            responses,
            failures,
            request.isVerbose()
        );

        assertNotNull(clusterResponse);
        assertEquals(clusterService.getClusterName(), clusterResponse.getClusterName());
        assertEquals(responses, clusterResponse.getNodes());
        assertEquals(failures, clusterResponse.failures());
    }
}
