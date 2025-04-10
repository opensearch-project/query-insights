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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
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
        client = mock(Client.class, Answers.RETURNS_DEEP_STUBS); // Use deep stubs for client.admin().cluster()
        actionFilters = mock(ActionFilters.class);

        localNode = new DiscoveryNode(
            "local_node_id",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        when(clusterService.localNode()).thenReturn(localNode);
        when(clusterService.getClusterName()).thenReturn(new ClusterName("test-cluster"));

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

        // Mock the listTasks future
        PlainActionFuture<ListTasksResponse> future = PlainActionFuture.newFuture();
        future.onResponse(listTasksResponse);
        when(clusterAdminClient.listTasks(any(ListTasksRequest.class))).thenReturn(future);

        // Execute node operation
        TransportLiveQueriesAction.NodeRequest nodeRequest = transportLiveQueriesAction.newNodeRequest(request);
        LiveQueries liveQueries = transportLiveQueriesAction.nodeOperation(nodeRequest);

        // Verify ListTasksRequest parameters
        verify(clusterAdminClient).listTasks(any(ListTasksRequest.class));
        // Could add ArgumentCaptor to verify request details (verbose=true, actions filter)

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

        PlainActionFuture<ListTasksResponse> future = PlainActionFuture.newFuture();
        future.onResponse(listTasksResponse);
        when(clusterAdminClient.listTasks(any(ListTasksRequest.class))).thenReturn(future);

        TransportLiveQueriesAction.NodeRequest nodeRequest = transportLiveQueriesAction.newNodeRequest(request);
        LiveQueries liveQueries = transportLiveQueriesAction.nodeOperation(nodeRequest);

        assertEquals(1, liveQueries.getLiveQueries().size());
        SearchQueryRecord record = liveQueries.getLiveQueries().get(0);
        assertNull(record.getAttributes().get(Attribute.DESCRIPTION)); // Description should be null
        assertEquals(localNode.getId(), record.getAttributes().get(Attribute.NODE_ID));
        assertEquals(1000000L, record.getMeasurements().get(MetricType.LATENCY).getMeasurement().longValue());
    }

    public void testNodeOperationHandlesException() {
        LiveQueriesRequest request = new LiveQueriesRequest(true, localNode.getId());

        // Mock listTasks to throw an exception
        PlainActionFuture<ListTasksResponse> future = PlainActionFuture.newFuture();
        future.onFailure(new RuntimeException("Simulated task fetch error"));
        when(clusterAdminClient.listTasks(any(ListTasksRequest.class))).thenReturn(future);

        TransportLiveQueriesAction.NodeRequest nodeRequest = transportLiveQueriesAction.newNodeRequest(request);
        LiveQueries liveQueries = transportLiveQueriesAction.nodeOperation(nodeRequest);

        // Verify that the operation completes and returns an empty list
        assertNotNull(liveQueries);
        assertEquals(localNode, liveQueries.getNode());
        assertTrue(liveQueries.getLiveQueries().isEmpty());
        // Check logs for error message (cannot easily verify logs in unit test)
    }

    public void testNewResponse() {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        LiveQueries nodeResponse = new LiveQueries(localNode, Collections.emptyList());
        List<LiveQueries> responses = List.of(nodeResponse);
        List<FailedNodeException> failures = Collections.emptyList();

        LiveQueriesResponse clusterResponse = transportLiveQueriesAction.newResponse(request, responses, failures);

        assertNotNull(clusterResponse);
        assertEquals(clusterService.getClusterName(), clusterResponse.getClusterName());
        assertEquals(responses, clusterResponse.getNodes());
        assertEquals(failures, clusterResponse.failures());
    }

    public void testNewNodeRequest() {
        LiveQueriesRequest request = new LiveQueriesRequest(true, "node1");
        TransportLiveQueriesAction.NodeRequest nodeRequest = transportLiveQueriesAction.newNodeRequest(request);
        assertEquals(request, nodeRequest.request);
    }

    public void testNodeRequestSerialization() throws IOException {
        LiveQueriesRequest request = new LiveQueriesRequest(randomBoolean(), "node1", "node2");
        TransportLiveQueriesAction.NodeRequest originalNodeRequest = new TransportLiveQueriesAction.NodeRequest(request);
        BytesStreamOutput out = new BytesStreamOutput();
        originalNodeRequest.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        TransportLiveQueriesAction.NodeRequest deserializedNodeRequest = new TransportLiveQueriesAction.NodeRequest(in);

        assertEquals(originalNodeRequest.request.isVerbose(), deserializedNodeRequest.request.isVerbose());
        assertArrayEquals(originalNodeRequest.request.nodesIds(), deserializedNodeRequest.request.nodesIds());
    }
}
