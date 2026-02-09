/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.insights.rules.model.TaskRecord;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the {@link LiveQueriesNodeResponse} class.
 */
public class LiveQueriesNodeResponseTests extends OpenSearchTestCase {

    private static TaskRecord createTaskRecord(String taskId) {
        return new TaskRecord(taskId, null, "node1", "RUNNING", "search", 1000L, 100L, 50L, 1024L, null, null);
    }

    private static TaskRecord createTaskRecordWithDetails(String taskId, String parentId, String description, String group) {
        return new TaskRecord(taskId, parentId, "node1", "RUNNING", "search", 1000L, 100L, 50L, 1024L, description, group);
    }

    private static DiscoveryNode createTestNode(String nodeId) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), Map.of(), Set.of(), null);
    }

    public void should_StoreNodeAndTasks_When_ConstructedWithValidData() {
        DiscoveryNode node = createTestNode("node1");
        List<TaskRecord> tasks = Arrays.asList(createTaskRecord("task1"), createTaskRecord("task2"));

        LiveQueriesNodeResponse response = new LiveQueriesNodeResponse(node, tasks);

        assertEquals(node, response.getNode());
        assertEquals(tasks, response.getTasks());
        assertEquals(2, response.getTasks().size());
    }

    public void should_HandleEmptyTasksList_When_NoTasksProvided() {
        DiscoveryNode node = createTestNode("node1");
        List<TaskRecord> tasks = Collections.emptyList();

        LiveQueriesNodeResponse response = new LiveQueriesNodeResponse(node, tasks);

        assertEquals(node, response.getNode());
        assertEquals(tasks, response.getTasks());
        assertTrue(response.getTasks().isEmpty());
    }

    public void should_PreserveAllFields_When_SerializedAndDeserialized() throws IOException {
        DiscoveryNode node = createTestNode("node1");
        List<TaskRecord> tasks = Arrays.asList(createTaskRecordWithDetails("task1", "parent1", "desc1", "group1"));
        LiveQueriesNodeResponse original = new LiveQueriesNodeResponse(node, tasks);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        LiveQueriesNodeResponse deserialized = new LiveQueriesNodeResponse(in);

        assertEquals(original.getNode().getId(), deserialized.getNode().getId());
        assertEquals(original.getTasks().size(), deserialized.getTasks().size());

        TaskRecord originalTask = original.getTasks().get(0);
        TaskRecord deserializedTask = deserialized.getTasks().get(0);
        assertTaskRecordsEqual(originalTask, deserializedTask);
    }

    public void should_SerializeCorrectly_When_TasksHaveNullValues() throws IOException {
        DiscoveryNode node = createTestNode("node1");
        List<TaskRecord> tasks = Arrays.asList(
            createTaskRecord("task1") // Has null parent, description, group
        );
        LiveQueriesNodeResponse original = new LiveQueriesNodeResponse(node, tasks);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        LiveQueriesNodeResponse deserialized = new LiveQueriesNodeResponse(in);

        TaskRecord deserializedTask = deserialized.getTasks().get(0);
        assertEquals("task1", deserializedTask.getTaskId());
        assertNull(deserializedTask.getParentTaskId());
        assertNull(deserializedTask.getDescription());
        assertNull(deserializedTask.getWlmGroupId());
    }

    public void should_SerializeEmptyTasksList_When_NoTasksProvided() throws IOException {
        DiscoveryNode node = createTestNode("node1");
        List<TaskRecord> tasks = Collections.emptyList();
        LiveQueriesNodeResponse original = new LiveQueriesNodeResponse(node, tasks);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        LiveQueriesNodeResponse deserialized = new LiveQueriesNodeResponse(in);

        assertTrue(deserialized.getTasks().isEmpty());
        assertEquals(original.getNode().getId(), deserialized.getNode().getId());
    }

    public void should_ReturnSameTasksReference_When_GetTasksCalled() {
        DiscoveryNode node = createTestNode("node1");
        List<TaskRecord> tasks = Arrays.asList(createTaskRecord("task1"));

        LiveQueriesNodeResponse response = new LiveQueriesNodeResponse(node, tasks);

        assertSame(tasks, response.getTasks());
        assertEquals(1, response.getTasks().size());
        assertEquals("task1", response.getTasks().get(0).getTaskId());
    }

    private void assertTaskRecordsEqual(TaskRecord expected, TaskRecord actual) {
        assertEquals(expected.getTaskId(), actual.getTaskId());
        assertEquals(expected.getParentTaskId(), actual.getParentTaskId());
        assertEquals(expected.getNodeId(), actual.getNodeId());
        assertEquals(expected.getStatus(), actual.getStatus());
        assertEquals(expected.getAction(), actual.getAction());
        assertEquals(expected.getStartTime(), actual.getStartTime());
        assertEquals(expected.getLatency(), actual.getLatency());
        assertEquals(expected.getCpu(), actual.getCpu());
        assertEquals(expected.getMemory(), actual.getMemory());
        assertEquals(expected.getDescription(), actual.getDescription());
        assertEquals(expected.getWlmGroupId(), actual.getWlmGroupId());
    }

    public void should_ThrowException_When_NullNodeProvided() {
        List<TaskRecord> tasks = Arrays.asList(createTaskRecord("task1"));

        assertThrows(NullPointerException.class, () -> { new LiveQueriesNodeResponse(null, tasks); });
    }

    public void should_ThrowException_When_NullTasksProvided() {
        DiscoveryNode node = createTestNode("node1");

        assertThrows(NullPointerException.class, () -> { new LiveQueriesNodeResponse(node, null); });
    }

    public void should_ThrowException_When_SerializingWithNullStream() {
        DiscoveryNode node = createTestNode("node1");
        List<TaskRecord> tasks = Arrays.asList(createTaskRecord("task1"));
        LiveQueriesNodeResponse response = new LiveQueriesNodeResponse(node, tasks);

        assertThrows(NullPointerException.class, () -> { response.writeTo(null); });
    }
}
