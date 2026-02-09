/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class TaskRecordTests extends OpenSearchTestCase {

    public void testTaskRecordCreation() {
        TaskRecord record = new TaskRecord(
            "task-1",
            "parent-1",
            "node-1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            100L,
            50L,
            1024L,
            "test task",
            "group-1"
        );

        assertEquals("task-1", record.getTaskId());
        assertEquals("parent-1", record.getParentTaskId());
        assertEquals("node-1", record.getNodeId());
        assertEquals("RUNNING", record.getStatus());
        assertEquals("indices:data/read/search", record.getAction());
        assertEquals(1000L, record.getStartTime());
        assertEquals(100L, record.getLatency());
        assertEquals(50L, record.getCpu());
        assertEquals(1024L, record.getMemory());
        assertEquals("test task", record.getDescription());
        assertEquals("group-1", record.getWlmGroupId());
    }

    public void testTaskRecordWithNullValues() {
        TaskRecord record = new TaskRecord(
            "task-1",
            null,
            "node-1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            100L,
            50L,
            1024L,
            null,
            null
        );

        assertEquals("task-1", record.getTaskId());
        assertNull(record.getParentTaskId());
        assertNull(record.getDescription());
        assertNull(record.getWlmGroupId());
    }

    public void testSerialization() throws IOException {
        TaskRecord original = new TaskRecord(
            "task-1",
            "parent-1",
            "node-1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            100L,
            50L,
            1024L,
            "test task",
            "group-1"
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        TaskRecord deserialized = new TaskRecord(in);

        assertEquals(original.getTaskId(), deserialized.getTaskId());
        assertEquals(original.getParentTaskId(), deserialized.getParentTaskId());
        assertEquals(original.getNodeId(), deserialized.getNodeId());
        assertEquals(original.getStatus(), deserialized.getStatus());
        assertEquals(original.getAction(), deserialized.getAction());
        assertEquals(original.getStartTime(), deserialized.getStartTime());
        assertEquals(original.getLatency(), deserialized.getLatency());
        assertEquals(original.getCpu(), deserialized.getCpu());
        assertEquals(original.getMemory(), deserialized.getMemory());
        assertEquals(original.getDescription(), deserialized.getDescription());
        assertEquals(original.getWlmGroupId(), deserialized.getWlmGroupId());
    }

    public void testSerializationWithNulls() throws IOException {
        TaskRecord original = new TaskRecord(
            "task-1",
            null,
            "node-1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            100L,
            50L,
            1024L,
            null,
            null
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        TaskRecord deserialized = new TaskRecord(in);

        assertEquals(original.getTaskId(), deserialized.getTaskId());
        assertNull(deserialized.getParentTaskId());
        assertNull(deserialized.getDescription());
        assertNull(deserialized.getWlmGroupId());
    }

    public void testToXContent() throws IOException {
        TaskRecord record = new TaskRecord(
            "task-1",
            "parent-1",
            "node-1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            100L,
            50L,
            1024L,
            "test task",
            "group-1"
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"task_id\":\"task-1\""));
        assertTrue(json.contains("\"parent_task_id\":\"parent-1\""));
        assertTrue(json.contains("\"node_id\":\"node-1\""));
        assertTrue(json.contains("\"status\":\"RUNNING\""));
        assertTrue(json.contains("\"action\":\"indices:data/read/search\""));
        assertTrue(json.contains("\"start_time\":1000"));
        assertTrue(json.contains("\"latency_millis\":100"));
        assertTrue(json.contains("\"cpu_nanos\":50"));
        assertTrue(json.contains("\"memory_bytes\":1024"));
        assertTrue(json.contains("\"description\":\"test task\""));
    }

    public void testToXContentWithNulls() throws IOException {
        TaskRecord record = new TaskRecord(
            "task-1",
            null,
            "node-1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            100L,
            50L,
            1024L,
            null,
            null
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"task_id\":\"task-1\""));
        assertFalse(json.contains("parent_task_id"));
        assertFalse(json.contains("description"));
    }
}
