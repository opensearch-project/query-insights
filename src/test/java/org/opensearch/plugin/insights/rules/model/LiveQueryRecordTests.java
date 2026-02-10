/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.List;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class LiveQueryRecordTests extends OpenSearchTestCase {

    public void testLiveQueryRecordCreation() {
        TaskRecord coordinator = new TaskRecord(
            "1",
            null,
            "node1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            100L,
            50L,
            1024L,
            "coordinator",
            "group1"
        );

        TaskRecord shard = new TaskRecord(
            "2",
            "node1:1",
            "node1",
            "RUNNING",
            "indices:data/read/search[phase/query]",
            1000L,
            50L,
            25L,
            512L,
            "shard",
            "group1"
        );

        LiveQueryRecord record = new LiveQueryRecord("query-1", "RUNNING", 1000L, "group1", 150L, 75L, 1536L, coordinator, List.of(shard));

        assertEquals("query-1", record.getQueryId());
        assertEquals("RUNNING", record.getStatus());
        assertEquals(1000L, record.getStartTime());
        assertEquals("group1", record.getWlmGroupId());
        assertEquals(150L, record.getTotalLatency());
        assertEquals(75L, record.getTotalCpu());
        assertEquals(1536L, record.getTotalMemory());
        assertEquals(coordinator, record.getCoordinatorTask());
        assertEquals(1, record.getShardTasks().size());
        assertEquals(shard, record.getShardTasks().get(0));
    }

    public void testLiveQueryRecordWithNullValues() {
        LiveQueryRecord record = new LiveQueryRecord("query-1", "RUNNING", 1000L, null, 150L, 75L, 1536L, null, null);

        assertEquals("query-1", record.getQueryId());
        assertNull(record.getWlmGroupId());
        assertNull(record.getCoordinatorTask());
        assertTrue(record.getShardTasks().isEmpty());
    }

    public void testSerialization() throws IOException {
        TaskRecord coordinator = new TaskRecord(
            "1",
            null,
            "node1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            100L,
            50L,
            1024L,
            "coordinator",
            "group1"
        );

        TaskRecord shard = new TaskRecord(
            "2",
            "node1:1",
            "node1",
            "RUNNING",
            "indices:data/read/search[phase/query]",
            1000L,
            50L,
            25L,
            512L,
            "shard",
            "group1"
        );

        LiveQueryRecord original = new LiveQueryRecord(
            "query-1",
            "RUNNING",
            1000L,
            "group1",
            150L,
            75L,
            1536L,
            coordinator,
            List.of(shard)
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LiveQueryRecord deserialized = new LiveQueryRecord(in);

        assertEquals(original.getQueryId(), deserialized.getQueryId());
        assertEquals(original.getStatus(), deserialized.getStatus());
        assertEquals(original.getStartTime(), deserialized.getStartTime());
        assertEquals(original.getWlmGroupId(), deserialized.getWlmGroupId());
        assertEquals(original.getTotalLatency(), deserialized.getTotalLatency());
        assertEquals(original.getTotalCpu(), deserialized.getTotalCpu());
        assertEquals(original.getTotalMemory(), deserialized.getTotalMemory());
        assertEquals(1, deserialized.getShardTasks().size());
        assertNotNull(deserialized.getCoordinatorTask());
    }

    public void testSerializationWithNulls() throws IOException {
        LiveQueryRecord original = new LiveQueryRecord("query-1", "RUNNING", 1000L, null, 150L, 75L, 1536L, null, null);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LiveQueryRecord deserialized = new LiveQueryRecord(in);

        assertEquals(original.getQueryId(), deserialized.getQueryId());
        assertNull(deserialized.getWlmGroupId());
        assertNull(deserialized.getCoordinatorTask());
        assertTrue(deserialized.getShardTasks().isEmpty());
    }

    public void testToXContent() throws IOException {
        TaskRecord coordinator = new TaskRecord(
            "1",
            null,
            "node1",
            "RUNNING",
            "indices:data/read/search",
            1000L,
            100L,
            50L,
            1024L,
            "coordinator",
            "group1"
        );

        TaskRecord shard = new TaskRecord(
            "2",
            "node1:1",
            "node1",
            "RUNNING",
            "indices:data/read/search[phase/query]",
            1000L,
            50L,
            25L,
            512L,
            "shard",
            "group1"
        );

        LiveQueryRecord record = new LiveQueryRecord("query-1", "RUNNING", 1000L, "group1", 150L, 75L, 1536L, coordinator, List.of(shard));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"id\":\"query-1\""));
        assertTrue(json.contains("\"status\":\"RUNNING\""));
        assertTrue(json.contains("\"start_time\":1000"));
        assertTrue(json.contains("\"wlm_group_id\":\"group1\""));
        assertTrue(json.contains("\"total_latency_millis\":150"));
        assertTrue(json.contains("\"total_cpu_nanos\":75"));
        assertTrue(json.contains("\"total_memory_bytes\":1536"));
        assertTrue(json.contains("\"coordinator_task\""));
        assertTrue(json.contains("\"shard_tasks\""));
    }

    public void testToXContentWithNulls() throws IOException {
        LiveQueryRecord record = new LiveQueryRecord("query-1", "RUNNING", 1000L, null, 150L, 75L, 1536L, null, null);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"id\":\"query-1\""));
        assertFalse(json.contains("wlm_group_id"));
        assertFalse(json.contains("coordinator_task"));
        assertTrue(json.contains("\"shard_tasks\":[]"));
    }

    public void testEmptyShardTasks() {
        LiveQueryRecord record = new LiveQueryRecord("query-1", "RUNNING", 1000L, "group1", 150L, 75L, 1536L, null, List.of());

        assertTrue(record.getShardTasks().isEmpty());
    }
}
