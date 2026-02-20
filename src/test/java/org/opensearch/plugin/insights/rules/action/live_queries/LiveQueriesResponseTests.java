/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.LiveQueryRecord;
import org.opensearch.plugin.insights.rules.model.TaskDetails;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the {@link LiveQueriesResponse} class.
 */
public class LiveQueriesResponseTests extends OpenSearchTestCase {

    private List<LiveQueryRecord> createLiveQueriesList(int count, long baseLatency) {
        return IntStream.range(0, count).mapToObj(i -> {
            TaskInfo coordinator = new TaskInfo(
                new TaskId("node_" + i, i),
                "test_type",
                "indices:data/read/search",
                "desc_" + i,
                null,
                System.currentTimeMillis(),
                baseLatency + i * 100,
                true,
                false,
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap(),
                null
            );
            return new LiveQueryRecord(
                "query_" + i,
                "running",
                System.currentTimeMillis(),
                "wlm_" + i,
                baseLatency + i * 100,
                randomLongBetween(10, 1000),
                randomLongBetween(1024, 10240),
                new TaskDetails(coordinator, "running"),
                new ArrayList<>()
            );
        }).collect(Collectors.toList());
    }

    public void testSerialization() throws IOException {
        List<LiveQueryRecord> queries = createLiveQueriesList(3, 1000);
        LiveQueriesResponse originalResponse = new LiveQueriesResponse(queries);
        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesResponse deserializedResponse = new LiveQueriesResponse(in);

        assertEquals(originalResponse.getLiveQueries().size(), deserializedResponse.getLiveQueries().size());
    }

    public void testToXContent() throws IOException {
        TaskInfo coordinator1 = new TaskInfo(
            new TaskId("node1", 1),
            "test_type",
            "indices:data/read/search",
            "desc1",
            null,
            1L,
            10L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            null
        );
        TaskInfo coordinator2 = new TaskInfo(
            new TaskId("node2", 2),
            "test_type",
            "indices:data/read/search",
            "desc2",
            null,
            2L,
            10L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            null
        );
        TaskInfo coordinator3 = new TaskInfo(
            new TaskId("node3", 3),
            "test_type",
            "indices:data/read/search",
            "desc3",
            null,
            3L,
            10L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            null
        );
        LiveQueryRecord rec1 = new LiveQueryRecord(
            "id1",
            "running",
            1L,
            null,
            10L,
            20L,
            30L,
            new TaskDetails(coordinator1, "running"),
            new ArrayList<>()
        );
        LiveQueryRecord rec2 = new LiveQueryRecord(
            "id2",
            "running",
            2L,
            null,
            10L,
            20L,
            30L,
            new TaskDetails(coordinator2, "running"),
            new ArrayList<>()
        );
        LiveQueryRecord rec3 = new LiveQueryRecord(
            "id3",
            "running",
            3L,
            null,
            10L,
            20L,
            30L,
            new TaskDetails(coordinator3, "running"),
            new ArrayList<>()
        );
        List<LiveQueryRecord> records = List.of(rec2, rec1, rec3);
        LiveQueriesResponse response = new LiveQueriesResponse(records);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> liveQueriesList = (List<Map<String, Object>>) parsed.get("live_queries");
        assertEquals(records.size(), liveQueriesList.size());
        assertEquals("id2", liveQueriesList.get(0).get("id"));
        assertEquals("id1", liveQueriesList.get(1).get("id"));
        assertEquals("id3", liveQueriesList.get(2).get("id"));
    }

    public void testToXContentEmptyList() throws IOException {
        LiveQueriesResponse response = new LiveQueriesResponse(Collections.emptyList());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertEquals("{\"live_queries\":[]}", json);
    }

    public void testTotalCpuMemoryIncludesCoordinatorAndShards() throws IOException {
        TaskInfo coordinator = new TaskInfo(
            new TaskId("node1", 1),
            "test_type",
            "indices:data/read/search",
            "desc",
            null,
            1L,
            100L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            null
        );
        TaskInfo shard1 = new TaskInfo(
            new TaskId("node2", 2),
            "test_type",
            "indices:data/read/search[phase/query]",
            "shard1",
            null,
            2L,
            50L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            null
        );
        TaskInfo shard2 = new TaskInfo(
            new TaskId("node3", 3),
            "test_type",
            "indices:data/read/search[phase/query]",
            "shard2",
            null,
            3L,
            50L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            null
        );

        long coordCpu = 1000L;
        long coordMem = 2000L;
        long shard1Cpu = 500L;
        long shard1Mem = 1000L;
        long shard2Cpu = 300L;
        long shard2Mem = 800L;
        long totalCpu = coordCpu + shard1Cpu + shard2Cpu;
        long totalMem = coordMem + shard1Mem + shard2Mem;

        LiveQueryRecord record = new LiveQueryRecord(
            "query1",
            "running",
            1L,
            null,
            100L,
            totalCpu,
            totalMem,
            new TaskDetails(coordinator, "running"),
            List.of(new TaskDetails(shard1, "running"), new TaskDetails(shard2, "running"))
        );

        assertEquals(1800L, record.getTotalCpu());
        assertEquals(3800L, record.getTotalMemory());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"total_cpu_nanos\":1800"));
        assertTrue(json.contains("\"total_memory_bytes\":3800"));
    }
}
