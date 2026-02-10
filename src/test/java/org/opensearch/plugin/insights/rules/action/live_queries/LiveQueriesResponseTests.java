/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.LiveQueryRecord;
import org.opensearch.plugin.insights.rules.model.TaskRecord;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the {@link LiveQueriesResponse} class.
 */
public class LiveQueriesResponseTests extends OpenSearchTestCase {

    private DiscoveryNode node1;

    private List<LiveQueryRecord> createLiveQueriesList(int count, long baseLatency) {
        return IntStream.range(0, count).mapToObj(i -> {
            TaskRecord coordTask = new TaskRecord(
                "task_" + i,
                null,
                "node1",
                "RUNNING",
                "indices:data/read/search",
                System.currentTimeMillis(),
                baseLatency + i * 100,
                1000L,
                2000L,
                "test description",
                "wlm_group_1"
            );
            return new LiveQueryRecord(
                "query_" + baseLatency + "_" + i,
                "RUNNING",
                System.currentTimeMillis(),
                "wlm_group_1",
                baseLatency + i * 100,
                1000L,
                2000L,
                coordTask,
                List.of()
            );
        }).collect(Collectors.toList());
    }

    public void testSerialization() throws IOException {
        node1 = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            java.util.Collections.emptyMap(),
            java.util.Collections.emptySet(),
            org.opensearch.Version.CURRENT
        );
        List<LiveQueryRecord> queries = createLiveQueriesList(3, 1000);
        LiveQueriesResponse originalResponse = new LiveQueriesResponse(new ClusterName("test"), queries, List.of());
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(org.opensearch.Version.CURRENT);
        originalResponse.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(org.opensearch.Version.CURRENT);
        LiveQueriesResponse deserializedResponse = new LiveQueriesResponse(in);

        assertEquals(originalResponse.getLiveQueries().size(), deserializedResponse.getLiveQueries().size());
    }

    public void testToXContent() throws IOException {
        node1 = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            java.util.Collections.emptyMap(),
            java.util.Collections.emptySet(),
            org.opensearch.Version.CURRENT
        );
        TaskRecord task1 = new TaskRecord("t1", null, "node1", "RUNNING", "indices:data/read/search", 1L, 10L, 100L, 200L, "desc1", null);
        TaskRecord task2 = new TaskRecord("t2", null, "node1", "RUNNING", "indices:data/read/search", 2L, 20L, 100L, 200L, "desc2", null);
        TaskRecord task3 = new TaskRecord("t3", null, "node1", "RUNNING", "indices:data/read/search", 3L, 30L, 100L, 200L, "desc3", null);
        LiveQueryRecord rec1 = new LiveQueryRecord("id1", "RUNNING", 1L, null, 10L, 100L, 200L, task1, List.of());
        LiveQueryRecord rec2 = new LiveQueryRecord("id2", "RUNNING", 2L, null, 20L, 100L, 200L, task2, List.of());
        LiveQueryRecord rec3 = new LiveQueryRecord("id3", "RUNNING", 3L, null, 30L, 100L, 200L, task3, List.of());
        List<LiveQueryRecord> records = List.of(rec2, rec1, rec3);
        LiveQueriesResponse response = new LiveQueriesResponse(new ClusterName("test"), records, List.of());
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
        LiveQueriesResponse response = new LiveQueriesResponse(new ClusterName("test"), List.of(), List.of());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertEquals("{\"live_queries\":[]}", json);
    }
}
