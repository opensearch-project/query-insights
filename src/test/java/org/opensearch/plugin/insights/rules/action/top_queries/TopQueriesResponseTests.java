/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.top_queries;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.recommendations.Recommendation;
import org.opensearch.plugin.insights.rules.model.recommendations.RecommendationType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

/**
 * Granular tests for the {@link TopQueriesResponse} class.
 */
public class TopQueriesResponseTests extends OpenSearchTestCase {

    /**
     * Check serialization and deserialization
     */
    public void testSerialize() throws Exception {
        TopQueries topQueries = QueryInsightsTestUtils.createRandomTopQueries();
        ClusterName clusterName = new ClusterName("test-cluster");
        TopQueriesResponse response = new TopQueriesResponse(clusterName, List.of(topQueries), new ArrayList<>(), MetricType.LATENCY);
        TopQueriesResponse deserializedResponse = roundTripResponse(response);
        assertEquals(response.toString(), deserializedResponse.toString());
    }

    public void testToXContent() throws IOException {
        String id = "sample_id";

        char[] expectedXContent = ("{"
            + "\"top_queries\":[{"
            + "\"timestamp\":1706574180000,"
            + "\"node_id\":\"node_for_top_queries_test\","
            + "\"phase_latency_map\":{"
            + "\"expand\":1,"
            + "\"query\":10,"
            + "\"fetch\":1"
            + "},"
            + "\"task_resource_usages\":[{"
            + "\"action\":\"action\","
            + "\"taskId\":2,"
            + "\"parentTaskId\":1,"
            + "\"nodeId\":\"id\","
            + "\"taskResourceUsage\":{"
            + "\"cpu_time_in_nanos\":1000,"
            + "\"memory_in_bytes\":2000"
            + "}"
            + "},{"
            + "\"action\":\"action2\","
            + "\"taskId\":3,"
            + "\"parentTaskId\":1,"
            + "\"nodeId\":\"id2\","
            + "\"taskResourceUsage\":{"
            + "\"cpu_time_in_nanos\":2000,"
            + "\"memory_in_bytes\":1000"
            + "}"
            + "}],"
            + "\"search_type\":\"query_then_fetch\","
            + "\"failed\":false,"
            + "\"username\":\"testuser\","
            + "\"user_roles\":[\"admin\",\"user\"],"
            + "\"backend_roles\":[\"role1\",\"role2\"],"
            + "\"measurements\":{"
            + "\"latency\":{"
            + "\"number\":1,"
            + "\"count\":1,"
            + "\"aggregationType\":\"NONE\""
            + "}"
            + "},"
            + "\"id\":\""
            + id
            + "\""
            + "}]"
            + "}").toCharArray();

        TopQueries topQueries = QueryInsightsTestUtils.createFixedTopQueries(id);
        ClusterName clusterName = new ClusterName("test-cluster");
        TopQueriesResponse response = new TopQueriesResponse(clusterName, List.of(topQueries), new ArrayList<>(), MetricType.LATENCY);

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        char[] xContent = BytesReference.bytes(response.toXContent(builder, ToXContent.EMPTY_PARAMS)).utf8ToString().toCharArray();

        Arrays.sort(expectedXContent);
        Arrays.sort(xContent);

        assertEquals(Arrays.hashCode(expectedXContent), Arrays.hashCode(xContent));
    }

    public void testToXContentWithRecommendations() throws IOException {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        String recordId = "rec_record_id";
        SearchQueryRecord record = new SearchQueryRecord(
            1706574180000L,
            Map.of(MetricType.LATENCY, new Measurement(1L)),
            Map.of(Attribute.NODE_ID, node.getId()),
            recordId
        );
        Recommendation rec = Recommendation.builder()
            .ruleId("test-rule")
            .title("Test Recommendation")
            .description("Test Description")
            .type(RecommendationType.QUERY_REWRITE)
            .confidence(0.95)
            .build();

        Map<String, List<Recommendation>> recsMap = new HashMap<>();
        recsMap.put(recordId, List.of(rec));
        TopQueries topQueries = new TopQueries(node, List.of(record), recsMap);

        ClusterName clusterName = new ClusterName("test-cluster");
        TopQueriesResponse response = new TopQueriesResponse(clusterName, List.of(topQueries), new ArrayList<>(), MetricType.LATENCY);

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        String json = BytesReference.bytes(response.toXContent(builder, ToXContent.EMPTY_PARAMS)).utf8ToString();

        assertTrue(json.contains("\"recommendations\""));
        assertTrue(json.contains("\"rule_id\":\"test-rule\""));
        assertTrue(json.contains("\"title\":\"Test Recommendation\""));
        assertTrue(json.contains("\"confidence\":0.95"));
    }

    public void testToXContentWithoutRecommendations() throws IOException {
        String id = "no_rec_id";
        TopQueries topQueries = QueryInsightsTestUtils.createFixedTopQueries(id);
        ClusterName clusterName = new ClusterName("test-cluster");
        TopQueriesResponse response = new TopQueriesResponse(clusterName, List.of(topQueries), new ArrayList<>(), MetricType.LATENCY);

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        String json = BytesReference.bytes(response.toXContent(builder, ToXContent.EMPTY_PARAMS)).utf8ToString();

        assertFalse(json.contains("\"recommendations\""));
    }

    public void testToXContentWithEmptyRecommendationsArray() throws IOException {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        String recordId = "empty_rec_record_id";
        SearchQueryRecord record = new SearchQueryRecord(
            1706574180000L,
            Map.of(MetricType.LATENCY, new Measurement(1L)),
            Map.of(Attribute.NODE_ID, node.getId()),
            recordId
        );

        // Recommendations requested but no rules matched — empty list in map
        Map<String, List<Recommendation>> recsMap = new HashMap<>();
        recsMap.put(recordId, List.of());
        TopQueries topQueries = new TopQueries(node, List.of(record), recsMap);

        ClusterName clusterName = new ClusterName("test-cluster");
        TopQueriesResponse response = new TopQueriesResponse(clusterName, List.of(topQueries), new ArrayList<>(), MetricType.LATENCY);

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        String json = BytesReference.bytes(response.toXContent(builder, ToXContent.EMPTY_PARAMS)).utf8ToString();

        assertTrue(
            "Should contain empty recommendations array when requested but no rules matched",
            json.contains("\"recommendations\":[]")
        );
    }

    public void testToXContentMergesRecommendationsFromMultipleNodes() throws IOException {
        DiscoveryNode node1 = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        DiscoveryNode node2 = new DiscoveryNode(
            "node2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );

        SearchQueryRecord record1 = new SearchQueryRecord(
            2L,
            Map.of(MetricType.LATENCY, new Measurement(10.0D, AggregationType.AVERAGE)),
            Map.of(Attribute.NODE_ID, node1.getId()),
            "id_node1"
        );
        SearchQueryRecord record2 = new SearchQueryRecord(
            1L,
            Map.of(MetricType.LATENCY, new Measurement(5.0D, AggregationType.AVERAGE)),
            Map.of(Attribute.NODE_ID, node2.getId()),
            "id_node2"
        );

        Recommendation rec1 = Recommendation.builder()
            .ruleId("rule-a")
            .title("Rec A")
            .description("From node1")
            .type(RecommendationType.QUERY_REWRITE)
            .confidence(0.8)
            .build();
        Recommendation rec2 = Recommendation.builder()
            .ruleId("rule-b")
            .title("Rec B")
            .description("From node2")
            .type(RecommendationType.INDEX_CONFIG)
            .confidence(0.7)
            .build();

        Map<String, List<Recommendation>> recsMap1 = Map.of("id_node1", List.of(rec1));
        Map<String, List<Recommendation>> recsMap2 = Map.of("id_node2", List.of(rec2));

        TopQueries tq1 = new TopQueries(node1, List.of(record1), recsMap1);
        TopQueries tq2 = new TopQueries(node2, List.of(record2), recsMap2);

        ClusterName clusterName = new ClusterName("test-cluster");
        TopQueriesResponse response = new TopQueriesResponse(clusterName, List.of(tq1, tq2), new ArrayList<>(), MetricType.LATENCY);

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        String json = BytesReference.bytes(response.toXContent(builder, ToXContent.EMPTY_PARAMS)).utf8ToString();

        assertTrue(json.contains("\"rule_id\":\"rule-a\""));
        assertTrue(json.contains("\"rule_id\":\"rule-b\""));
    }

    public void testToXContentMergesRecommendationsForDuplicateRecordId() throws IOException {
        DiscoveryNode node1 = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        DiscoveryNode node2 = new DiscoveryNode(
            "node2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );

        // Same record ID on both nodes (e.g. grouped query)
        String sharedId = "shared_id";
        SearchQueryRecord record1 = new SearchQueryRecord(
            1L,
            Map.of(MetricType.LATENCY, new Measurement(10.0D, AggregationType.AVERAGE)),
            Map.of(Attribute.NODE_ID, node1.getId()),
            sharedId
        );
        SearchQueryRecord record2 = new SearchQueryRecord(
            1L,
            Map.of(MetricType.LATENCY, new Measurement(10.0D, AggregationType.AVERAGE)),
            Map.of(Attribute.NODE_ID, node2.getId()),
            sharedId
        );

        Recommendation recA = Recommendation.builder()
            .ruleId("rule-a")
            .title("Rec A")
            .description("From node1")
            .type(RecommendationType.QUERY_REWRITE)
            .confidence(0.8)
            .build();
        Recommendation recB = Recommendation.builder()
            .ruleId("rule-b")
            .title("Rec B")
            .description("From node2")
            .type(RecommendationType.INDEX_CONFIG)
            .confidence(0.7)
            .build();

        TopQueries tq1 = new TopQueries(node1, List.of(record1), Map.of(sharedId, List.of(recA)));
        TopQueries tq2 = new TopQueries(node2, List.of(record2), Map.of(sharedId, List.of(recB)));

        ClusterName clusterName = new ClusterName("test-cluster");
        TopQueriesResponse response = new TopQueriesResponse(clusterName, List.of(tq1, tq2), new ArrayList<>(), MetricType.LATENCY);

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        String json = BytesReference.bytes(response.toXContent(builder, ToXContent.EMPTY_PARAMS)).utf8ToString();

        // Both recommendations should be present (merged, not dropped)
        assertTrue(json.contains("\"rule_id\":\"rule-a\""));
        assertTrue(json.contains("\"rule_id\":\"rule-b\""));
    }

    /**
     * Serialize and deserialize a TopQueriesResponse.
     * @param response A response to serialize.
     * @return The deserialized, "round-tripped" response.
     */
    private static TopQueriesResponse roundTripResponse(TopQueriesResponse response) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new TopQueriesResponse(in);
            }
        }
    }
}
