/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

/**
 * Unit tests for the {@link LiveQueriesResponse} class.
 */
public class LiveQueriesResponseTests extends OpenSearchTestCase {
    private final DiscoveryNode node1 = new DiscoveryNode(
        "node1",
        buildNewFakeTransportAddress(),
        emptyMap(),
        emptySet(),
        VersionUtils.randomVersion(random())
    );
    private final DiscoveryNode node2 = new DiscoveryNode(
        "node2",
        buildNewFakeTransportAddress(),
        emptyMap(),
        emptySet(),
        VersionUtils.randomVersion(random())
    );

    private List<SearchQueryRecord> createLiveQueriesList(int count, long baseLatency) {
        return IntStream.range(0, count).mapToObj(i -> {
            Map<MetricType, Measurement> measurements = new HashMap<>();
            // Ensure unique latencies for sorting tests
            measurements.put(MetricType.LATENCY, new Measurement(baseLatency + i * 100));
            measurements.put(MetricType.CPU, new Measurement(randomLongBetween(10, 1000)));
            measurements.put(MetricType.MEMORY, new Measurement(randomLongBetween(1024, 10240)));

            Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.NODE_ID, "node" + (i % 2 + 1)); // Assign to node1 or node2
            return new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, "query_" + baseLatency + "_" + i);
        }).collect(Collectors.toList());
    }

    public void testSerialization() throws IOException {
        ClusterName clusterName = new ClusterName("test-cluster");
        List<SearchQueryRecord> queries1 = createLiveQueriesList(2, 1000);
        List<SearchQueryRecord> queries2 = createLiveQueriesList(1, 2000);
        List<LiveQueries> nodes = List.of(new LiveQueries(node1, queries1), new LiveQueries(node2, queries2));
        List<FailedNodeException> failures = Collections.emptyList();
        boolean verbose = randomBoolean();

        LiveQueriesResponse originalResponse = new LiveQueriesResponse(clusterName, nodes, failures, verbose);
        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesResponse deserializedResponse = new LiveQueriesResponse(in);

        assertEquals(originalResponse.getClusterName(), deserializedResponse.getClusterName());
        assertEquals(originalResponse.getNodes().size(), deserializedResponse.getNodes().size());
        assertEquals(originalResponse.failures().size(), deserializedResponse.failures().size());
        for (int i = 0; i < originalResponse.getNodes().size(); i++) {
            assertEquals(originalResponse.getNodes().get(i).getLiveQueries(), deserializedResponse.getNodes().get(i).getLiveQueries());
        }
    }

    public void testToXContentIncludesAllQueries() throws IOException {
        ClusterName clusterName = new ClusterName("test-cluster");
        List<SearchQueryRecord> queries1 = createLiveQueriesList(2, 1000); // Latencies 1000, 1100
        List<SearchQueryRecord> queries2 = createLiveQueriesList(1, 500); // Latency 500
        List<LiveQueries> nodes = List.of(new LiveQueries(node1, queries1), new LiveQueries(node2, queries2));
        List<FailedNodeException> failures = Collections.emptyList();
        boolean verbose = true;

        LiveQueriesResponse response = new LiveQueriesResponse(clusterName, nodes, failures, verbose);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String jsonString = builder.toString();

        // Verify root structure
        assertTrue(jsonString.startsWith("{\"live_queries\":["));
        assertTrue(jsonString.endsWith("]}"));

        // Verify all queries are present (check by id)
        assertTrue(jsonString.contains("\"id\":\"query_1000_0\""));
        assertTrue(jsonString.contains("\"id\":\"query_1000_1\""));
        assertTrue(jsonString.contains("\"id\":\"query_500_0\""));
    }

    public void testToXContentSortsByLatencyDescending() throws IOException {
        ClusterName clusterName = new ClusterName("test-cluster");
        // Create queries with specific latencies for sorting test
        List<SearchQueryRecord> queries1 = createLiveQueriesList(1, 1000); // Latency 1000
        List<SearchQueryRecord> queries2 = createLiveQueriesList(1, 500); // Latency 500
        List<SearchQueryRecord> queries3 = createLiveQueriesList(1, 1500); // Latency 1500

        List<LiveQueries> nodes = List.of(
            new LiveQueries(node1, queries1),
            new LiveQueries(node2, queries2),
            new LiveQueries(node1, queries3)
        );

        List<FailedNodeException> failures = Collections.emptyList();
        boolean verbose = true;

        LiveQueriesResponse response = new LiveQueriesResponse(clusterName, nodes, failures, verbose);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String jsonString = builder.toString();

        // Skip parsing and use string validation to verify sorting
        // First verify that all three queries are present
        assertTrue(jsonString.contains("\"id\":\"query_1500_0\""));
        assertTrue(jsonString.contains("\"id\":\"query_1000_0\""));
        assertTrue(jsonString.contains("\"id\":\"query_500_0\""));

        // Verify order by checking position in string (the higher latency 1500 should appear before lower latency 500)
        int pos1500 = jsonString.indexOf("\"id\":\"query_1500_0\"");
        int pos1000 = jsonString.indexOf("\"id\":\"query_1000_0\"");
        int pos500 = jsonString.indexOf("\"id\":\"query_500_0\"");

        assertTrue("Highest latency (1500) should come before medium latency (1000)", pos1500 < pos1000);
        assertTrue("Medium latency (1000) should come before lowest latency (500)", pos1000 < pos500);
    }

    public void testGetNodes() {
        ClusterName clusterName = new ClusterName("test-cluster");
        List<SearchQueryRecord> queries = createLiveQueriesList(3, 1000);
        List<LiveQueries> nodes = List.of(new LiveQueries(node1, queries));
        List<FailedNodeException> failures = Collections.emptyList();

        LiveQueriesResponse response = new LiveQueriesResponse(clusterName, nodes, failures, true);

        assertEquals(nodes, response.getNodes());
        assertEquals(1, response.getNodes().size());
        assertEquals(node1, response.getNodes().get(0).getNode());
        assertEquals(queries, response.getNodes().get(0).getLiveQueries());
    }
}
