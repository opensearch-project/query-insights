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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
 * Unit tests for the {@link LiveQueries} class.
 */
public class LiveQueriesTests extends OpenSearchTestCase {
    private final DiscoveryNode discoveryNode = new DiscoveryNode(
        "node_for_live_queries_test",
        buildNewFakeTransportAddress(),
        emptyMap(),
        emptySet(),
        VersionUtils.randomVersion(random())
    );

    private List<SearchQueryRecord> createLiveQueriesList(int count) {
        return IntStream.range(0, count).mapToObj(i -> {
            long timestamp = System.currentTimeMillis();
            Map<MetricType, Measurement> measurements = new HashMap<>();
            measurements.put(MetricType.LATENCY, new Measurement(randomLongBetween(100, 5000)));
            measurements.put(MetricType.CPU, new Measurement(randomLongBetween(10, 1000)));
            measurements.put(MetricType.MEMORY, new Measurement(randomLongBetween(1024, 10240)));

            Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.NODE_ID, discoveryNode.getId());

            String id = "test_query_" + i + "_" + UUID.randomUUID().toString().substring(0, 8);
            return new SearchQueryRecord(timestamp, measurements, attributes, id);
        }).collect(Collectors.toList());
    }

    public void testConstructorWithNodeAndStats() {
        List<SearchQueryRecord> liveQueriesList = createLiveQueriesList(3);
        LiveQueries response = new LiveQueries(discoveryNode, liveQueriesList);
        assertEquals(discoveryNode, response.getNode());
        assertEquals(liveQueriesList, response.getLiveQueries());
    }

    public void testSerialization() throws IOException {
        List<SearchQueryRecord> liveQueriesList = createLiveQueriesList(randomIntBetween(1, 5));
        LiveQueries originalResponse = new LiveQueries(discoveryNode, liveQueriesList);
        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueries response = new LiveQueries(in);
        assertNotNull(response);
        assertEquals(originalResponse.getNode(), response.getNode());
        assertEquals(originalResponse.getLiveQueries().size(), response.getLiveQueries().size());
        for (int i = 0; i < originalResponse.getLiveQueries().size(); i++) {
            assertEquals(originalResponse.getLiveQueries().get(i), response.getLiveQueries().get(i));
        }
    }

    public void testToXContent() throws IOException {
        List<SearchQueryRecord> liveQueriesList = createLiveQueriesList(2);
        LiveQueries response = new LiveQueries(discoveryNode, liveQueriesList);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String jsonString = builder.toString();
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("\"node_id\":\"" + discoveryNode.getId() + "\""));
        assertTrue(jsonString.contains("\"node_name\":\"" + discoveryNode.getName() + "\""));
        assertTrue(jsonString.contains("\"live_queries\":["));
        // Verify individual records are present (simplified check)
        assertTrue(jsonString.contains("\"id\":\"" + liveQueriesList.get(0).getId() + "\""));
        assertTrue(jsonString.contains("\"id\":\"" + liveQueriesList.get(1).getId() + "\""));
    }

    public void testEqualsAndHashCode() {
        List<SearchQueryRecord> liveQueriesList1 = createLiveQueriesList(3);
        List<SearchQueryRecord> liveQueriesList2 = createLiveQueriesList(3);
        List<SearchQueryRecord> liveQueriesList3 = createLiveQueriesList(2); // Different size

        LiveQueries response1 = new LiveQueries(discoveryNode, liveQueriesList1);
        LiveQueries response1Dup = new LiveQueries(discoveryNode, liveQueriesList1); // Same data
        LiveQueries response2 = new LiveQueries(discoveryNode, liveQueriesList2); // Different list content
        LiveQueries response3 = new LiveQueries(discoveryNode, liveQueriesList3); // Different list size

        DiscoveryNode differentNode = new DiscoveryNode(
            "different_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        LiveQueries response4 = new LiveQueries(differentNode, liveQueriesList1); // Different node

        assertEquals(response1.getLiveQueries(), response1Dup.getLiveQueries());
        assertEquals(response1.getLiveQueries().hashCode(), response1Dup.getLiveQueries().hashCode());

        if (!liveQueriesList1.equals(liveQueriesList2)) {
            assertNotEquals(response1, response2);
        }

        assertNotEquals(response1, response3);
        assertNotEquals(response1, response4);
        assertNotEquals(response1, null);
        assertNotEquals(response1, new Object());
    }

    public void testGetLiveQueries() {
        List<SearchQueryRecord> liveQueriesList = createLiveQueriesList(1);
        LiveQueries response = new LiveQueries(discoveryNode, liveQueriesList);
        assertEquals(liveQueriesList, response.getLiveQueries());
    }
}
