/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import static java.util.Collections.emptyMap;

import java.io.IOException;
import java.util.HashMap;
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
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the {@link LiveQueriesResponse} class.
 */
public class LiveQueriesResponseTests extends OpenSearchTestCase {

    private DiscoveryNode node1;

    private List<SearchQueryRecord> createLiveQueriesList(int count, long baseLatency) {
        return IntStream.range(0, count).mapToObj(i -> {
            Map<MetricType, Measurement> measurements = new HashMap<>();
            measurements.put(MetricType.LATENCY, new Measurement(baseLatency + i * 100));
            measurements.put(MetricType.CPU, new Measurement(randomLongBetween(10, 1000)));
            measurements.put(MetricType.MEMORY, new Measurement(randomLongBetween(1024, 10240)));
            Map<Attribute, Object> attributes = new HashMap<>();
            if (randomBoolean()) {
                attributes.put(Attribute.DESCRIPTION, "desc_" + baseLatency + "_" + i);
            }
            return new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, "query_" + baseLatency + "_" + i);
        }).collect(Collectors.toList());
    }

    public void testSerialization() throws IOException {
        node1 = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            java.util.Collections.emptyMap(),
            java.util.Collections.emptySet(),
            org.opensearch.test.VersionUtils.randomVersion(random())
        );
        List<SearchQueryRecord> queries = createLiveQueriesList(3, 1000);
        LiveQueriesResponse originalResponse = new LiveQueriesResponse(
            new ClusterName("test"),
            List.of(new LiveQueriesNodeResponse(node1, queries)),
            List.of()
        );
        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesResponse deserializedResponse = new LiveQueriesResponse(in);

        assertEquals(originalResponse.getLiveQueries().size(), deserializedResponse.getLiveQueries().size());
        assertEquals(originalResponse.getLiveQueries(), deserializedResponse.getLiveQueries());
    }

    public void testToXContent() throws IOException {
        node1 = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            java.util.Collections.emptyMap(),
            java.util.Collections.emptySet(),
            org.opensearch.test.VersionUtils.randomVersion(random())
        );
        Map<MetricType, Measurement> measurements = Map.of(
            MetricType.LATENCY,
            new Measurement(10L),
            MetricType.CPU,
            new Measurement(20L),
            MetricType.MEMORY,
            new Measurement(30L)
        );
        SearchQueryRecord rec1 = new SearchQueryRecord(1L, measurements, emptyMap(), "id1");
        SearchQueryRecord rec2 = new SearchQueryRecord(2L, measurements, emptyMap(), "id2");
        SearchQueryRecord rec3 = new SearchQueryRecord(3L, measurements, emptyMap(), "id3");
        List<SearchQueryRecord> records = List.of(rec2, rec1, rec3);
        LiveQueriesResponse response = new LiveQueriesResponse(
            new ClusterName("test"),
            List.of(new LiveQueriesNodeResponse(node1, records)),
            List.of()
        );
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
