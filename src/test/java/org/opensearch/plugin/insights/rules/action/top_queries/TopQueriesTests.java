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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
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
 * Tests for {@link TopQueries}.
 */
public class TopQueriesTests extends OpenSearchTestCase {

    public void testTopQueries() throws IOException {
        TopQueries topQueries = QueryInsightsTestUtils.createRandomTopQueries();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            topQueries.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TopQueries readTopQueries = new TopQueries(in);
                assertTrue(
                    QueryInsightsTestUtils.checkRecordsEquals(topQueries.getTopQueriesRecord(), readTopQueries.getTopQueriesRecord())
                );
            }
        }
    }

    public void testTopQueriesWithRecommendationsRoundTrip() throws IOException {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        SearchQueryRecord record = new SearchQueryRecord(
            1L,
            Map.of(MetricType.LATENCY, new Measurement(5.0D, AggregationType.AVERAGE)),
            Map.of(Attribute.NODE_ID, node.getId()),
            "rec_test_id"
        );
        Recommendation rec = Recommendation.builder()
            .ruleId("test-rule")
            .title("Test Title")
            .description("Test Description")
            .type(RecommendationType.QUERY_REWRITE)
            .confidence(0.9)
            .build();

        Map<String, List<Recommendation>> recsMap = new HashMap<>();
        recsMap.put(record.getId(), List.of(rec));

        TopQueries topQueries = new TopQueries(node, List.of(record), recsMap);
        assertEquals(1, topQueries.getRecommendations().size());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            topQueries.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TopQueries deserialized = new TopQueries(in);
                assertTrue(QueryInsightsTestUtils.checkRecordsEquals(topQueries.getTopQueriesRecord(), deserialized.getTopQueriesRecord()));
                assertEquals(1, deserialized.getRecommendations().size());
                List<Recommendation> deserializedRecs = deserialized.getRecommendations().get("rec_test_id");
                assertNotNull(deserializedRecs);
                assertEquals(1, deserializedRecs.size());
                assertEquals(rec, deserializedRecs.get(0));
            }
        }
    }

    public void testTopQueriesWithEmptyRecommendationsRoundTrip() throws IOException {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        SearchQueryRecord record = new SearchQueryRecord(
            1L,
            Map.of(MetricType.LATENCY, new Measurement(5.0D, AggregationType.AVERAGE)),
            Map.of(Attribute.NODE_ID, node.getId()),
            "test_id"
        );
        TopQueries topQueries = new TopQueries(node, List.of(record));
        assertTrue(topQueries.getRecommendations().isEmpty());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            topQueries.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TopQueries deserialized = new TopQueries(in);
                assertTrue(deserialized.getRecommendations().isEmpty());
            }
        }
    }

    public void testTopQueriesNullRecommendationsDefaultsToEmpty() {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        TopQueries topQueries = new TopQueries(node, Collections.emptyList(), null);
        assertNotNull(topQueries.getRecommendations());
        assertTrue(topQueries.getRecommendations().isEmpty());
    }
}
