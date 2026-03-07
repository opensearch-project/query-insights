/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.recommendations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.recommendations.Recommendation;
import org.opensearch.plugin.insights.rules.model.recommendations.RecommendationType;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link AnalyzeQueryResponse}
 */
public class AnalyzeQueryResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws IOException {
        List<Recommendation> recs = Arrays.asList(createRecommendation("rule-1", "Title 1"), createRecommendation("rule-2", "Title 2"));
        AnalyzeQueryResponse original = new AnalyzeQueryResponse(recs);
        AnalyzeQueryResponse deserialized = roundTrip(original);

        assertEquals(original.getRecommendations().size(), deserialized.getRecommendations().size());
        for (int i = 0; i < original.getRecommendations().size(); i++) {
            assertEquals(original.getRecommendations().get(i), deserialized.getRecommendations().get(i));
        }
    }

    public void testSerializationEmpty() throws IOException {
        AnalyzeQueryResponse original = new AnalyzeQueryResponse(Collections.emptyList());
        AnalyzeQueryResponse deserialized = roundTrip(original);

        assertTrue(deserialized.getRecommendations().isEmpty());
    }

    public void testSerializationWithNull() throws IOException {
        List<Recommendation> nullList = null;
        AnalyzeQueryResponse original = new AnalyzeQueryResponse(nullList);
        AnalyzeQueryResponse deserialized = roundTrip(original);

        assertTrue(deserialized.getRecommendations().isEmpty());
    }

    public void testToXContent() throws IOException {
        List<Recommendation> recs = List.of(createRecommendation("test-rule", "Test Title"));
        AnalyzeQueryResponse response = new AnalyzeQueryResponse(recs);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"count\":1"));
        assertTrue(json.contains("\"recommendations\""));
        assertTrue(json.contains("\"rule_id\":\"test-rule\""));
        assertTrue(json.contains("\"title\":\"Test Title\""));
    }

    public void testToXContentEmpty() throws IOException {
        AnalyzeQueryResponse response = new AnalyzeQueryResponse(Collections.emptyList());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"count\":0"));
        assertTrue(json.contains("\"recommendations\":[]"));
    }

    public void testGetRecommendations() {
        List<Recommendation> recs = new ArrayList<>();
        recs.add(createRecommendation("r1", "T1"));
        AnalyzeQueryResponse response = new AnalyzeQueryResponse(recs);

        assertEquals(1, response.getRecommendations().size());
        assertEquals("r1", response.getRecommendations().get(0).getRuleId());
    }

    private Recommendation createRecommendation(String ruleId, String title) {
        return Recommendation.builder()
            .ruleId(ruleId)
            .title(title)
            .description("Test description for " + ruleId)
            .type(RecommendationType.QUERY_REWRITE)
            .build();
    }

    private AnalyzeQueryResponse roundTrip(AnalyzeQueryResponse response) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new AnalyzeQueryResponse(in);
            }
        }
    }
}
