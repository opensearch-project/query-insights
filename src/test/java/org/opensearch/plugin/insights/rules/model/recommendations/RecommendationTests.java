/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.recommendations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link Recommendation}
 */
public class RecommendationTests extends OpenSearchTestCase {

    public void testBuilder() {
        Action action = createTestAction();
        ImpactVector impact = createTestImpact();
        Map<String, Object> metadata = createTestMetadata();

        Recommendation rec = Recommendation.builder()
            .id("rec-1")
            .ruleId("term-on-text-field")
            .title("Term query on text field")
            .description("Term queries on text fields return incorrect results")
            .type(RecommendationType.CORRECTNESS)
            .action(action)
            .impact(impact)
            .confidence(0.98)
            .metadata(metadata)
            .build();

        assertEquals("rec-1", rec.getId());
        assertEquals("term-on-text-field", rec.getRuleId());
        assertEquals("Term query on text field", rec.getTitle());
        assertEquals("Term queries on text fields return incorrect results", rec.getDescription());
        assertEquals(RecommendationType.CORRECTNESS, rec.getType());
        assertEquals(action, rec.getAction());
        assertEquals(impact, rec.getImpact());
        assertEquals(0.98, rec.getConfidence(), 0.001);
        assertEquals(metadata.get("field"), rec.getMetadata().get("field"));
    }

    public void testBuilderRequiredFields() {
        expectThrows(
            NullPointerException.class,
            () -> Recommendation.builder().title("t").description("d").type(RecommendationType.CORRECTNESS).build()
        );

        expectThrows(
            NullPointerException.class,
            () -> Recommendation.builder().ruleId("r").description("d").type(RecommendationType.CORRECTNESS).build()
        );

        expectThrows(
            NullPointerException.class,
            () -> Recommendation.builder().ruleId("r").title("t").type(RecommendationType.CORRECTNESS).build()
        );

        expectThrows(NullPointerException.class, () -> Recommendation.builder().ruleId("r").title("t").description("d").build());
    }

    public void testBuilderMinimal() {
        Recommendation rec = Recommendation.builder()
            .ruleId("test-rule")
            .title("Test")
            .description("Test description")
            .type(RecommendationType.QUERY_REWRITE)
            .build();

        assertNull(rec.getId());
        assertNull(rec.getAction());
        assertNull(rec.getImpact());
        assertEquals(1.0, rec.getConfidence(), 0.001);
        assertTrue(rec.getMetadata().isEmpty());
    }

    public void testSerialization() throws IOException {
        Recommendation original = createTestRecommendation();
        Recommendation deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertEquals(original.getId(), deserialized.getId());
        assertEquals(original.getRuleId(), deserialized.getRuleId());
        assertEquals(original.getTitle(), deserialized.getTitle());
        assertEquals(original.getDescription(), deserialized.getDescription());
        assertEquals(original.getType(), deserialized.getType());
        assertEquals(original.getAction(), deserialized.getAction());
        assertEquals(original.getImpact(), deserialized.getImpact());
        assertEquals(original.getConfidence(), deserialized.getConfidence(), 0.001);
        assertEquals(original.getMetadata(), deserialized.getMetadata());
    }

    public void testSerializationMinimal() throws IOException {
        Recommendation original = Recommendation.builder()
            .ruleId("test-rule")
            .title("Test")
            .description("Test description")
            .type(RecommendationType.QUERY_REWRITE)
            .build();
        Recommendation deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertNull(deserialized.getId());
        assertNull(deserialized.getAction());
        assertNull(deserialized.getImpact());
        assertTrue(deserialized.getMetadata().isEmpty());
    }

    public void testEquals() {
        Recommendation rec1 = createTestRecommendation();
        Recommendation rec2 = createTestRecommendation();

        assertEquals(rec1, rec2);
        assertEquals(rec1, rec1);
        assertNotEquals(rec1, null);
        assertNotEquals(rec1, "not a recommendation");
    }

    public void testNotEquals() {
        Recommendation base = createTestRecommendation();

        Recommendation differentId = Recommendation.builder()
            .id("different-id")
            .ruleId(base.getRuleId())
            .title(base.getTitle())
            .description(base.getDescription())
            .type(base.getType())
            .action(base.getAction())
            .impact(base.getImpact())
            .confidence(base.getConfidence())
            .metadata(new HashMap<>(base.getMetadata()))
            .build();
        assertNotEquals(base, differentId);

        Recommendation differentType = Recommendation.builder()
            .id(base.getId())
            .ruleId(base.getRuleId())
            .title(base.getTitle())
            .description(base.getDescription())
            .type(RecommendationType.INDEX_CONFIG)
            .action(base.getAction())
            .impact(base.getImpact())
            .confidence(base.getConfidence())
            .metadata(new HashMap<>(base.getMetadata()))
            .build();
        assertNotEquals(base, differentType);
    }

    public void testHashCode() {
        Recommendation rec1 = createTestRecommendation();
        Recommendation rec2 = createTestRecommendation();

        assertEquals(rec1.hashCode(), rec2.hashCode());
    }

    public void testToXContent() throws IOException {
        Recommendation rec = createTestRecommendation();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        rec.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"id\":\"rec-1\""));
        assertTrue(json.contains("\"rule_id\":\"term-on-text-field\""));
        assertTrue(json.contains("\"title\":\"Term query on text field\""));
        assertTrue(json.contains("\"description\":"));
        assertTrue(json.contains("\"type\":\"correctness\""));
        assertTrue(json.contains("\"action\""));
        assertTrue(json.contains("\"impact\""));
        assertTrue(json.contains("\"confidence\":0.98"));
        assertTrue(json.contains("\"metadata\""));
    }

    public void testToXContentMinimal() throws IOException {
        Recommendation rec = Recommendation.builder()
            .ruleId("test-rule")
            .title("Test")
            .description("Test description")
            .type(RecommendationType.QUERY_REWRITE)
            .build();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        rec.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"rule_id\":\"test-rule\""));
        assertTrue(json.contains("\"title\":\"Test\""));
        assertTrue(json.contains("\"type\":\"query_rewrite\""));
        assertFalse(json.contains("\"id\""));
        assertFalse(json.contains("\"action\""));
        assertFalse(json.contains("\"impact\""));
        assertFalse(json.contains("\"metadata\""));
    }

    public void testMetadataIsUnmodifiable() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("key", "value");

        Recommendation rec = Recommendation.builder()
            .ruleId("test-rule")
            .title("Test")
            .description("Test description")
            .type(RecommendationType.QUERY_REWRITE)
            .metadata(metadata)
            .build();

        expectThrows(UnsupportedOperationException.class, () -> rec.getMetadata().put("new_key", "new_value"));
    }

    private Recommendation createTestRecommendation() {
        return Recommendation.builder()
            .id("rec-1")
            .ruleId("term-on-text-field")
            .title("Term query on text field")
            .description("Term queries on text fields return incorrect results")
            .type(RecommendationType.CORRECTNESS)
            .action(createTestAction())
            .impact(createTestImpact())
            .confidence(0.98)
            .metadata(createTestMetadata())
            .build();
    }

    private Action createTestAction() {
        return Action.builder()
            .name("use_keyword_subfield")
            .hint("Use title.keyword instead of title")
            .documentationUrl("https://opensearch.org/docs/latest/query-dsl/term/term/")
            .suggestedQuery("{\"query\":{\"term\":{\"title.keyword\":\"OpenSearch\"}}}")
            .build();
    }

    private ImpactVector createTestImpact() {
        return ImpactVector.builder()
            .correctness(new Impact(Direction.INCREASE))
            .latency(new Impact(Direction.NEUTRAL))
            .cpu(new Impact(Direction.NEUTRAL))
            .memory(new Impact(Direction.NEUTRAL))
            .estimatedImprovement("Returns correct results")
            .build();
    }

    private Map<String, Object> createTestMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("field", "title");
        metadata.put("field_type", "text");
        metadata.put("has_keyword_subfield", true);
        return metadata;
    }

    private Recommendation roundTrip(Recommendation rec) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            rec.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new Recommendation(in);
            }
        }
    }
}
