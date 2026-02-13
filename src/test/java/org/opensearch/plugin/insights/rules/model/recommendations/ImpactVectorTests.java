/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.recommendations;

import java.io.IOException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link ImpactVector}
 */
public class ImpactVectorTests extends OpenSearchTestCase {

    public void testBuilder() {
        ImpactVector impact = ImpactVector.builder()
            .latency(Direction.DECREASE)
            .cpu(Direction.DECREASE)
            .memory(Direction.NEUTRAL)
            .correctness(Direction.INCREASE)
            .confidence(0.95)
            .estimatedImprovement("50% latency reduction")
            .build();

        assertEquals(Direction.DECREASE, impact.getLatency());
        assertEquals(Direction.DECREASE, impact.getCpu());
        assertEquals(Direction.NEUTRAL, impact.getMemory());
        assertEquals(Direction.INCREASE, impact.getCorrectness());
        assertEquals(0.95, impact.getConfidence(), 0.001);
        assertEquals("50% latency reduction", impact.getEstimatedImprovement());
    }

    public void testBuilderDefaults() {
        ImpactVector impact = ImpactVector.builder().build();

        assertEquals(Direction.NEUTRAL, impact.getLatency());
        assertEquals(Direction.NEUTRAL, impact.getCpu());
        assertEquals(Direction.NEUTRAL, impact.getMemory());
        assertEquals(Direction.NEUTRAL, impact.getCorrectness());
        assertEquals(1.0, impact.getConfidence(), 0.001);
        assertNull(impact.getEstimatedImprovement());
    }

    public void testSerialization() throws IOException {
        ImpactVector original = createTestImpactVector();
        ImpactVector deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertEquals(original.getLatency(), deserialized.getLatency());
        assertEquals(original.getCpu(), deserialized.getCpu());
        assertEquals(original.getMemory(), deserialized.getMemory());
        assertEquals(original.getCorrectness(), deserialized.getCorrectness());
        assertEquals(original.getConfidence(), deserialized.getConfidence(), 0.001);
        assertEquals(original.getEstimatedImprovement(), deserialized.getEstimatedImprovement());
    }

    public void testSerializationWithNullImprovement() throws IOException {
        ImpactVector original = ImpactVector.builder().latency(Direction.DECREASE).build();
        ImpactVector deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertNull(deserialized.getEstimatedImprovement());
    }

    public void testEquals() {
        ImpactVector impact1 = createTestImpactVector();
        ImpactVector impact2 = createTestImpactVector();

        assertEquals(impact1, impact2);
        assertEquals(impact1, impact1);
        assertNotEquals(impact1, null);
        assertNotEquals(impact1, "not an impact vector");
    }

    public void testNotEquals() {
        ImpactVector base = createTestImpactVector();

        assertNotEquals(
            base,
            ImpactVector.builder()
                .latency(Direction.INCREASE)
                .cpu(Direction.DECREASE)
                .memory(Direction.NEUTRAL)
                .correctness(Direction.INCREASE)
                .confidence(0.95)
                .estimatedImprovement("50% latency reduction")
                .build()
        );

        assertNotEquals(
            base,
            ImpactVector.builder()
                .latency(Direction.DECREASE)
                .cpu(Direction.DECREASE)
                .memory(Direction.NEUTRAL)
                .correctness(Direction.INCREASE)
                .confidence(0.5)
                .estimatedImprovement("50% latency reduction")
                .build()
        );

        assertNotEquals(
            base,
            ImpactVector.builder()
                .latency(Direction.DECREASE)
                .cpu(Direction.DECREASE)
                .memory(Direction.NEUTRAL)
                .correctness(Direction.INCREASE)
                .confidence(0.95)
                .estimatedImprovement("different")
                .build()
        );
    }

    public void testHashCode() {
        ImpactVector impact1 = createTestImpactVector();
        ImpactVector impact2 = createTestImpactVector();

        assertEquals(impact1.hashCode(), impact2.hashCode());
    }

    public void testToXContent() throws IOException {
        ImpactVector impact = createTestImpactVector();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        impact.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"latency\":\"decrease\""));
        assertTrue(json.contains("\"cpu\":\"decrease\""));
        assertTrue(json.contains("\"memory\":\"neutral\""));
        assertTrue(json.contains("\"correctness\":\"increase\""));
        assertTrue(json.contains("\"confidence\":0.95"));
        assertTrue(json.contains("\"estimated_improvement\":\"50% latency reduction\""));
    }

    public void testToXContentWithoutEstimatedImprovement() throws IOException {
        ImpactVector impact = ImpactVector.builder().latency(Direction.DECREASE).build();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        impact.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"latency\":\"decrease\""));
        assertFalse(json.contains("estimated_improvement"));
    }

    private ImpactVector createTestImpactVector() {
        return ImpactVector.builder()
            .latency(Direction.DECREASE)
            .cpu(Direction.DECREASE)
            .memory(Direction.NEUTRAL)
            .correctness(Direction.INCREASE)
            .confidence(0.95)
            .estimatedImprovement("50% latency reduction")
            .build();
    }

    private ImpactVector roundTrip(ImpactVector impact) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            impact.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new ImpactVector(in);
            }
        }
    }
}
