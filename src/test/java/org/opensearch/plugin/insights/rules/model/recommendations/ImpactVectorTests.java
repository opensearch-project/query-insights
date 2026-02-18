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
            .latency(new Impact(Direction.DECREASE))
            .cpu(new Impact(Direction.DECREASE))
            .memory(new Impact(Direction.NEUTRAL))
            .correctness(new Impact(Direction.INCREASE))
            .estimatedImprovement("50% latency reduction")
            .build();

        assertEquals(Direction.DECREASE, impact.getLatency().getDirection());
        assertEquals(Direction.DECREASE, impact.getCpu().getDirection());
        assertEquals(Direction.NEUTRAL, impact.getMemory().getDirection());
        assertEquals(Direction.INCREASE, impact.getCorrectness().getDirection());
        assertEquals("50% latency reduction", impact.getEstimatedImprovement());
    }

    public void testBuilderDefaults() {
        ImpactVector impact = ImpactVector.builder().build();

        assertEquals(Direction.NEUTRAL, impact.getLatency().getDirection());
        assertEquals(Direction.NEUTRAL, impact.getCpu().getDirection());
        assertEquals(Direction.NEUTRAL, impact.getMemory().getDirection());
        assertEquals(Direction.NEUTRAL, impact.getCorrectness().getDirection());
        assertNull(impact.getEstimatedImprovement());
    }

    public void testBuilderWithImpactValues() {
        ImpactVector impact = ImpactVector.builder()
            .latency(new Impact(Direction.DECREASE, 0.5))
            .cpu(new Impact(Direction.DECREASE, 0.3))
            .memory(new Impact(Direction.NEUTRAL))
            .correctness(new Impact(Direction.INCREASE))
            .build();

        assertEquals(Direction.DECREASE, impact.getLatency().getDirection());
        assertEquals(0.5, impact.getLatency().getValue(), 0.001);
        assertEquals(Direction.DECREASE, impact.getCpu().getDirection());
        assertEquals(0.3, impact.getCpu().getValue(), 0.001);
        assertNull(impact.getMemory().getValue());
        assertNull(impact.getCorrectness().getValue());
    }

    public void testSerialization() throws IOException {
        ImpactVector original = createTestImpactVector();
        ImpactVector deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertEquals(original.getLatency(), deserialized.getLatency());
        assertEquals(original.getCpu(), deserialized.getCpu());
        assertEquals(original.getMemory(), deserialized.getMemory());
        assertEquals(original.getCorrectness(), deserialized.getCorrectness());
        assertEquals(original.getEstimatedImprovement(), deserialized.getEstimatedImprovement());
    }

    public void testSerializationWithNullImprovement() throws IOException {
        ImpactVector original = ImpactVector.builder().latency(new Impact(Direction.DECREASE)).build();
        ImpactVector deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertNull(deserialized.getEstimatedImprovement());
    }

    public void testSerializationWithImpactValues() throws IOException {
        ImpactVector original = ImpactVector.builder()
            .latency(new Impact(Direction.DECREASE, 0.5))
            .cpu(new Impact(Direction.NEUTRAL))
            .memory(new Impact(Direction.NEUTRAL))
            .correctness(new Impact(Direction.INCREASE, 0.9))
            .build();
        ImpactVector deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertEquals(0.5, deserialized.getLatency().getValue(), 0.001);
        assertNull(deserialized.getCpu().getValue());
        assertEquals(0.9, deserialized.getCorrectness().getValue(), 0.001);
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
                .latency(new Impact(Direction.INCREASE))
                .cpu(new Impact(Direction.DECREASE))
                .memory(new Impact(Direction.NEUTRAL))
                .correctness(new Impact(Direction.INCREASE))
                .estimatedImprovement("50% latency reduction")
                .build()
        );

        assertNotEquals(
            base,
            ImpactVector.builder()
                .latency(new Impact(Direction.DECREASE))
                .cpu(new Impact(Direction.DECREASE))
                .memory(new Impact(Direction.NEUTRAL))
                .correctness(new Impact(Direction.INCREASE))
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

        assertTrue(json.contains("\"latency\":{\"direction\":\"decrease\"}"));
        assertTrue(json.contains("\"cpu\":{\"direction\":\"decrease\"}"));
        assertTrue(json.contains("\"memory\":{\"direction\":\"neutral\"}"));
        assertTrue(json.contains("\"correctness\":{\"direction\":\"increase\"}"));
        assertTrue(json.contains("\"estimated_improvement\":\"50% latency reduction\""));
    }

    public void testToXContentWithValues() throws IOException {
        ImpactVector impact = ImpactVector.builder()
            .latency(new Impact(Direction.DECREASE, 0.5))
            .cpu(new Impact(Direction.NEUTRAL))
            .memory(new Impact(Direction.NEUTRAL))
            .correctness(new Impact(Direction.INCREASE))
            .build();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        impact.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"latency\":{\"direction\":\"decrease\",\"value\":0.5}"));
        assertTrue(json.contains("\"cpu\":{\"direction\":\"neutral\"}"));
    }

    public void testToXContentWithoutEstimatedImprovement() throws IOException {
        ImpactVector impact = ImpactVector.builder().latency(new Impact(Direction.DECREASE)).build();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        impact.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"latency\":{\"direction\":\"decrease\"}"));
        assertFalse(json.contains("estimated_improvement"));
    }

    private ImpactVector createTestImpactVector() {
        return ImpactVector.builder()
            .latency(new Impact(Direction.DECREASE))
            .cpu(new Impact(Direction.DECREASE))
            .memory(new Impact(Direction.NEUTRAL))
            .correctness(new Impact(Direction.INCREASE))
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
