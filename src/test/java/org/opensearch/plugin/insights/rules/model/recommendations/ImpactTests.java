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
 * Unit tests for {@link Impact}
 */
public class ImpactTests extends OpenSearchTestCase {

    public void testDirectionOnly() {
        Impact impact = new Impact(Direction.DECREASE);

        assertEquals(Direction.DECREASE, impact.getDirection());
        assertNull(impact.getValue());
    }

    public void testDirectionAndValue() {
        Impact impact = new Impact(Direction.DECREASE, 0.5);

        assertEquals(Direction.DECREASE, impact.getDirection());
        assertEquals(0.5, impact.getValue(), 0.001);
    }

    public void testSerializationDirectionOnly() throws IOException {
        Impact original = new Impact(Direction.INCREASE);
        Impact deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertEquals(Direction.INCREASE, deserialized.getDirection());
        assertNull(deserialized.getValue());
    }

    public void testSerializationWithValue() throws IOException {
        Impact original = new Impact(Direction.DECREASE, 0.75);
        Impact deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertEquals(Direction.DECREASE, deserialized.getDirection());
        assertEquals(0.75, deserialized.getValue(), 0.001);
    }

    public void testEquals() {
        Impact impact1 = new Impact(Direction.DECREASE, 0.5);
        Impact impact2 = new Impact(Direction.DECREASE, 0.5);

        assertEquals(impact1, impact2);
        assertEquals(impact1, impact1);
        assertNotEquals(impact1, null);
        assertNotEquals(impact1, "not an impact");
    }

    public void testNotEquals() {
        Impact base = new Impact(Direction.DECREASE, 0.5);

        assertNotEquals(base, new Impact(Direction.INCREASE, 0.5));
        assertNotEquals(base, new Impact(Direction.DECREASE, 0.9));
        assertNotEquals(base, new Impact(Direction.DECREASE));
    }

    public void testHashCode() {
        Impact impact1 = new Impact(Direction.DECREASE, 0.5);
        Impact impact2 = new Impact(Direction.DECREASE, 0.5);

        assertEquals(impact1.hashCode(), impact2.hashCode());
    }

    public void testToXContentDirectionOnly() throws IOException {
        Impact impact = new Impact(Direction.DECREASE);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        impact.toXContent(builder, null);
        String json = builder.toString();

        assertEquals("{\"direction\":\"decrease\"}", json);
    }

    public void testToXContentWithValue() throws IOException {
        Impact impact = new Impact(Direction.DECREASE, 0.5);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        impact.toXContent(builder, null);
        String json = builder.toString();

        assertEquals("{\"direction\":\"decrease\",\"value\":0.5}", json);
    }

    private Impact roundTrip(Impact impact) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            impact.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new Impact(in);
            }
        }
    }
}
