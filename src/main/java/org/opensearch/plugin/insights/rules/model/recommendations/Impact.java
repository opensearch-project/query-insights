/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.recommendations;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Represents the impact on a single dimension (e.g., latency, cpu, memory, correctness).
 * Contains a direction and an optional numeric value for estimated magnitude.
 */
public class Impact implements ToXContentObject, Writeable {
    private final Direction direction;
    private final Double value;

    /**
     * Create an Impact with direction only
     * @param direction the direction of impact
     */
    public Impact(Direction direction) {
        this.direction = direction;
        this.value = null;
    }

    /**
     * Create an Impact with direction and value
     * @param direction the direction of impact
     * @param value the estimated magnitude of impact
     */
    public Impact(Direction direction, Double value) {
        this.direction = direction;
        this.value = value;
    }

    /**
     * Create an Impact from a StreamInput
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public Impact(StreamInput in) throws IOException {
        this.direction = in.readEnum(Direction.class);
        this.value = in.readOptionalDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(direction);
        out.writeOptionalDouble(value);
    }

    /**
     * @return the direction of impact
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * @return the estimated magnitude of impact, or null if not available
     */
    public Double getValue() {
        return value;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (direction != null) {
            builder.field("direction", direction.toString().toLowerCase(Locale.ROOT));
        }
        if (value != null) {
            builder.field("value", value);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Impact impact = (Impact) o;
        return direction == impact.direction && Objects.equals(value, impact.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(direction, value);
    }
}
