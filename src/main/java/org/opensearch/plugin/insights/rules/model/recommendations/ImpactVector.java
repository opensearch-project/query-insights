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
 * Represents the estimated impact of applying a recommendation
 */
public class ImpactVector implements ToXContentObject, Writeable {
    private final Direction latency;
    private final Direction cpu;
    private final Direction memory;
    private final Direction correctness;
    private final double confidence;
    private final String estimatedImprovement;

    private ImpactVector(Builder builder) {
        this.latency = builder.latency;
        this.cpu = builder.cpu;
        this.memory = builder.memory;
        this.correctness = builder.correctness;
        this.confidence = builder.confidence;
        this.estimatedImprovement = builder.estimatedImprovement;
    }

    /**
     * Create an ImpactVector from a StreamInput
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public ImpactVector(StreamInput in) throws IOException {
        this.latency = in.readEnum(Direction.class);
        this.cpu = in.readEnum(Direction.class);
        this.memory = in.readEnum(Direction.class);
        this.correctness = in.readEnum(Direction.class);
        this.confidence = in.readDouble();
        this.estimatedImprovement = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(latency);
        out.writeEnum(cpu);
        out.writeEnum(memory);
        out.writeEnum(correctness);
        out.writeDouble(confidence);
        out.writeOptionalString(estimatedImprovement);
    }

    /**
     * @return the latency impact direction
     */
    public Direction getLatency() {
        return latency;
    }

    /**
     * @return the CPU impact direction
     */
    public Direction getCpu() {
        return cpu;
    }

    /**
     * @return the memory impact direction
     */
    public Direction getMemory() {
        return memory;
    }

    /**
     * @return the correctness impact direction
     */
    public Direction getCorrectness() {
        return correctness;
    }

    /**
     * @return the confidence level (0.0 to 1.0)
     */
    public double getConfidence() {
        return confidence;
    }

    /**
     * @return the estimated improvement description
     */
    public String getEstimatedImprovement() {
        return estimatedImprovement;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (latency != null) {
            builder.field("latency", latency.toString().toLowerCase(Locale.ROOT));
        }
        if (cpu != null) {
            builder.field("cpu", cpu.toString().toLowerCase(Locale.ROOT));
        }
        if (memory != null) {
            builder.field("memory", memory.toString().toLowerCase(Locale.ROOT));
        }
        if (correctness != null) {
            builder.field("correctness", correctness.toString().toLowerCase(Locale.ROOT));
        }
        builder.field("confidence", confidence);
        if (estimatedImprovement != null) {
            builder.field("estimated_improvement", estimatedImprovement);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ImpactVector that = (ImpactVector) o;
        return Double.compare(that.confidence, confidence) == 0
            && latency == that.latency
            && cpu == that.cpu
            && memory == that.memory
            && correctness == that.correctness
            && Objects.equals(estimatedImprovement, that.estimatedImprovement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latency, cpu, memory, correctness, confidence, estimatedImprovement);
    }

    /**
     * Builder for ImpactVector
     */
    public static class Builder {
        private Direction latency = Direction.NEUTRAL;
        private Direction cpu = Direction.NEUTRAL;
        private Direction memory = Direction.NEUTRAL;
        private Direction correctness = Direction.NEUTRAL;
        private double confidence = 1.0;
        private String estimatedImprovement;

        /**
         * Set the latency impact direction
         * @param latency the direction
         * @return this builder
         */
        public Builder latency(Direction latency) {
            this.latency = latency;
            return this;
        }

        /**
         * Set the CPU impact direction
         * @param cpu the direction
         * @return this builder
         */
        public Builder cpu(Direction cpu) {
            this.cpu = cpu;
            return this;
        }

        /**
         * Set the memory impact direction
         * @param memory the direction
         * @return this builder
         */
        public Builder memory(Direction memory) {
            this.memory = memory;
            return this;
        }

        /**
         * Set the correctness impact direction
         * @param correctness the direction
         * @return this builder
         */
        public Builder correctness(Direction correctness) {
            this.correctness = correctness;
            return this;
        }

        /**
         * Set the confidence level
         * @param confidence the confidence (0.0 to 1.0)
         * @return this builder
         */
        public Builder confidence(double confidence) {
            this.confidence = confidence;
            return this;
        }

        /**
         * Set the estimated improvement description
         * @param estimatedImprovement the description
         * @return this builder
         */
        public Builder estimatedImprovement(String estimatedImprovement) {
            this.estimatedImprovement = estimatedImprovement;
            return this;
        }

        /**
         * Build the ImpactVector
         * @return the impact vector
         */
        public ImpactVector build() {
            return new ImpactVector(this);
        }
    }

    /**
     * Create a new builder
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }
}
