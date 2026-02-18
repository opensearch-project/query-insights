/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.recommendations;

import java.io.IOException;
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
    private final Impact latency;
    private final Impact cpu;
    private final Impact memory;
    private final Impact correctness;
    private final String estimatedImprovement;

    private ImpactVector(Builder builder) {
        this.latency = builder.latency;
        this.cpu = builder.cpu;
        this.memory = builder.memory;
        this.correctness = builder.correctness;
        this.estimatedImprovement = builder.estimatedImprovement;
    }

    /**
     * Create an ImpactVector from a StreamInput
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public ImpactVector(StreamInput in) throws IOException {
        this.latency = new Impact(in);
        this.cpu = new Impact(in);
        this.memory = new Impact(in);
        this.correctness = new Impact(in);
        this.estimatedImprovement = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        latency.writeTo(out);
        cpu.writeTo(out);
        memory.writeTo(out);
        correctness.writeTo(out);
        out.writeOptionalString(estimatedImprovement);
    }

    /**
     * @return the latency impact
     */
    public Impact getLatency() {
        return latency;
    }

    /**
     * @return the CPU impact
     */
    public Impact getCpu() {
        return cpu;
    }

    /**
     * @return the memory impact
     */
    public Impact getMemory() {
        return memory;
    }

    /**
     * @return the correctness impact
     */
    public Impact getCorrectness() {
        return correctness;
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
            builder.field("latency", latency);
        }
        if (cpu != null) {
            builder.field("cpu", cpu);
        }
        if (memory != null) {
            builder.field("memory", memory);
        }
        if (correctness != null) {
            builder.field("correctness", correctness);
        }
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
        return Objects.equals(latency, that.latency)
            && Objects.equals(cpu, that.cpu)
            && Objects.equals(memory, that.memory)
            && Objects.equals(correctness, that.correctness)
            && Objects.equals(estimatedImprovement, that.estimatedImprovement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latency, cpu, memory, correctness, estimatedImprovement);
    }

    /**
     * Builder for ImpactVector
     */
    public static class Builder {
        private Impact latency = new Impact(Direction.NEUTRAL);
        private Impact cpu = new Impact(Direction.NEUTRAL);
        private Impact memory = new Impact(Direction.NEUTRAL);
        private Impact correctness = new Impact(Direction.NEUTRAL);
        private String estimatedImprovement;

        /**
         * Set the latency impact
         * @param latency the impact
         * @return this builder
         */
        public Builder latency(Impact latency) {
            this.latency = latency;
            return this;
        }

        /**
         * Set the CPU impact
         * @param cpu the impact
         * @return this builder
         */
        public Builder cpu(Impact cpu) {
            this.cpu = cpu;
            return this;
        }

        /**
         * Set the memory impact
         * @param memory the impact
         * @return this builder
         */
        public Builder memory(Impact memory) {
            this.memory = memory;
            return this;
        }

        /**
         * Set the correctness impact
         * @param correctness the impact
         * @return this builder
         */
        public Builder correctness(Impact correctness) {
            this.correctness = correctness;
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
