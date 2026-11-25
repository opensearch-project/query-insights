/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Count and summed cost of the searches a running PPL query has finished. Live Queries only lists
 * running tasks, so these would otherwise be missing.
 */
public final class FinishedSearches implements Writeable, ToXContentObject {

    private final int count;
    private final long cpuNanos;
    private final long memoryBytes;

    /**
     * @param count       how many searches have finished
     * @param cpuNanos    their summed CPU time, in nanoseconds
     * @param memoryBytes their summed memory, in bytes
     */
    public FinishedSearches(final int count, final long cpuNanos, final long memoryBytes) {
        this.count = count;
        this.cpuNanos = cpuNanos;
        this.memoryBytes = memoryBytes;
    }

    /**
     * @param in the stream to read from
     * @throws IOException if reading fails
     */
    public FinishedSearches(final StreamInput in) throws IOException {
        this.count = in.readVInt();
        this.cpuNanos = in.readLong();
        this.memoryBytes = in.readLong();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(count);
        out.writeLong(cpuNanos);
        out.writeLong(memoryBytes);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("count", count);
        builder.field("cpu_nanos", cpuNanos);
        builder.field("memory_bytes", memoryBytes);
        return builder.endObject();
    }

    /**
     * @return how many searches have finished
     */
    public int getCount() {
        return count;
    }

    /**
     * @return their summed CPU time, in nanoseconds
     */
    public long getCpuNanos() {
        return cpuNanos;
    }

    /**
     * @return their summed memory, in bytes
     */
    public long getMemoryBytes() {
        return memoryBytes;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FinishedSearches)) {
            return false;
        }
        final FinishedSearches other = (FinishedSearches) o;
        return count == other.count && cpuNanos == other.cpuNanos && memoryBytes == other.memoryBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, cpuNanos, memoryBytes);
    }
}
