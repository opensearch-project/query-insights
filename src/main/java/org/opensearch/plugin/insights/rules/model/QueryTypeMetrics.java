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
 * Represents aggregated metrics for a specific query type
 * Tracks count, latency, CPU, and memory statistics.
 */
public class QueryTypeMetrics implements ToXContentObject, Writeable {
    private final String queryType;
    private long count;
    private long latencyTotal;
    private long latencyMin;
    private long latencyMax;
    private long cpuTotal;
    private long cpuMin;
    private long cpuMax;
    private long memoryTotal;
    private long memoryMin;
    private long memoryMax;

    /**
     * Constructor for QueryTypeMetrics
     *
     * @param queryType the type of query (e.g., aggregation type)
     */
    public QueryTypeMetrics(String queryType) {
        this.queryType = queryType;
        this.count = 0;
        this.latencyTotal = 0;
        this.latencyMin = Long.MAX_VALUE;
        this.latencyMax = Long.MIN_VALUE;
        this.cpuTotal = 0;
        this.cpuMin = Long.MAX_VALUE;
        this.cpuMax = Long.MIN_VALUE;
        this.memoryTotal = 0;
        this.memoryMin = Long.MAX_VALUE;
        this.memoryMax = Long.MIN_VALUE;
    }

    /**
     * Constructor for QueryTypeMetrics from StreamInput
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs
     */
    public QueryTypeMetrics(StreamInput in) throws IOException {
        this.queryType = in.readString();
        this.count = in.readVLong();
        this.latencyTotal = in.readVLong();
        this.latencyMin = in.readLong();
        this.latencyMax = in.readLong();
        this.cpuTotal = in.readVLong();
        this.cpuMin = in.readLong();
        this.cpuMax = in.readLong();
        this.memoryTotal = in.readVLong();
        this.memoryMin = in.readLong();
        this.memoryMax = in.readLong();
    }

    /**
     * Record a measurement for this query type
     *
     * @param latency latency in milliseconds
     * @param cpu CPU usage in nanoseconds
     * @param memory memory usage in bytes
     */
    public synchronized void recordMeasurement(long latency, long cpu, long memory) {
        count++;

        latencyTotal += latency;
        latencyMin = Math.min(latencyMin, latency);
        latencyMax = Math.max(latencyMax, latency);

        cpuTotal += cpu;
        cpuMin = Math.min(cpuMin, cpu);
        cpuMax = Math.max(cpuMax, cpu);

        memoryTotal += memory;
        memoryMin = Math.min(memoryMin, memory);
        memoryMax = Math.max(memoryMax, memory);
    }

    /**
     * Get the query type
     *
     * @return the query type
     */
    public String getQueryType() {
        return queryType;
    }

    /**
     * Get the count of queries
     *
     * @return the count
     */
    public long getCount() {
        return count;
    }

    /**
     * Get the total latency
     *
     * @return total latency in milliseconds
     */
    public long getLatencyTotal() {
        return latencyTotal;
    }

    /**
     * Get the average latency
     *
     * @return average latency in milliseconds
     */
    public double getLatencyAvg() {
        return count > 0 ? (double) latencyTotal / count : 0;
    }

    /**
     * Get the minimum latency
     *
     * @return minimum latency in milliseconds
     */
    public long getLatencyMin() {
        return count > 0 ? latencyMin : 0;
    }

    /**
     * Get the maximum latency
     *
     * @return maximum latency in milliseconds
     */
    public long getLatencyMax() {
        return count > 0 ? latencyMax : 0;
    }

    /**
     * Get the total CPU usage
     *
     * @return total CPU in nanoseconds
     */
    public long getCpuTotal() {
        return cpuTotal;
    }

    /**
     * Get the average CPU usage
     *
     * @return average CPU in nanoseconds
     */
    public double getCpuAvg() {
        return count > 0 ? (double) cpuTotal / count : 0;
    }

    /**
     * Get the minimum CPU usage
     *
     * @return minimum CPU in nanoseconds
     */
    public long getCpuMin() {
        return count > 0 ? cpuMin : 0;
    }

    /**
     * Get the maximum CPU usage
     *
     * @return maximum CPU in nanoseconds
     */
    public long getCpuMax() {
        return count > 0 ? cpuMax : 0;
    }

    /**
     * Get the total memory usage
     *
     * @return total memory in bytes
     */
    public long getMemoryTotal() {
        return memoryTotal;
    }

    /**
     * Get the average memory usage
     *
     * @return average memory in bytes
     */
    public double getMemoryAvg() {
        return count > 0 ? (double) memoryTotal / count : 0;
    }

    /**
     * Get the minimum memory usage
     *
     * @return minimum memory in bytes
     */
    public long getMemoryMin() {
        return count > 0 ? memoryMin : 0;
    }

    /**
     * Get the maximum memory usage
     *
     * @return maximum memory in bytes
     */
    public long getMemoryMax() {
        return count > 0 ? memoryMax : 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(queryType);
        out.writeVLong(count);
        out.writeVLong(latencyTotal);
        out.writeLong(latencyMin);
        out.writeLong(latencyMax);
        out.writeVLong(cpuTotal);
        out.writeLong(cpuMin);
        out.writeLong(cpuMax);
        out.writeVLong(memoryTotal);
        out.writeLong(memoryMin);
        out.writeLong(memoryMax);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("query_type", queryType);
        builder.field("count", count);

        builder.startObject("latency");
        builder.field("total", latencyTotal);
        builder.field("avg", getLatencyAvg());
        builder.field("min", getLatencyMin());
        builder.field("max", getLatencyMax());
        builder.endObject();

        builder.startObject("cpu");
        builder.field("total", cpuTotal);
        builder.field("avg", getCpuAvg());
        builder.field("min", getCpuMin());
        builder.field("max", getCpuMax());
        builder.endObject();

        builder.startObject("memory");
        builder.field("total", memoryTotal);
        builder.field("avg", getMemoryAvg());
        builder.field("min", getMemoryMin());
        builder.field("max", getMemoryMax());
        builder.endObject();

        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryTypeMetrics that = (QueryTypeMetrics) o;
        return Objects.equals(queryType, that.queryType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryType);
    }
}
