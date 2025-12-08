/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Minimal cached query record with only essential fields
 */
public class CachedQueryRecord implements Writeable, ToXContentObject {

    public final long timestamp;
    public final String taskId;
    public final String nodeId;
    public final long latencyNanos;
    public final long cpuNanos;
    public final long memoryBytes;
    public final String workloadGroup;
    public final String searchType;

    public CachedQueryRecord(
        long timestamp,
        String taskId,
        String nodeId,
        long latencyNanos,
        long cpuNanos,
        long memoryBytes,
        String workloadGroup,
        String searchType
    ) {
        this.timestamp = timestamp;
        this.taskId = taskId;
        this.nodeId = nodeId;
        this.latencyNanos = latencyNanos;
        this.cpuNanos = cpuNanos;
        this.memoryBytes = memoryBytes;
        this.workloadGroup = workloadGroup;
        this.searchType = searchType;
    }

    public CachedQueryRecord(StreamInput in) throws IOException {
        this.timestamp = in.readLong();
        this.taskId = in.readString();
        this.nodeId = in.readString();
        this.latencyNanos = in.readLong();
        this.cpuNanos = in.readLong();
        this.memoryBytes = in.readLong();
        this.workloadGroup = in.readOptionalString();
        this.searchType = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeString(taskId);
        out.writeString(nodeId);
        out.writeLong(latencyNanos);
        out.writeLong(cpuNanos);
        out.writeLong(memoryBytes);
        out.writeOptionalString(workloadGroup);
        out.writeOptionalString(searchType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("timestamp", timestamp);
        builder.field("task_id", taskId);
        builder.field("node_id", nodeId);
        builder.field("latency_nanos", latencyNanos);
        builder.field("cpu_nanos", cpuNanos);
        builder.field("memory_bytes", memoryBytes);
        if (workloadGroup != null) {
            builder.field("workload_group_id", workloadGroup);
        }
        if (searchType != null) {
            builder.field("search_type", searchType);
        }
        return builder.endObject();
    }

    public long getLatencyNanos() {
        return latencyNanos;
    }

    public long timestamp() {
        return timestamp;
    }

    public String taskId() {
        return taskId;
    }

    public String nodeId() {
        return nodeId;
    }

    public long cpuNanos() {
        return cpuNanos;
    }

    public long memoryBytes() {
        return memoryBytes;
    }

    public String workloadGroup() {
        return workloadGroup;
    }
}
