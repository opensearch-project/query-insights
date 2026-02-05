/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Minimal shard task record for live queries
 */
public class ShardTaskRecord implements Writeable, ToXContentObject {

    private final String shardId;
    private final String nodeId;
    private final String action;
    private final long latency;
    private final long cpuNanos;
    private final long memoryBytes;
    private final String parentTaskId;

    public ShardTaskRecord(
        String shardId,
        String nodeId,
        String action,
        long latency,
        long cpuNanos,
        long memoryBytes,
        String parentTaskId
    ) {
        this.shardId = shardId;
        this.nodeId = nodeId;
        this.action = action;
        this.latency = latency;
        this.cpuNanos = cpuNanos;
        this.memoryBytes = memoryBytes;
        this.parentTaskId = parentTaskId;
    }

    public ShardTaskRecord(StreamInput in) throws IOException {
        this.shardId = in.readString();
        this.nodeId = in.readString();
        this.action = in.readString();
        this.latency = in.readLong();
        this.cpuNanos = in.readLong();
        this.memoryBytes = in.readLong();
        this.parentTaskId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(shardId);
        out.writeString(nodeId);
        out.writeString(action);
        out.writeLong(latency);
        out.writeLong(cpuNanos);
        out.writeLong(memoryBytes);
        out.writeOptionalString(parentTaskId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("shard_id", shardId);
        builder.field("node_id", nodeId);
        builder.field("action", action);
        builder.field("latency_nanos", latency);
        builder.field("cpu_nanos", cpuNanos);
        builder.field("memory_bytes", memoryBytes);
        if (parentTaskId != null) {
            builder.field("parent_task_id", parentTaskId);
        }
        builder.endObject();
        return builder;
    }

    public String getShardId() {
        return shardId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getAction() {
        return action;
    }

    public long getLatency() {
        return latency;
    }

    public long getCpuNanos() {
        return cpuNanos;
    }

    public long getMemoryBytes() {
        return memoryBytes;
    }

    public String getParentTaskId() {
        return parentTaskId;
    }
}
