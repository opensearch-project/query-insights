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
 * Record for individual task (coordinator or shard) in a live query
 */
public class TaskRecord implements Writeable, ToXContentObject {

    private final String taskId;
    private final String parentTaskId;
    private final String nodeId;
    private final String status;
    private final String action;
    private final long startTime;
    private final long latency;
    private final long cpu;
    private final long memory;
    private final String description;
    private final String wlmGroupId;

    public TaskRecord(
        String taskId,
        String parentTaskId,
        String nodeId,
        String status,
        String action,
        long startTime,
        long latency,
        long cpu,
        long memory,
        String description,
        String wlmGroupId
    ) {
        this.taskId = taskId;
        this.parentTaskId = parentTaskId;
        this.nodeId = nodeId;
        this.status = status;
        this.action = action;
        this.startTime = startTime;
        this.latency = latency;
        this.cpu = cpu;
        this.memory = memory;
        this.description = description;
        this.wlmGroupId = wlmGroupId;
    }

    public TaskRecord(StreamInput in) throws IOException {
        this.taskId = in.readString();
        this.parentTaskId = in.readOptionalString();
        this.nodeId = in.readString();
        this.status = in.readString();
        this.action = in.readString();
        this.startTime = in.readLong();
        this.latency = in.readLong();
        this.cpu = in.readLong();
        this.memory = in.readLong();
        this.description = in.readOptionalString();
        this.wlmGroupId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(taskId);
        out.writeOptionalString(parentTaskId);
        out.writeString(nodeId);
        out.writeString(status);
        out.writeString(action);
        out.writeLong(startTime);
        out.writeLong(latency);
        out.writeLong(cpu);
        out.writeLong(memory);
        out.writeOptionalString(description);
        out.writeOptionalString(wlmGroupId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("task_id", taskId);
        if (parentTaskId != null) {
            builder.field("parent_task_id", parentTaskId);
        }
        builder.field("node_id", nodeId);
        builder.field("status", status);
        builder.field("action", action);
        builder.field("start_time", startTime);
        builder.field("latency_millis", latency);
        builder.field("cpu_nanos", cpu);
        builder.field("memory_bytes", memory);
        if (description != null) {
            builder.field("description", description);
        }
        builder.endObject();
        return builder;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getParentTaskId() {
        return parentTaskId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getStatus() {
        return status;
    }

    public String getAction() {
        return action;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getLatency() {
        return latency;
    }

    public long getCpu() {
        return cpu;
    }

    public long getMemory() {
        return memory;
    }

    public String getDescription() {
        return description;
    }

    public String getWlmGroupId() {
        return wlmGroupId;
    }
}
