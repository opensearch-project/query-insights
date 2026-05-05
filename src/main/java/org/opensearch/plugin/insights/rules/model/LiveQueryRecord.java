/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Record for live/active queries with real-time status and task details
 */
public class LiveQueryRecord implements Writeable, ToXContentObject {

    private final String liveQueryRecordId;
    private final String status;
    private final long startTime;
    private final String wlmGroupId;
    private final long totalLatency;
    private final long totalCpu;
    private final long totalMemory;
    private final TaskDetails coordinatorTask;
    private final List<TaskDetails> shardTasks;
    private final String username;
    private final List<String> userRoles;
    private final List<String> backendRoles;

    public LiveQueryRecord(
        String liveQueryRecordId,
        String status,
        long startTime,
        String wlmGroupId,
        long totalLatency,
        long totalCpu,
        long totalMemory,
        TaskDetails coordinatorTask,
        List<TaskDetails> shardTasks,
        String username,
        List<String> userRoles,
        List<String> backendRoles
    ) {
        this.liveQueryRecordId = liveQueryRecordId;
        this.status = status;
        this.startTime = startTime;
        this.wlmGroupId = wlmGroupId;
        this.totalLatency = totalLatency;
        this.totalCpu = totalCpu;
        this.totalMemory = totalMemory;
        this.coordinatorTask = coordinatorTask;
        this.shardTasks = shardTasks != null ? shardTasks : new ArrayList<>();
        this.username = username;
        this.userRoles = userRoles != null ? userRoles : List.of();
        this.backendRoles = backendRoles != null ? backendRoles : List.of();
    }

    public LiveQueryRecord(StreamInput in) throws IOException {
        this.liveQueryRecordId = in.readString();
        this.status = in.readString();
        this.startTime = in.readLong();
        this.wlmGroupId = in.readOptionalString();
        this.totalLatency = in.readLong();
        this.totalCpu = in.readLong();
        this.totalMemory = in.readLong();
        this.coordinatorTask = in.readOptionalWriteable(TaskDetails::new);
        this.shardTasks = in.readList(TaskDetails::new);
        this.username = in.readOptionalString();
        this.userRoles = in.readOptionalStringList();
        this.backendRoles = in.readOptionalStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(liveQueryRecordId);
        out.writeString(status);
        out.writeLong(startTime);
        out.writeOptionalString(wlmGroupId);
        out.writeLong(totalLatency);
        out.writeLong(totalCpu);
        out.writeLong(totalMemory);
        out.writeOptionalWriteable(coordinatorTask);
        out.writeList(shardTasks);
        out.writeOptionalString(username);
        out.writeOptionalStringCollection(userRoles.isEmpty() ? null : userRoles);
        out.writeOptionalStringCollection(backendRoles.isEmpty() ? null : backendRoles);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", liveQueryRecordId);
        builder.field("status", status);
        builder.field("start_time", startTime);
        if (wlmGroupId != null) {
            builder.field("wlm_group_id", wlmGroupId);
        }
        builder.field("total_latency_millis", totalLatency);
        builder.field("total_cpu_nanos", totalCpu);
        builder.field("total_memory_bytes", totalMemory);
        if (coordinatorTask != null) {
            builder.field("coordinator_task");
            coordinatorTask.toXContent(builder, params);
        }
        builder.startArray("shard_tasks");
        for (TaskDetails task : shardTasks) {
            task.toXContent(builder, params);
        }
        builder.endArray();
        if (username != null) {
            builder.field("username", username);
        }
        if (!userRoles.isEmpty()) {
            builder.array("user_roles", userRoles.toArray(new String[0]));
        }
        if (!backendRoles.isEmpty()) {
            builder.array("backend_roles", backendRoles.toArray(new String[0]));
        }
        builder.endObject();
        return builder;
    }

    public String getQueryId() {
        return liveQueryRecordId;
    }

    public String getStatus() {
        return status;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getWlmGroupId() {
        return wlmGroupId;
    }

    public long getTotalLatency() {
        return totalLatency;
    }

    public long getTotalCpu() {
        return totalCpu;
    }

    public long getTotalMemory() {
        return totalMemory;
    }

    public TaskDetails getCoordinatorTask() {
        return coordinatorTask;
    }

    public List<TaskDetails> getShardTasks() {
        return shardTasks;
    }

    public String getUsername() {
        return username;
    }

    public List<String> getUserRoles() {
        return userRoles;
    }

    public List<String> getBackendRoles() {
        return backendRoles;
    }
}
