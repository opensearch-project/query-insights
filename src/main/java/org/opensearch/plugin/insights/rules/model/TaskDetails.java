/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.Map;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.TaskInfo;

/**
 * Wrapper for TaskInfo with additional status information
 */
public class TaskDetails implements Writeable, ToXContentObject {

    private final TaskInfo taskInfo;
    private final String status;

    public TaskDetails(TaskInfo taskInfo, String status) {
        this.taskInfo = taskInfo;
        this.status = status;
    }

    public TaskDetails(StreamInput in) throws IOException {
        this.taskInfo = new TaskInfo(in);
        this.status = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskInfo.writeTo(out);
        out.writeString(status);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("task_id", taskInfo.getTaskId().toString());
        builder.field("node_id", taskInfo.getTaskId().getNodeId());
        builder.field("action", taskInfo.getAction());
        builder.field("status", status);
        if (taskInfo.getDescription() != null) {
            builder.field("description", taskInfo.getDescription());
        }
        builder.field("start_time", taskInfo.getStartTime());
        builder.field("running_time_nanos", taskInfo.getRunningTimeNanos());

        long cpuNanos = 0L;
        long memBytes = 0L;
        if (taskInfo.getResourceStats() != null) {
            Map<String, TaskResourceUsage> usageInfo = taskInfo.getResourceStats().getResourceUsageInfo();
            if (usageInfo != null && usageInfo.get("total") != null) {
                cpuNanos = usageInfo.get("total").getCpuTimeInNanos();
                memBytes = usageInfo.get("total").getMemoryInBytes();
            }
        }
        builder.field("cpu_nanos", cpuNanos);
        builder.field("memory_bytes", memBytes);
        builder.endObject();
        return builder;
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public String getStatus() {
        return status;
    }
}
