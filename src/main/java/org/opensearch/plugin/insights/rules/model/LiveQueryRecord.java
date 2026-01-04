/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.model;

import java.util.List;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;

/**
 * Lightweight record for live/finished queries
 */
public class LiveQueryRecord {
    private final String taskId;
    private final long startTime;
    private final long endTime;
    private final long latencyNanos;
    private final long cpuNanos;
    private final long memoryBytes;
    private final String nodeId;
    private final boolean cancelled;
    private final String wlmGroupId;
    private final String source;
    private final String searchType;
    private final int totalShards;
    private final String[] indices;
    private final List<TaskResourceInfo> taskResourceUsages;

    public LiveQueryRecord(
        String taskId,
        long startTime,
        long endTime,
        long latencyNanos,
        long cpuNanos,
        long memoryBytes,
        String nodeId,
        boolean cancelled,
        String wlmGroupId,
        String source,
        String searchType,
        int totalShards,
        String[] indices,
        List<TaskResourceInfo> taskResourceUsages
    ) {
        this.taskId = taskId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.latencyNanos = latencyNanos;
        this.cpuNanos = cpuNanos;
        this.memoryBytes = memoryBytes;
        this.nodeId = nodeId;
        this.cancelled = cancelled;
        this.wlmGroupId = wlmGroupId;
        this.source = source;
        this.searchType = searchType;
        this.totalShards = totalShards;
        this.indices = indices;
        this.taskResourceUsages = taskResourceUsages;
    }

    public String getTaskId() {
        return taskId;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public long getLatencyNanos() {
        return latencyNanos;
    }

    public long getCpuNanos() {
        return cpuNanos;
    }

    public long getMemoryBytes() {
        return memoryBytes;
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public String getWlmGroupId() {
        return wlmGroupId;
    }

    public String getSource() {
        return source;
    }

    public String getSearchType() {
        return searchType;
    }

    public int getTotalShards() {
        return totalShards;
    }

    public String[] getIndices() {
        return indices;
    }

    public List<TaskResourceInfo> getTaskResourceUsages() {
        return taskResourceUsages;
    }
}
