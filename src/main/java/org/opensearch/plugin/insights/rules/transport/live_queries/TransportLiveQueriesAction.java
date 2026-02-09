/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesNodeRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesNodeResponse;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.LiveQueryRecord;
import org.opensearch.plugin.insights.rules.model.TaskRecord;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action for fetching ongoing live queries
 */
public class TransportLiveQueriesAction extends TransportNodesAction<
    LiveQueriesRequest,
    LiveQueriesResponse,
    LiveQueriesNodeRequest,
    LiveQueriesNodeResponse> {

    @Inject
    public TransportLiveQueriesAction(
        final ThreadPool threadPool,
        final TransportService transportService,
        final ClusterService clusterService,
        final ActionFilters actionFilters
    ) {
        super(
            LiveQueriesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            LiveQueriesRequest::new,
            LiveQueriesNodeRequest::new,
            ThreadPool.Names.GENERIC,
            LiveQueriesNodeResponse.class
        );
    }

    @Override
    protected LiveQueriesResponse newResponse(
        LiveQueriesRequest request,
        List<LiveQueriesNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        // Collect all tasks from all nodes
        List<TaskRecord> allTasks = new ArrayList<>();
        for (LiveQueriesNodeResponse nodeResponse : responses) {
            allTasks.addAll(nodeResponse.getTasks());
        }

        // Group tasks by parent query ID and extract metadata
        Map<String, List<TaskRecord>> tasksByQuery = new HashMap<>();
        Map<String, String> queryStatus = new HashMap<>();
        Map<String, Long> queryStartTime = new HashMap<>();
        Map<String, String> queryWlmGroupId = new HashMap<>();

        for (TaskRecord task : allTasks) {
            String queryId;
            if (task.getParentTaskId() == null) {
                // This is a coordinator task
                queryId = task.getTaskId();
                queryStatus.put(queryId, task.getStatus());
                queryStartTime.put(queryId, task.getStartTime());
            } else {
                // This is a shard task - extract parent ID
                String parentId = task.getParentTaskId();
                if (parentId.contains(":")) {
                    parentId = parentId.substring(parentId.lastIndexOf(":") + 1);
                }
                queryId = parentId;
            }
            tasksByQuery.computeIfAbsent(queryId, k -> new ArrayList<>()).add(task);
        }

        // Build aggregated LiveQueryRecords
        List<LiveQueryRecord> aggregatedRecords = new ArrayList<>();
        for (Map.Entry<String, List<TaskRecord>> entry : tasksByQuery.entrySet()) {
            String queryId = entry.getKey();
            List<TaskRecord> tasks = entry.getValue();

            // Filter by taskId if specified
            if (request.getTaskId() != null && !request.getTaskId().equals(queryId)) {
                continue;
            }

            // Filter by nodeId if specified
            if (request.nodesIds() != null && request.nodesIds().length > 0) {
                boolean hasMatchingNode = tasks.stream()
                    .anyMatch(task -> java.util.Arrays.asList(request.nodesIds()).contains(task.getNodeId()));
                if (!hasMatchingNode) {
                    continue;
                }
            }

            // Separate coordinator and shard tasks
            TaskRecord coordinatorTask = null;
            List<TaskRecord> shardTasks = new ArrayList<>();
            String wlmGroupId = null;

            for (TaskRecord task : tasks) {
                if (task.getParentTaskId() == null && task.getAction().equals("indices:data/read/search")) {
                    coordinatorTask = task;
                    wlmGroupId = task.getWlmGroupId();
                } else {
                    shardTasks.add(task);
                }
            }

            // Calculate aggregated metrics
            long totalLatency = tasks.stream().mapToLong(TaskRecord::getLatency).max().orElse(0L);
            long totalCpu = tasks.stream().mapToLong(TaskRecord::getCpu).sum();
            long totalMemory = tasks.stream().mapToLong(TaskRecord::getMemory).sum();

            LiveQueryRecord aggregated = new LiveQueryRecord(
                queryId,
                queryStatus.getOrDefault(queryId, "RUNNING"),
                queryStartTime.getOrDefault(queryId, 0L),
                wlmGroupId,
                totalLatency,
                totalCpu,
                totalMemory,
                coordinatorTask,
                shardTasks
            );
            aggregatedRecords.add(aggregated);
        }

        // Sort and limit results based on request parameters
        aggregatedRecords.sort((a, b) -> {
            switch (request.getSortBy()) {
                case CPU:
                    return Long.compare(b.getTotalCpu(), a.getTotalCpu());
                case MEMORY:
                    return Long.compare(b.getTotalMemory(), a.getTotalMemory());
                default: // LATENCY
                    return Long.compare(b.getTotalLatency(), a.getTotalLatency());
            }
        });

        // Apply size limit
        if (aggregatedRecords.size() > request.getSize()) {
            aggregatedRecords = aggregatedRecords.subList(0, request.getSize());
        }

        // Create response with sorted and limited records
        return new LiveQueriesResponse(clusterService.getClusterName(), aggregatedRecords, failures);
    }

    @Override
    protected LiveQueriesNodeRequest newNodeRequest(LiveQueriesRequest request) {
        return new LiveQueriesNodeRequest(request);
    }

    @Override
    protected LiveQueriesNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new LiveQueriesNodeResponse(in);
    }

    @Override
    protected LiveQueriesNodeResponse nodeOperation(LiveQueriesNodeRequest request) {
        List<TaskRecord> localTasks = getLocalTasks(request.getRequest());
        return new LiveQueriesNodeResponse(transportService.getLocalNode(), localTasks);
    }

    private List<TaskRecord> getLocalTasks(LiveQueriesRequest request) {
        Map<Long, Task> allTasks = transportService.getTaskManager().getTasks();
        List<TaskRecord> taskRecords = new ArrayList<>();

        for (Task runningTask : allTasks.values()) {
            if (!isSearchTask(runningTask)) {
                continue;
            }

            TaskRecord record = createTaskRecord(runningTask, request);
            if (record != null) {
                taskRecords.add(record);
            }
        }

        return taskRecords;
    }

    private boolean isSearchTask(Task task) {
        return task.getAction().equals("indices:data/read/search") || task.getAction().startsWith("indices:data/read/search[phase/");
    }

    private TaskRecord createTaskRecord(Task runningTask, LiveQueriesRequest request) {
        String wlmGroupId = getWlmGroupId(runningTask);
        if (!isWlmGroupMatched(wlmGroupId, request.getWlmGroupId())) {
            return null;
        }

        long runningNanos = System.nanoTime() - runningTask.getStartTimeNanos();
        long runningMillis = runningNanos / 1_000_000;
        long cpuNanos = 0L;
        long memBytes = 0L;

        try {
            var totalResourceUsage = runningTask.getTotalResourceStats();
            if (totalResourceUsage != null) {
                cpuNanos = totalResourceUsage.getCpuTimeInNanos();
                memBytes = totalResourceUsage.getMemoryInBytes();
            }
        } catch (Exception e) {
            // Resource stats unavailable
        }

        String nodeId = transportService.getLocalNode().getId();
        String parentTaskId = runningTask.getParentTaskId() != null && runningTask.getParentTaskId().getId() != -1
            ? runningTask.getParentTaskId().toString()
            : null;
        String status = "RUNNING";
        String action = runningTask.getAction();

        // Create TaskRecord
        return new TaskRecord(
            String.valueOf(runningTask.getId()),
            parentTaskId,
            nodeId,
            status,
            action,
            runningTask.getStartTime(),
            runningMillis,
            cpuNanos,
            memBytes,
            runningTask.getDescription(),
            wlmGroupId
        );
    }

    private String getWlmGroupId(Task runningTask) {
        if (runningTask instanceof org.opensearch.wlm.WorkloadGroupTask workloadTask) {
            return workloadTask.getWorkloadGroupId();
        }
        return null;
    }

    private boolean isWlmGroupMatched(String taskWlmGroupId, String targetWlmGroupId) {
        return targetWlmGroupId == null || targetWlmGroupId.equals(taskWlmGroupId);
    }
}
