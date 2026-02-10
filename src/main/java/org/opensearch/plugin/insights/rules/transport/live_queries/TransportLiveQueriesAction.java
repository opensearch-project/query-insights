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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    private static final Logger logger = LogManager.getLogger(TransportLiveQueriesAction.class);

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
                queryId = task.getNodeId() + ":" + task.getTaskId();
                queryStatus.put(queryId, task.getStatus());
                queryStartTime.put(queryId, task.getStartTime());
            } else {
                queryId = task.getParentTaskId();
            }
            tasksByQuery.computeIfAbsent(queryId, k -> new ArrayList<>()).add(task);
        }

        // Build aggregated LiveQueryRecords
        List<LiveQueryRecord> aggregatedRecords = new ArrayList<>();
        for (Map.Entry<String, List<TaskRecord>> entry : tasksByQuery.entrySet()) {
            String queryId = entry.getKey();
            List<TaskRecord> tasks = entry.getValue();

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

        // Filter by taskId if specified
        if (request.getTaskId() != null) {
            aggregatedRecords.removeIf(record -> !request.getTaskId().equals(record.getQueryId()));
        }

        // Filter by nodeId if specified
        if (request.nodesIds() != null && request.nodesIds().length > 0) {
            List<String> nodeIdList = java.util.Arrays.asList(request.nodesIds());
            aggregatedRecords.removeIf(
                record -> record.getCoordinatorTask() == null || !nodeIdList.contains(record.getCoordinatorTask().getNodeId())
            );
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
        List<Task> taskSnapshot = new ArrayList<>(allTasks.values());

        for (Task runningTask : taskSnapshot) {
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

        long startTimeNanos = runningTask.getStartTimeNanos();
        long startTime = runningTask.getStartTime();
        String taskId = String.valueOf(runningTask.getId());
        String action = runningTask.getAction();
        String description = runningTask.getDescription();

        long runningNanos = System.nanoTime() - startTimeNanos;
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

        return new TaskRecord(
            taskId,
            parentTaskId,
            nodeId,
            status,
            action,
            startTime,
            runningMillis,
            cpuNanos,
            memBytes,
            description,
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
