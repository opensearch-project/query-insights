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
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.ShardTaskRecord;
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
        List<SearchQueryRecord> allTasks = new ArrayList<>();
        for (LiveQueriesNodeResponse nodeResponse : responses) {
            allTasks.addAll(nodeResponse.getLiveQueries());
        }

        // Group parent and shard tasks globally
        Map<String, SearchQueryRecord> parentTasks = new HashMap<>();
        List<SearchQueryRecord> shardTasks = new ArrayList<>();

        for (SearchQueryRecord task : allTasks) {
            // Determine task type from stored action in LABELS
            Map<String, Object> labels = (Map<String, Object>) task.getAttributes().get(Attribute.LABELS);
            String taskAction = labels != null ? (String) labels.get("action") : null;
            if (taskAction != null && taskAction.startsWith("indices:data/read/search[phase/")) {
                shardTasks.add(task);
            } else {
                parentTasks.put(task.getId(), task);
            }
        }

        // Attach shard tasks to their parent tasks globally
        for (SearchQueryRecord shardTask : shardTasks) {
            String parentTaskId = (String) shardTask.getAttributes().get(Attribute.PARENT_TASK_ID);
            if (parentTaskId != null) {
                if (parentTaskId.contains(":")) {
                    parentTaskId = parentTaskId.substring(parentTaskId.lastIndexOf(":") + 1);
                }
                SearchQueryRecord parent = parentTasks.get(parentTaskId);
                if (parent != null) {
                    String taskDescription = (String) shardTask.getAttributes().get(Attribute.DESCRIPTION);
                    String shardId = extractShardIdFromDescription(taskDescription);
                    Map<String, Object> labels = (Map<String, Object>) shardTask.getAttributes().get(Attribute.LABELS);
                    String actionName = labels != null ? (String) labels.get("action") : "indices:data/read/search[phase/query]";
                    ShardTaskRecord shardRecord = new ShardTaskRecord(
                        shardId,
                        (String) shardTask.getAttributes().get(Attribute.NODE_ID),
                        actionName,
                        shardTask.getMeasurement(MetricType.LATENCY) != null
                            ? shardTask.getMeasurement(MetricType.LATENCY).longValue()
                            : 0L,
                        shardTask.getMeasurement(MetricType.CPU) != null ? shardTask.getMeasurement(MetricType.CPU).longValue() : 0L,
                        shardTask.getMeasurement(MetricType.MEMORY) != null ? shardTask.getMeasurement(MetricType.MEMORY).longValue() : 0L,
                        (String) shardTask.getAttributes().get(Attribute.PARENT_TASK_ID)
                    );
                    parent.addShardTask(shardRecord);
                }
            }
        }

        // Replace original responses with grouped results
        List<LiveQueriesNodeResponse> groupedResponses = new ArrayList<>();
        if (!parentTasks.isEmpty() && !responses.isEmpty()) {
            // Use first node to represent all grouped results
            groupedResponses.add(new LiveQueriesNodeResponse(responses.get(0).getNode(), new ArrayList<>(parentTasks.values())));
        }
        return new LiveQueriesResponse(clusterService.getClusterName(), groupedResponses, failures);
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
        List<SearchQueryRecord> localTasks = getLocalTasks(request.getRequest());
        return new LiveQueriesNodeResponse(transportService.getLocalNode(), localTasks);
    }

    private List<SearchQueryRecord> getLocalTasks(LiveQueriesRequest request) {
        Map<Long, Task> allTasks = transportService.getTaskManager().getTasks();
        List<SearchQueryRecord> allRecords = new ArrayList<>();

        for (Task runningTask : allTasks.values()) {
            if (!isSearchTask(runningTask)) {
                continue;
            }

            SearchQueryRecord record = createSearchQueryRecord(runningTask, request);
            if (record != null) {
                allRecords.add(record);
            }
        }

        return allRecords;
    }

    private boolean isSearchTask(Task task) {
        return task.getAction().equals("indices:data/read/search") || task.getAction().startsWith("indices:data/read/search[phase/");
    }

    private SearchQueryRecord createSearchQueryRecord(Task runningTask, LiveQueriesRequest request) {
        String wlmGroupId = getWlmGroupId(runningTask);
        if (!isWlmGroupMatched(wlmGroupId, request.getWlmGroupId())) {
            return null;
        }

        Map<MetricType, Measurement> measurements = createMeasurements(runningTask);
        Map<Attribute, Object> attributes = createAttributes(runningTask, request, wlmGroupId);

        return new SearchQueryRecord(runningTask.getStartTime(), measurements, attributes, null, null, String.valueOf(runningTask.getId()));
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

    private Map<MetricType, Measurement> createMeasurements(Task runningTask) {
        long runningNanos = System.nanoTime() - runningTask.getStartTimeNanos();
        long cpuNanos = 0L;
        long memBytes = 0L;

        try {
            var totalResourceUsage = runningTask.getTotalResourceStats();
            if (totalResourceUsage != null) {
                cpuNanos = totalResourceUsage.getCpuTimeInNanos();
                memBytes = totalResourceUsage.getMemoryInBytes();
            }
        } catch (Exception e) {
            // Resource stats unavailable - using zero values
        }

        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(runningNanos));
        measurements.put(MetricType.CPU, new Measurement(cpuNanos));
        measurements.put(MetricType.MEMORY, new Measurement(memBytes));
        return measurements;
    }

    private Map<Attribute, Object> createAttributes(Task runningTask, LiveQueriesRequest request, String wlmGroupId) {
        Map<Attribute, Object> attributes = new HashMap<>();
        String nodeId = transportService.getLocalNode().getId();
        String parentTaskId = runningTask.getParentTaskId() != null && runningTask.getParentTaskId().getId() != -1
            ? runningTask.getParentTaskId().toString()
            : null;

        attributes.put(Attribute.NODE_ID, nodeId);
        attributes.put(Attribute.WLM_GROUP_ID, wlmGroupId);
        attributes.put(Attribute.DESCRIPTION, runningTask.getDescription());
        attributes.put(Attribute.LABELS, Map.of("action", runningTask.getAction()));

        if (parentTaskId != null) {
            attributes.put(Attribute.PARENT_TASK_ID, parentTaskId);
        }

        if (runningTask.getAction().startsWith("indices:data/read/search[phase/")) {
            String[] indices = extractIndicesFromDescription(runningTask.getDescription());
            attributes.put(Attribute.INDICES, indices.length > 0 ? indices : new String[] { "unknown" });
        }

        return attributes;
    }

    private String[] extractIndicesFromDescription(String description) {
        if (description != null && description.startsWith("indices[")) {
            int start = description.indexOf("indices[") + 8;
            int end = description.indexOf("]", start);
            if (end > start) {
                String indicesStr = description.substring(start, end);
                if (!indicesStr.isEmpty()) {
                    // Clean up the indices string - remove brackets and split by comma
                    return indicesStr.replaceAll("[\\[\\]]", "").split(",");
                }
            }
        }
        return new String[0];
    }

    private String extractShardIdFromDescription(String description) {
        if (description != null && description.startsWith("shardId[")) {
            int start = description.indexOf("shardId[") + 8;
            int end = description.indexOf("]]", start);
            if (end > start) {
                return description.substring(start, end + 2);
            }
        }
        return description != null ? description : "unknown_shard";
    }

}
