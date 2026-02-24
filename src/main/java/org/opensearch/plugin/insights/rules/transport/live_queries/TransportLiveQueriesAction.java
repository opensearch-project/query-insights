/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.resourcetracker.TaskResourceStats;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.plugin.insights.core.service.FinishedQueriesCache;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.FinishedQueryRecord;
import org.opensearch.plugin.insights.rules.model.LiveQueryRecord;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.TaskDetails;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Transport action for fetching ongoing live queries
 */
public class TransportLiveQueriesAction extends HandledTransportAction<LiveQueriesRequest, LiveQueriesResponse> {

    private static final Logger logger = LogManager.getLogger(TransportLiveQueriesAction.class);
    private static final String TOTAL = "total";
    private static final String SEARCH_ACTION = "indices:data/read/search";

    private final Client client;
    private final TransportService transportService;
    private final QueryInsightsService queryInsightsService;

    @Inject
    public TransportLiveQueriesAction(
        final TransportService transportService,
        final Client client,
        final ActionFilters actionFilters,
        final QueryInsightsService queryInsightsService
    ) {
        super(LiveQueriesAction.NAME, transportService, actionFilters, LiveQueriesRequest::new, ThreadPool.Names.GENERIC);
        this.transportService = transportService;
        this.client = client;
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    protected void doExecute(final Task task, final LiveQueriesRequest request, final ActionListener<LiveQueriesResponse> listener) {
        ListTasksRequest listTasksRequest = new ListTasksRequest().setDetailed(request.isVerbose()).setActions("indices:data/read/search*");

        // Set nodes filter if provided in the request
        String[] requestedNodeIds = request.nodesIds();
        if (requestedNodeIds != null && requestedNodeIds.length > 0) {
            listTasksRequest.setNodes(requestedNodeIds);
        }

        // Execute tasks request asynchronously to avoid blocking
        client.admin().cluster().listTasks(listTasksRequest, new ActionListener<ListTasksResponse>() {
            @Override
            public void onResponse(ListTasksResponse taskResponse) {
                try {
                    List<LiveQueryRecord> allRecords = new ArrayList<>();

                    for (TaskGroup taskGroup : taskResponse.getTaskGroups()) {
                        TaskInfo coordinatorInfo = taskGroup.getTaskInfo();
                        String action = coordinatorInfo.getAction();

                        if (!action.equals("indices:data/read/search")) {
                            continue;
                        }

                        String queryId = coordinatorInfo.getTaskId().toString();

                        // Get WLM group ID
                        String wlmGroupId = null;
                        Task runningTask = null;
                        if (transportService.getLocalNode().getId().equals(coordinatorInfo.getTaskId().getNodeId())) {
                            runningTask = transportService.getTaskManager().getTask(coordinatorInfo.getTaskId().getId());
                        }
                        if (runningTask instanceof org.opensearch.wlm.WorkloadGroupTask workloadTask) {
                            wlmGroupId = workloadTask.getWorkloadGroupId();
                        }

                        String targetWlmGroupId = request.getWlmGroupId();
                        if (targetWlmGroupId != null && !targetWlmGroupId.equals(wlmGroupId)) {
                            continue;
                        }

                        // Build coordinator task
                        TaskResourceStats coordStats = coordinatorInfo.getResourceStats();
                        long coordCpu = 0L;
                        long coordMem = 0L;
                        if (coordStats != null) {
                            Map<String, TaskResourceUsage> usageInfo = coordStats.getResourceUsageInfo();
                            if (usageInfo != null) {
                                TaskResourceUsage totalUsage = usageInfo.get(TOTAL);
                                if (totalUsage != null) {
                                    coordCpu = totalUsage.getCpuTimeInNanos();
                                    coordMem = totalUsage.getMemoryInBytes();
                                }
                            }
                        }

                        // Build shard tasks
                        List<TaskDetails> shardTasks = new ArrayList<>();
                        long totalCpu = coordCpu;
                        long totalMem = coordMem;

                        for (TaskGroup childTask : taskGroup.getChildTasks()) {
                            TaskInfo shardInfo = childTask.getTaskInfo();

                            TaskResourceStats shardStats = shardInfo.getResourceStats();
                            long shardCpu = 0L;
                            long shardMem = 0L;
                            if (shardStats != null) {
                                Map<String, TaskResourceUsage> usageInfo = shardStats.getResourceUsageInfo();
                                if (usageInfo != null) {
                                    TaskResourceUsage totalUsage = usageInfo.get(TOTAL);
                                    if (totalUsage != null) {
                                        shardCpu = totalUsage.getCpuTimeInNanos();
                                        shardMem = totalUsage.getMemoryInBytes();
                                    }
                                }
                            }

                            totalCpu += shardCpu;
                            totalMem += shardMem;
                            String shardStatus = shardInfo.isCancelled() ? "cancelled" : "running";
                            shardTasks.add(new TaskDetails(shardInfo, shardStatus));
                        }

                        // Determine status based on coordinator cancellation
                        String queryStatus = coordinatorInfo.isCancelled() ? "cancelled" : "running";

                        LiveQueryRecord record = new LiveQueryRecord(
                            queryId,
                            queryStatus,
                            coordinatorInfo.getStartTime(),
                            wlmGroupId,
                            TimeUnit.NANOSECONDS.toMillis(coordinatorInfo.getRunningTimeNanos()),
                            totalCpu,
                            totalMem,
                            new TaskDetails(coordinatorInfo, queryStatus),
                            shardTasks
                        );

                        allRecords.add(record);
                    }

                    List<LiveQueryRecord> finalRecords = allRecords.stream().sorted((a, b) -> {
                        switch (request.getSortBy()) {
                            case CPU:
                                return Long.compare(b.getTotalCpu(), a.getTotalCpu());
                            case MEMORY:
                                return Long.compare(b.getTotalMemory(), a.getTotalMemory());
                            default:
                                return Long.compare(b.getTotalLatency(), a.getTotalLatency());
                        }
                    }).limit(request.getSize() < 0 ? Long.MAX_VALUE : request.getSize()).toList();

                    List<FinishedQueryRecord> finishedRecords = new ArrayList<>();
                    if (request.isUseFinishedCache()) {
                        FinishedQueriesCache finishedCache = queryInsightsService.getFinishedQueriesCache();
                        if (finishedCache != null) {
                            finishedRecords.addAll(finishedCache.getFinishedQueries(true));
                        }
                    }

                    List<FinishedQueryRecord> finalFinishedRecords = finishedRecords.stream()
                        .sorted((a, b) -> SearchQueryRecord.compare(b, a, request.getSortBy()))
                        .limit(request.getSize() < 0 ? Long.MAX_VALUE : request.getSize())
                        .toList();

                    listener.onResponse(new LiveQueriesResponse(finalRecords, finalFinishedRecords, request.isUseFinishedCache()));
                } catch (Exception ex) {
                    logger.error("Failed to process live queries response", ex);
                    listener.onFailure(ex);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to retrieve live queries", e);
                listener.onFailure(e);
            }
        });
    }
}
