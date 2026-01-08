/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.resourcetracker.TaskResourceStats;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.plugin.insights.core.service.FinishedQueriesCache;
import org.opensearch.plugin.insights.core.service.LiveQueriesCache;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
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
        // If cached=true, read directly from cache without ListTasks
        if (request.isCached()) {
            try {
                LiveQueriesCache liveCache = queryInsightsService.getLiveQueriesCache();
                List<SearchQueryRecord> liveRecords = liveCache.getCurrentQueries();

                List<SearchQueryRecord> finishedRecords = new ArrayList<>();
                if (request.isIncludeFinished()) {
                    FinishedQueriesCache finishedCache = queryInsightsService.getFinishedQueriesCacheForAPI();
                    if (finishedCache != null) {
                        finishedRecords.addAll(finishedCache.getFinishedQueries(true));
                    }
                }

                // Sort and limit results
                List<SearchQueryRecord> finalLiveRecords = liveRecords.stream()
                    .sorted((a, b) -> SearchQueryRecord.compare(b, a, request.getSortBy()))
                    .limit(request.getSize() < 0 ? Long.MAX_VALUE : request.getSize())
                    .toList();

                List<SearchQueryRecord> finalFinishedRecords = finishedRecords.stream()
                    .sorted((a, b) -> SearchQueryRecord.compare(b, a, request.getSortBy()))
                    .limit(request.getSize() < 0 ? Long.MAX_VALUE : request.getSize())
                    .toList();

                listener.onResponse(new LiveQueriesResponse(finalLiveRecords, finalFinishedRecords, request.isIncludeFinished()));
            } catch (Exception ex) {
                logger.error("Failed to retrieve cached queries", ex);
                listener.onFailure(ex);
            }
            return;
        }

        // cached=false: Use ListTasks path
        ListTasksRequest listTasksRequest = new ListTasksRequest().setDetailed(true).setActions(SEARCH_ACTION);

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
                    List<SearchQueryRecord> allFilteredRecords = new ArrayList<>();
                    for (TaskInfo taskInfo : taskResponse.getTasks()) {
                        if (!taskInfo.getAction().equals(SEARCH_ACTION)) {
                            continue;
                        }
                        long timestamp = taskInfo.getStartTime();
                        String nodeId = taskInfo.getTaskId().getNodeId();
                        long runningNanos = taskInfo.getRunningTimeNanos();

                        Map<MetricType, Measurement> measurements = new HashMap<>();
                        measurements.put(MetricType.LATENCY, new Measurement(runningNanos));

                        long cpuNanos = 0L;
                        long memBytes = 0L;
                        TaskResourceStats stats = taskInfo.getResourceStats();
                        if (stats != null) {
                            Map<String, TaskResourceUsage> usageInfo = stats.getResourceUsageInfo();
                            if (usageInfo != null) {
                                TaskResourceUsage totalUsage = usageInfo.get(TOTAL);
                                if (totalUsage != null) {
                                    cpuNanos = totalUsage.getCpuTimeInNanos();
                                    memBytes = totalUsage.getMemoryInBytes();
                                }
                            }
                        }
                        measurements.put(MetricType.CPU, new Measurement(cpuNanos));
                        measurements.put(MetricType.MEMORY, new Measurement(memBytes));

                        Map<Attribute, Object> attributes = new HashMap<>();
                        attributes.put(Attribute.NODE_ID, nodeId);
                        if (request.isVerbose()) {
                            attributes.put(Attribute.DESCRIPTION, taskInfo.getDescription());
                            attributes.put(Attribute.IS_CANCELLED, taskInfo.isCancelled());
                        }
                        Task runningTask = null;
                        if (transportService.getLocalNode().getId().equals(taskInfo.getTaskId().getNodeId())) {
                            runningTask = transportService.getTaskManager().getTask(taskInfo.getTaskId().getId());
                        }

                        String wlmGroupId = null;
                        if (runningTask instanceof org.opensearch.wlm.WorkloadGroupTask workloadTask) {
                            wlmGroupId = workloadTask.getWorkloadGroupId();
                        }
                        attributes.put(Attribute.WLM_GROUP_ID, wlmGroupId);
                        String targetWlmGroupId = request.getWlmGroupId();
                        if (targetWlmGroupId != null && !targetWlmGroupId.equals(wlmGroupId)) {
                            // skip if this task's wlm group does not match user requested wlm group
                            continue;
                        }
                        String targetTaskId = request.getTaskId();
                        if (targetTaskId != null && !targetTaskId.equals(taskInfo.getTaskId().toString())) {
                            // skip if this task's id does not match user requested task id
                            continue;
                        }
                        SearchQueryRecord record = new SearchQueryRecord(
                            timestamp,
                            measurements,
                            attributes,
                            taskInfo.getTaskId().toString()
                        );

                        allFilteredRecords.add(record);

                        // Add to finished queries cache
                        FinishedQueriesCache cache = queryInsightsService.getFinishedQueriesCache();
                        if (cache != null) {
                            cache.addFinishedQuery(record);
                        }
                    }

                    List<SearchQueryRecord> finishedRecords = new ArrayList<>();
                    if (request.isIncludeFinished()) {
                        FinishedQueriesCache finishedCache = queryInsightsService.getFinishedQueriesCacheForAPI();
                        if (finishedCache != null) {
                            finishedRecords.addAll(finishedCache.getFinishedQueries(true));
                        }
                    }

                    // Sort and limit results
                    List<SearchQueryRecord> finalLiveRecords = allFilteredRecords.stream()
                        .sorted((a, b) -> SearchQueryRecord.compare(b, a, request.getSortBy()))
                        .limit(request.getSize() < 0 ? Long.MAX_VALUE : request.getSize())
                        .toList();

                    List<SearchQueryRecord> finalFinishedRecords = finishedRecords.stream()
                        .sorted((a, b) -> SearchQueryRecord.compare(b, a, request.getSortBy()))
                        .limit(request.getSize() < 0 ? Long.MAX_VALUE : request.getSize())
                        .toList();

                    listener.onResponse(new LiveQueriesResponse(finalLiveRecords, finalFinishedRecords, request.isIncludeFinished()));
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
