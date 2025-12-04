/*
 * SPDX-License-Identifier: Apache-2.0
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
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesStreamAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesStreamRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesStreamResponse;
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
 * Transport action for streaming live queries
 */
public class TransportLiveQueriesStreamAction extends HandledTransportAction<LiveQueriesStreamRequest, LiveQueriesStreamResponse> {

    private static final Logger logger = LogManager.getLogger(TransportLiveQueriesStreamAction.class);
    private static final String TOTAL = "total";

    private final Client client;
    private final TransportService transportService;

    @Inject
    public TransportLiveQueriesStreamAction(TransportService transportService, Client client, ActionFilters actionFilters) {
        super(LiveQueriesStreamAction.NAME, transportService, actionFilters, LiveQueriesStreamRequest::new, ThreadPool.Names.GENERIC);
        this.client = client;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, LiveQueriesStreamRequest request, ActionListener<LiveQueriesStreamResponse> listener) {
        ListTasksRequest listTasksRequest = new ListTasksRequest().setDetailed(true).setActions("indices:data/read/search");

        client.admin().cluster().listTasks(listTasksRequest, new ActionListener<ListTasksResponse>() {
            @Override
            public void onResponse(ListTasksResponse taskResponse) {
                try {
                    List<SearchQueryRecord> allRecords = new ArrayList<>();
                    for (TaskInfo taskInfo : taskResponse.getTasks()) {
                        if (!taskInfo.getAction().equals("indices:data/read/search")) {
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
                        attributes.put(Attribute.DESCRIPTION, taskInfo.getDescription());
                        attributes.put(Attribute.IS_CANCELLED, taskInfo.isCancelled());

                        String wlmGroupId = null;
                        if (transportService.getLocalNode().getId().equals(taskInfo.getTaskId().getNodeId())) {
                            Task runningTask = transportService.getTaskManager().getTask(taskInfo.getTaskId().getId());
                            if (runningTask instanceof org.opensearch.wlm.WorkloadGroupTask workloadTask) {
                                wlmGroupId = workloadTask.getWorkloadGroupId();
                            }
                        }
                        attributes.put(Attribute.WLM_GROUP_ID, wlmGroupId);

                        SearchQueryRecord record = new SearchQueryRecord(
                            timestamp,
                            measurements,
                            attributes,
                            taskInfo.getTaskId().toString()
                        );

                        allRecords.add(record);
                    }

                    listener.onResponse(new LiveQueriesStreamResponse(allRecords));
                } catch (Exception ex) {
                    logger.error("Failed to process live queries stream response", ex);
                    listener.onFailure(ex);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to retrieve live queries for stream", e);
                listener.onFailure(e);
            }
        });
    }
}
