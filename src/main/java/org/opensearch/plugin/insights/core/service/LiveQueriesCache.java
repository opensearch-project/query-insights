/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.resourcetracker.TaskResourceStats;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * Cache for live running queries
 */
public class LiveQueriesCache {

    private static final Logger logger = LogManager.getLogger(LiveQueriesCache.class);
    private static final int MAX_CACHE_SIZE = 10000;
    private final ConcurrentHashMap<String, SearchQueryRecord> runningQueries = new ConcurrentHashMap<>();
    private final Client client;
    private final ThreadPool threadPool;
    private final org.opensearch.transport.TransportService transportService;
    private Scheduler.Cancellable pollingTask;

    public LiveQueriesCache(Client client, ThreadPool threadPool, org.opensearch.transport.TransportService transportService) {
        this.client = client;
        this.threadPool = threadPool;
        this.transportService = transportService;
    }

    public void start() {
        // Add initial delay to allow cluster initialization, then poll every 10ms
        pollingTask = threadPool.schedule(this::startPolling, new TimeValue(5, TimeUnit.SECONDS), ThreadPool.Names.GENERIC);
    }

    private void startPolling() {
        try {
            // Check if cluster is ready before starting polling
            client.admin().cluster().listTasks(new ListTasksRequest(), new ActionListener<ListTasksResponse>() {
                @Override
                public void onResponse(ListTasksResponse response) {
                    // Cluster is ready, start polling
                    pollingTask = threadPool.scheduleWithFixedDelay(
                        LiveQueriesCache.this::pollRunningTasks,
                        new TimeValue(10, TimeUnit.MILLISECONDS),
                        ThreadPool.Names.GENERIC
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    // Retry after 1 second if cluster not ready
                    pollingTask = threadPool.schedule(
                        LiveQueriesCache.this::startPolling,
                        new TimeValue(1, TimeUnit.SECONDS),
                        ThreadPool.Names.GENERIC
                    );
                }
            });
        } catch (Exception e) {
            // Retry after 1 second if any error
            pollingTask = threadPool.schedule(this::startPolling, new TimeValue(1, TimeUnit.SECONDS), ThreadPool.Names.GENERIC);
        }
    }

    public void stop() {
        if (pollingTask != null) {
            pollingTask.cancel();
        }
    }

    private void pollRunningTasks() {
        try {
            ListTasksRequest request = new ListTasksRequest();
            request.setActions("indices:data/read/search*");

            client.admin().cluster().listTasks(request, new ActionListener<ListTasksResponse>() {
                @Override
                public void onResponse(ListTasksResponse response) {
                    try {
                        runningQueries.clear();
                        int count = 0;
                        for (TaskInfo task : response.getTasks()) {
                            if (count >= MAX_CACHE_SIZE) break;
                            SearchQueryRecord record = createRecordFromTask(task);
                            runningQueries.put(String.valueOf(task.getId()), record);
                            count++;
                        }
                    } catch (Throwable e) {
                        logger.debug("Error processing live queries response: {}", e.getMessage());
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug("Failed to list tasks during polling: {}", e.getMessage());
                }
            });
        } catch (Throwable e) {
            logger.debug("Error polling running tasks: {}", e.getMessage());
        }
    }

    private SearchQueryRecord createRecordFromTask(TaskInfo task) {
        Map<MetricType, Measurement> measurements = new HashMap<>();

        long runningNanos = task.getRunningTimeNanos();
        measurements.put(MetricType.LATENCY, new Measurement(runningNanos));

        long cpuNanos = 0L;
        long memBytes = 0L;
        TaskResourceStats stats = task.getResourceStats();
        if (stats != null) {
            Map<String, TaskResourceUsage> usageInfo = stats.getResourceUsageInfo();
            if (usageInfo != null) {
                TaskResourceUsage totalUsage = usageInfo.get("total");
                if (totalUsage != null) {
                    cpuNanos = totalUsage.getCpuTimeInNanos();
                    memBytes = totalUsage.getMemoryInBytes();
                }
            }
        }
        measurements.put(MetricType.CPU, new Measurement(cpuNanos));
        measurements.put(MetricType.MEMORY, new Measurement(memBytes));

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.NODE_ID, task.getTaskId().getNodeId());
        attributes.put(Attribute.DESCRIPTION, task.getDescription());
        attributes.put(Attribute.IS_CANCELLED, task.isCancelled());

        String wlmGroupId = null;
        if (transportService.getLocalNode().getId().equals(task.getTaskId().getNodeId())) {
            org.opensearch.tasks.Task runningTask = transportService.getTaskManager().getTask(task.getTaskId().getId());
            if (runningTask instanceof org.opensearch.wlm.WorkloadGroupTask workloadTask) {
                wlmGroupId = workloadTask.getWorkloadGroupId();
            }
        }
        attributes.put(Attribute.WLM_GROUP_ID, wlmGroupId);

        return new SearchQueryRecord(task.getStartTime(), measurements, attributes, task.getTaskId().toString());
    }

    public List<SearchQueryRecord> getCurrentQueries() {
        return new CopyOnWriteArrayList<>(runningQueries.values());
    }
}
