/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Cache for live running queries
 */
public class LiveQueriesCache {

    private static final Logger logger = LogManager.getLogger(LiveQueriesCache.class);
    private static final int MAX_CACHE_SIZE = 100;
    private static final String SEARCH_ACTION = "indices:data/read/search";
    private volatile SearchQueryRecord[] sortedQueries = new SearchQueryRecord[0];
    private final FinishedQueriesCache finishedQueriesCache;
    private final Client client;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private Scheduler.Cancellable pollingTask;

    public LiveQueriesCache(
        Client client,
        ThreadPool threadPool,
        TransportService transportService,
        FinishedQueriesCache finishedQueriesCache
    ) {
        this.client = client;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.finishedQueriesCache = finishedQueriesCache;
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
                        new TimeValue(100, TimeUnit.MILLISECONDS),
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
            request.setActions(SEARCH_ACTION);

            client.admin().cluster().listTasks(request, new ActionListener<ListTasksResponse>() {
                @Override
                public void onResponse(ListTasksResponse response) {
                    try {
                        logger.info("LiveQueriesCache polling found {} total tasks", response.getTasks().size());
                        Map<String, TaskInfo> currentTasks = new java.util.HashMap<>();
                        java.util.PriorityQueue<SearchQueryRecord> topQueries = new java.util.PriorityQueue<>(
                            MAX_CACHE_SIZE + 1,
                            (a, b) -> Long.compare(
                                ((Number) a.getMeasurement(MetricType.LATENCY)).longValue(),
                                ((Number) b.getMeasurement(MetricType.LATENCY)).longValue()
                            )
                        );

                        int searchTasks = 0;
                        for (TaskInfo task : response.getTasks()) {
                            if (task.getAction().equals(SEARCH_ACTION)) {
                                searchTasks++;
                                currentTasks.put(task.getTaskId().toString(), task);
                                SearchQueryRecord record = createRecordFromTask(task);
                                topQueries.offer(record);
                                if (topQueries.size() > MAX_CACHE_SIZE) {
                                    topQueries.poll();
                                }
                            }
                        }

                        detectFinishedQueries(currentTasks);

                        logger.info("LiveQueriesCache found {} search tasks, keeping top {}", searchTasks, topQueries.size());

                        List<SearchQueryRecord> sorted = new ArrayList<>(topQueries);
                        sorted.sort(
                            (a, b) -> Long.compare(
                                ((Number) b.getMeasurement(MetricType.LATENCY)).longValue(),
                                ((Number) a.getMeasurement(MetricType.LATENCY)).longValue()
                            )
                        );
                        sortedQueries = sorted.toArray(new SearchQueryRecord[0]);
                    } catch (Throwable e) {
                        logger.error("Error processing live queries response: {}", e.getMessage());
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

        String workloadGroup = null;
        // TransportService may be null during initialization, skip workload group detection
        if (transportService != null) {
            try {
                if (transportService.getLocalNode().getId().equals(task.getTaskId().getNodeId())) {
                    Task runningTask = transportService.getTaskManager().getTask(task.getTaskId().getId());
                    if (runningTask instanceof org.opensearch.wlm.WorkloadGroupTask workloadTask) {
                        workloadGroup = workloadTask.getWorkloadGroupId();
                    }
                }
            } catch (Exception e) {
                logger.debug("Failed to get workload group for task {}: {}", task.getTaskId(), e.getMessage());
            }
        }

        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(task.getRunningTimeNanos()));
        measurements.put(MetricType.CPU, new Measurement(cpuNanos));
        measurements.put(MetricType.MEMORY, new Measurement(memBytes));

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.NODE_ID, task.getTaskId().getNodeId());
        if (workloadGroup != null) {
            attributes.put(Attribute.WLM_GROUP_ID, workloadGroup);
        }

        return new SearchQueryRecord(task.getStartTime(), measurements, attributes, task.getTaskId().toString());
    }

    private void detectFinishedQueries(Map<String, TaskInfo> currentTasks) {
        SearchQueryRecord[] previous = sortedQueries;
        for (SearchQueryRecord record : previous) {
            if (!currentTasks.containsKey(record.getId())) {
                finishedQueriesCache.addFinishedQuery(record);
            }
        }
    }

    public List<SearchQueryRecord> getCurrentQueries() {
        logger.info("LiveQueriesCache returning {} cached queries", sortedQueries.length);
        return java.util.Arrays.asList(sortedQueries);
    }

}
