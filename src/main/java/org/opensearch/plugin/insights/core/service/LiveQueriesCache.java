/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.resourcetracker.TaskResourceStats;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.plugin.insights.core.auth.PrincipalExtractor;
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
    private static final int MAX_CACHE_SIZE = 1000;
    private static final int MAX_RETURNED_QUERIES = 100;
    private static final String SEARCH_ACTION = "indices:data/read/search";
    private static final int POLLING_INTERVAL_MS = 500;
    private volatile SearchQueryRecord[] sortedQueries = new SearchQueryRecord[0];
    private final Client client;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private Scheduler.Cancellable pollingTask;

    public LiveQueriesCache(Client client, ThreadPool threadPool, TransportService transportService) {
        this.client = client;
        this.threadPool = threadPool;
        this.transportService = transportService;
    }

    public void start() {
        // Start recurring polling task immediately
        pollingTask = threadPool.scheduleWithFixedDelay(
            this::getRunningTasks,
            new TimeValue(POLLING_INTERVAL_MS, TimeUnit.MILLISECONDS),
            ThreadPool.Names.GENERIC
        );
        // Do initial poll
        getRunningTasks();
    }

    public void stop() {
        if (pollingTask != null) {
            pollingTask.cancel();
        }
    }

    private void getRunningTasks() {
        try {
            // Skip polling if client is not ready
            if (client == null) {
                logger.debug("Client not available, skipping polling");
                return;
            }

            ListTasksRequest request = new ListTasksRequest().setActions(SEARCH_ACTION).setDetailed(true);

            client.admin().cluster().listTasks(request, new ActionListener<ListTasksResponse>() {
                @Override
                public void onResponse(ListTasksResponse response) {
                    try {
                        Map<String, TaskInfo> currentTasks = new HashMap<>();
                        TreeSet<SearchQueryRecord> topQueries = new TreeSet<>((a, b) -> {
                            int latencyCompare = Long.compare(
                                ((Number) b.getMeasurement(MetricType.LATENCY)).longValue(),
                                ((Number) a.getMeasurement(MetricType.LATENCY)).longValue()
                            );
                            return latencyCompare != 0 ? latencyCompare : a.hashCode() - b.hashCode();
                        });

                        int searchTasks = 0;
                        for (TaskInfo task : response.getTasks()) {
                            if (task.getAction().equals(SEARCH_ACTION)) {
                                searchTasks++;
                                currentTasks.put(task.getTaskId().toString(), task);
                                SearchQueryRecord record = createRecordFromTask(task);
                                topQueries.add(record);
                                if (topQueries.size() > MAX_CACHE_SIZE) {
                                    topQueries.pollLast();
                                }
                            }
                        }

                        logger.debug("LiveQueriesCache found {} search tasks", searchTasks);

                        sortedQueries = topQueries.stream().limit(MAX_RETURNED_QUERIES).toArray(SearchQueryRecord[]::new);
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

        // Always include description and cancellation status
        attributes.put(Attribute.DESCRIPTION, task.getDescription());
        attributes.put(Attribute.IS_CANCELLED, task.isCancelled());

        // Add task resource usage info
        if (stats != null) {
            List<org.opensearch.core.tasks.resourcetracker.TaskResourceInfo> taskResourceUsages = new ArrayList<>();
            taskResourceUsages.add(
                new org.opensearch.core.tasks.resourcetracker.TaskResourceInfo(
                    task.getAction(),
                    task.getId(),
                    task.getParentTaskId().getId(),
                    task.getTaskId().getNodeId(),
                    stats.getResourceUsageInfo().get("total")
                )
            );
            attributes.put(Attribute.TASK_RESOURCE_USAGES, taskResourceUsages);
        }

        // Always include WLM group ID (even if null)
        attributes.put(Attribute.WLM_GROUP_ID, workloadGroup);

        // Add user information if available using PrincipalExtractor (consistent with TopN queries)
        try {
            if (transportService != null) {
                PrincipalExtractor principalExtractor = new PrincipalExtractor(transportService.getThreadPool());
                PrincipalExtractor.UserPrincipalInfo userInfo = principalExtractor.extractUserInfo();
                if (userInfo != null) {
                    if (userInfo.getUserName() != null) {
                        attributes.put(Attribute.USERNAME, userInfo.getUserName());
                    }
                    if (userInfo.getRoles() != null && !userInfo.getRoles().isEmpty()) {
                        attributes.put(Attribute.USER_ROLES, userInfo.getRoles().toArray(new String[0]));
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to extract user info for task {}: {}", task.getTaskId(), e.getMessage());
        }

        return new SearchQueryRecord(task.getStartTime(), measurements, attributes, task.getTaskId().toString());
    }

    public List<SearchQueryRecord> getCurrentQueries() {
        logger.debug("LiveQueriesCache returning {} cached queries", sortedQueries.length);
        return Arrays.asList(sortedQueries);
    }

}
