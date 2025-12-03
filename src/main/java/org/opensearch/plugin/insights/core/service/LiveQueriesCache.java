/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.service;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * Cache for live running queries
 */
public class LiveQueriesCache {
    
    private static final int MAX_CACHE_SIZE = 10000;
    private final ConcurrentHashMap<String, SearchQueryRecord> runningQueries = new ConcurrentHashMap<>();
    private final Client client;
    private final ThreadPool threadPool;
    private org.opensearch.threadpool.Scheduler.Cancellable pollingTask;

    public LiveQueriesCache(Client client, ThreadPool threadPool) {
        this.client = client;
        this.threadPool = threadPool;
    }

    public void start() {
        // Add initial delay to allow cluster initialization, then poll every 10ms
        pollingTask = threadPool.schedule(this::startPolling, 
            new org.opensearch.common.unit.TimeValue(5, TimeUnit.SECONDS), 
            ThreadPool.Names.GENERIC);
    }

    private void startPolling() {
        try {
            // Check if cluster is ready before starting polling
            client.admin().cluster().listTasks(new org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest(), 
                new org.opensearch.core.action.ActionListener<org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse>() {
                    @Override
                    public void onResponse(org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse response) {
                        // Cluster is ready, start polling
                        pollingTask = threadPool.scheduleWithFixedDelay(LiveQueriesCache.this::pollRunningTasks, 
                            new org.opensearch.common.unit.TimeValue(10, TimeUnit.MILLISECONDS), 
                            ThreadPool.Names.GENERIC);
                    }
                    
                    @Override
                    public void onFailure(Exception e) {
                        // Retry after 1 second if cluster not ready
                        pollingTask = threadPool.schedule(LiveQueriesCache.this::startPolling, 
                            new org.opensearch.common.unit.TimeValue(1, TimeUnit.SECONDS), 
                            ThreadPool.Names.GENERIC);
                    }
                });
        } catch (Exception e) {
            // Retry after 1 second if any error
            pollingTask = threadPool.schedule(this::startPolling, 
                new org.opensearch.common.unit.TimeValue(1, TimeUnit.SECONDS), 
                ThreadPool.Names.GENERIC);
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
                        // Silent failure - don't crash
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // Silent failure to avoid log spam during initialization
                }
            });
        } catch (Throwable e) {
            // Silent failure - catch all exceptions to prevent crashes
        }
    }

    private SearchQueryRecord createRecordFromTask(TaskInfo task) {
        java.util.Map<org.opensearch.plugin.insights.rules.model.MetricType, 
                     org.opensearch.plugin.insights.rules.model.Measurement> measurements = new java.util.HashMap<>();
        
        long runningTime = System.currentTimeMillis() - task.getStartTime();
        measurements.put(org.opensearch.plugin.insights.rules.model.MetricType.LATENCY, 
                        new org.opensearch.plugin.insights.rules.model.Measurement(runningTime));

        java.util.Map<org.opensearch.plugin.insights.rules.model.Attribute, Object> attributes = new java.util.HashMap<>();
        attributes.put(org.opensearch.plugin.insights.rules.model.Attribute.NODE_ID, task.getTaskId().getNodeId());
        
        return new SearchQueryRecord(task.getStartTime(), measurements, attributes, String.valueOf(task.getId()));
    }

    public List<SearchQueryRecord> getCurrentQueries() {
        return new CopyOnWriteArrayList<>(runningQueries.values());
    }
}