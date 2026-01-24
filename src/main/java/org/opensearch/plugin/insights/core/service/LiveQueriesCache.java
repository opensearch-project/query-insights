/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.LIVE_QUERIES_MAX_CACHE_SIZE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.LIVE_QUERIES_MAX_RETURNED_QUERIES;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.LIVE_QUERIES_POLLING_INTERVAL_MS;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Cache for live running queries
 */
public class LiveQueriesCache {

    private static final Logger logger = LogManager.getLogger(LiveQueriesCache.class);
    private static final String SEARCH_ACTION = "indices:data/read/search*";
    private static final AtomicReference<LiveQueriesCache> INSTANCE = new AtomicReference<>();

    private final AtomicReference<SearchQueryRecord[]> sortedQueries = new AtomicReference<>(new SearchQueryRecord[0]);
    private final Client client;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private volatile Scheduler.Cancellable pollingTask;
    private final Map<String, Map<String, Object>> taskUserInfoCache = new ConcurrentHashMap<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicLong lastAccessTime = new AtomicLong(0);
    private final AtomicReference<Scheduler.Cancellable> idleCheckTask = new AtomicReference<>();
    private final long idleTimeoutMs;
    private static final TimeValue IDLE_CHECK_INTERVAL = new TimeValue(60, TimeUnit.SECONDS);

    public static LiveQueriesCache getInstance(
        Client client,
        ThreadPool threadPool,
        TransportService transportService,
        long idleTimeoutMs
    ) {
        if (idleTimeoutMs < 0) {
            throw new IllegalStateException(
                "Live queries cache is disabled. Set search.insights.live_queries.cache_timeout to a positive value (2-10 minutes) to enable it."
            );
        }
        LiveQueriesCache cache = INSTANCE.get();
        if (cache == null) {
            LiveQueriesCache newCache = new LiveQueriesCache(client, threadPool, transportService, idleTimeoutMs);
            if (!INSTANCE.compareAndSet(null, newCache)) {
                newCache = INSTANCE.get();
            }
            cache = newCache;
        }
        cache.recordAccess();
        cache.start();
        return cache;
    }

    public static void stopInstance() {
        LiveQueriesCache cache = INSTANCE.get();
        if (cache != null) {
            cache.stop();
        }
    }

    public LiveQueriesCache(Client client, ThreadPool threadPool, TransportService transportService, long idleTimeoutMs) {
        this.client = client;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.idleTimeoutMs = idleTimeoutMs;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        lastAccessTime.set(System.currentTimeMillis());
        pollingTask = threadPool.scheduleWithFixedDelay(
            this::getRunningTasks,
            new TimeValue(LIVE_QUERIES_POLLING_INTERVAL_MS, TimeUnit.MILLISECONDS),
            ThreadPool.Names.GENERIC
        );
        getRunningTasks();
        startIdleCheck();
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (pollingTask != null) {
            pollingTask.cancel();
            pollingTask = null;
        }
        Scheduler.Cancellable task = idleCheckTask.getAndSet(null);
        if (task != null) {
            task.cancel();
        }
        lastAccessTime.set(0);
    }

    public boolean isStarted() {
        return started.get();
    }

    public void recordAccess() {
        lastAccessTime.set(System.currentTimeMillis());
    }

    private void startIdleCheck() {
        if (idleTimeoutMs < 0) {
            return;
        }
        Scheduler.Cancellable task = threadPool.scheduleWithFixedDelay(() -> {
            if (started.get() && System.currentTimeMillis() - lastAccessTime.get() > idleTimeoutMs) {
                stop();
            }
        }, IDLE_CHECK_INTERVAL, ThreadPool.Names.GENERIC);
        idleCheckTask.set(task);
    }

    private void getRunningTasks() {
        try {
            ListTasksRequest request = new ListTasksRequest().setActions(SEARCH_ACTION).setDetailed(true);

            // Call listTasks directly on local node instead of using cluster().listTasks() to avoid fan-out
            DiscoveryNode localNode = transportService.getLocalNode();
            if (localNode != null) {
                request.setNodes(new String[] { localNode.getId() });

                client.admin().cluster().listTasks(request, new ActionListener<ListTasksResponse>() {
                    @Override
                    public void onResponse(ListTasksResponse response) {
                        try {
                            Map<String, SearchQueryRecord> parentRecords = LiveQueriesHelper.processTaskGroups(
                                response.getTaskGroups(),
                                transportService,
                                true,
                                taskUserInfoCache,
                                null,
                                null
                            );

                            // Clean up user info cache for tasks that are no longer running
                            Set<String> activeTaskIds = parentRecords.keySet();
                            taskUserInfoCache.keySet().removeIf(taskId -> !activeTaskIds.contains(taskId));

                            TreeSet<SearchQueryRecord> allQueries = new TreeSet<>((a, b) -> {
                                int latencyCompare = Long.compare(
                                    ((Number) b.getMeasurement(MetricType.LATENCY)).longValue(),
                                    ((Number) a.getMeasurement(MetricType.LATENCY)).longValue()
                                );
                                return latencyCompare != 0 ? latencyCompare : a.getId().compareTo(b.getId());
                            });

                            for (SearchQueryRecord record : parentRecords.values()) {
                                allQueries.add(record);
                                if (allQueries.size() > LIVE_QUERIES_MAX_CACHE_SIZE) {
                                    allQueries.pollLast();
                                }
                            }

                            sortedQueries.set(
                                allQueries.stream().limit(LIVE_QUERIES_MAX_RETURNED_QUERIES).toArray(SearchQueryRecord[]::new)
                            );
                        } catch (Throwable e) {
                            logger.error("Error processing live queries response: {}", e.getMessage());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug("Failed to list tasks during polling: {}", e.getMessage());
                    }
                });
            }
        } catch (Throwable e) {
            logger.debug("Error polling running tasks: {}", e.getMessage());
        }
    }

    public List<SearchQueryRecord> getCurrentQueries() {
        return Arrays.asList(sortedQueries.get());
    }

    public void storeTaskUserInfo(String taskId, String username, String[] roles) {
        Map<String, Object> userInfo = new HashMap<>();
        userInfo.put("username", username);
        userInfo.put("roles", roles);
        taskUserInfoCache.put(taskId, userInfo);
    }

    public void removeTaskUserInfo(String taskId) {
        taskUserInfoCache.remove(taskId);
    }

}
