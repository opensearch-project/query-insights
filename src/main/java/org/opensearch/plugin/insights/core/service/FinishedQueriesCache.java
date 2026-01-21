/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.service;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.plugin.insights.core.auth.PrincipalExtractor;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.threadpool.ThreadPool;

/**
 * Cache for recently finished queries
 */
public class FinishedQueriesCache {

    private static final int MAX_FINISHED_QUERIES = 1000;
    private static final int MAX_RETURNED_QUERIES = 50;

    private final ArrayDeque<FinishedQuery> finishedQueries = new ArrayDeque<>();
    private volatile long lastAccessTime = 0;
    private volatile long retentionPeriodMs;
    private volatile long idleTimeoutMs;
    private final ClusterService clusterService;
    private final PrincipalExtractor principalExtractor;

    public FinishedQueriesCache(long retentionPeriodMs, ClusterService clusterService, ThreadPool threadPool) {
        this.retentionPeriodMs = retentionPeriodMs;
        this.idleTimeoutMs = clusterService.getClusterSettings()
            .get(org.opensearch.plugin.insights.settings.QueryInsightsSettings.LIVE_QUERIES_CACHE_IDLE_TIMEOUT)
            .millis();
        this.clusterService = clusterService;
        this.principalExtractor = threadPool != null ? new PrincipalExtractor(threadPool) : null;
        this.lastAccessTime = System.currentTimeMillis();
    }

    private static class FinishedQuery {
        final SearchQueryRecord record;
        final long timestamp;

        FinishedQuery(SearchQueryRecord record) {
            this.record = record;
            this.timestamp = System.currentTimeMillis();
        }
    }

    public void addFinishedQuery(SearchQueryRecord record) {
        synchronized (finishedQueries) {
            removeExpiredQueries();
            finishedQueries.addLast(new FinishedQuery(record));
            if (finishedQueries.size() > MAX_FINISHED_QUERIES) {
                finishedQueries.removeFirst();
            }
        }
    }

    private void removeExpiredQueries() {
        long currentTime = System.currentTimeMillis();
        while (!finishedQueries.isEmpty() && currentTime - finishedQueries.peekFirst().timestamp > retentionPeriodMs) {
            finishedQueries.removeFirst();
        }
    }

    public boolean isExpired() {
        if (idleTimeoutMs == -1) {
            return false;
        }
        return System.currentTimeMillis() - lastAccessTime > idleTimeoutMs;
    }

    public List<SearchQueryRecord> getFinishedQueries(boolean enableListener) {
        synchronized (finishedQueries) {
            lastAccessTime = System.currentTimeMillis();
            removeExpiredQueries();
            return finishedQueries.stream().limit(MAX_RETURNED_QUERIES).map(fq -> fq.record).toList();
        }
    }

    public void setRetentionPeriod(long retentionPeriodMs) {
        this.retentionPeriodMs = retentionPeriodMs;
    }

    public void setIdleTimeout(long idleTimeoutMs) {
        this.idleTimeoutMs = idleTimeoutMs;
    }

    public void clear() {
        synchronized (finishedQueries) {
            finishedQueries.clear();
        }
    }

    public void captureQuery(SearchPhaseContext context, SearchRequestContext searchRequestContext, String recordId) {
        List<TaskResourceInfo> tasksResourceUsages = searchRequestContext.getPhaseResourceUsage();
        long cpuNanos = 0L;
        long memBytes = 0L;
        long taskId = context.getTask().getId();
        String nodeId = clusterService.localNode().getId();

        for (TaskResourceInfo taskInfo : tasksResourceUsages) {
            cpuNanos += taskInfo.getTaskResourceUsage().getCpuTimeInNanos();
            memBytes += taskInfo.getTaskResourceUsage().getMemoryInBytes();
            if (taskInfo.getParentTaskId() == -1) {
                taskId = taskInfo.getTaskId();
                nodeId = taskInfo.getNodeId();
            }
        }

        long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchRequestContext.getAbsoluteStartNanos());
        String taskIdStr = nodeId + ":" + taskId;
        String description = "[task_id="
            + taskIdStr
            + "] "
            + (context.getRequest().source() != null ? context.getRequest().source().toString() : "");
        boolean isCancelled = context.getTask().isCancelled();
        String wlmGroupId = null;
        String username = null;
        String[] userRoles = null;

        if (context.getTask() instanceof org.opensearch.wlm.WorkloadGroupTask workloadTask) {
            wlmGroupId = workloadTask.getWorkloadGroupId();
        }

        if (principalExtractor != null) {
            try {
                PrincipalExtractor.UserPrincipalInfo userInfo = principalExtractor.extractUserInfo();
                if (userInfo != null) {
                    username = userInfo.getUserName();
                    if (userInfo.getRoles() != null && !userInfo.getRoles().isEmpty()) {
                        userRoles = userInfo.getRoles().toArray(new String[0]);
                    }
                }
            } catch (Exception e) {
                // Ignore
            }
        }

        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(latencyMs));
        measurements.put(MetricType.CPU, new Measurement(cpuNanos));
        measurements.put(MetricType.MEMORY, new Measurement(memBytes));

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.NODE_ID, clusterService.localNode().getId());
        attributes.put(Attribute.DESCRIPTION, description);
        attributes.put(Attribute.IS_CANCELLED, isCancelled);
        if (wlmGroupId != null) attributes.put(Attribute.WLM_GROUP_ID, wlmGroupId);
        if (username != null) attributes.put(Attribute.USERNAME, username);
        if (userRoles != null) attributes.put(Attribute.USER_ROLES, userRoles);

        SearchQueryRecord record = new SearchQueryRecord(
            context.getRequest().getOrCreateAbsoluteStartMillis(),
            measurements,
            attributes,
            recordId
        );

        addFinishedQuery(record);
    }
}
