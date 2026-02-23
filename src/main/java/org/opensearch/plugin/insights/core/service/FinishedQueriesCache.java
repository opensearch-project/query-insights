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
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Cache for recently finished queries
 */
public class FinishedQueriesCache {

    private static final int MAX_FINISHED_QUERIES = 1000;
    private static final int MAX_RETURNED_QUERIES = 50;
    private static final long RETENTION_MS = TimeUnit.MINUTES.toMillis(5);

    private final ArrayDeque<FinishedQuery> finishedQueries = new ArrayDeque<>();
    private volatile long lastAccessTime = 0;
    private volatile long idleTimeoutMs;
    private final ClusterService clusterService;

    public FinishedQueriesCache(ClusterService clusterService) {
        this.idleTimeoutMs = clusterService.getClusterSettings()
            .get(org.opensearch.plugin.insights.settings.QueryInsightsSettings.LIVE_QUERIES_CACHE_IDLE_TIMEOUT)
            .millis();
        this.clusterService = clusterService;
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
        while (!finishedQueries.isEmpty() && currentTime - finishedQueries.peekFirst().timestamp > RETENTION_MS) {
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

    public void setIdleTimeout(long idleTimeoutMs) {
        this.idleTimeoutMs = idleTimeoutMs;
    }

    public void clear() {
        synchronized (finishedQueries) {
            finishedQueries.clear();
        }
    }

    public void captureQuery(SearchPhaseContext context, SearchRequestContext searchRequestContext, String topNId, boolean failed) {
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
        String liveQueryId = nodeId + ":" + taskId;
        boolean isCancelled = context.getTask().isCancelled();
        String status = isCancelled ? "cancelled" : (failed ? "failed" : "completed");
        String wlmGroupId = null;

        if (context.getTask() instanceof org.opensearch.wlm.WorkloadGroupTask workloadTask) {
            wlmGroupId = workloadTask.getWorkloadGroupId();
        }

        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(latencyMs));
        measurements.put(MetricType.CPU, new Measurement(cpuNanos));
        measurements.put(MetricType.MEMORY, new Measurement(memBytes));

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.NODE_ID, nodeId);
        attributes.put(Attribute.IS_CANCELLED, isCancelled);
        attributes.put(Attribute.FAILED, failed);
        attributes.put(Attribute.DESCRIPTION, context.getRequest().source() != null ? context.getRequest().source().toString() : "");
        if (wlmGroupId != null) attributes.put(Attribute.WLM_GROUP_ID, wlmGroupId);
        if (topNId != null) attributes.put(Attribute.TOP_N_ID, topNId);
        attributes.put(Attribute.STATUS, status);

        addFinishedQuery(new SearchQueryRecord(
            context.getRequest().getOrCreateAbsoluteStartMillis(),
            measurements,
            attributes,
            liveQueryId
        ));
    }
}
