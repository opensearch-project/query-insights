/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.FinishedQueryRecord;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

/**
 * Self-contained cache for recently finished queries.
 * Manages its own lifecycle: lazy start via first write, auto-stop via idle timeout.
 */
public class FinishedQueriesCache {

    private static final int MAX_FINISHED_QUERIES = 1000;
    private static final int MAX_RETURNED_QUERIES = 50;
    private static final long RETENTION_MS = TimeUnit.MINUTES.toMillis(5);

    private final ArrayDeque<FinishedQuery> finishedQueries = new ArrayDeque<>();
    private volatile long lastAccessTime;
    private volatile long idleTimeoutMs;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable idleCheckTask;
    private volatile boolean started = false;

    public FinishedQueriesCache(ClusterService clusterService, ThreadPool threadPool) {
        this.idleTimeoutMs = clusterService.getClusterSettings().get(QueryInsightsSettings.LIVE_QUERIES_CACHE_IDLE_TIMEOUT).millis();
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.lastAccessTime = System.currentTimeMillis();
    }

    private void startIdleCheck() {
        idleCheckTask = threadPool.scheduleWithFixedDelay(() -> {
            if (isExpired()) {
                stop();
                if (onExpired != null) onExpired.run();
            }
        }, TimeValue.timeValueMinutes(1), QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR);
    }

    private Runnable onExpired;

    public void setOnExpired(Runnable onExpired) {
        this.onExpired = onExpired;
    }

    public void stop() {
        if (idleCheckTask != null) {
            idleCheckTask.cancel();
            idleCheckTask = null;
        }
        clear();
    }

    private static class FinishedQuery {
        final FinishedQueryRecord record;
        final long timestamp;

        FinishedQuery(FinishedQueryRecord record) {
            this.record = record;
            this.timestamp = System.currentTimeMillis();
        }
    }

    public void capture(SearchQueryRecord record, boolean failed, boolean cancelled, long coordinatorTaskId) {
        String liveQueryId = clusterService.localNode().getId() + ":" + coordinatorTaskId;
        Object taskUsages = record.getAttributes().get(Attribute.TASK_RESOURCE_USAGES);
        if (taskUsages instanceof List) {
            for (Object t : (List<?>) taskUsages) {
                if (t instanceof TaskResourceInfo info && info.getParentTaskId() == -1) {
                    liveQueryId = info.getNodeId() + ":" + info.getTaskId();
                    break;
                }
            }
        }
        if (record.getAttributes().get(Attribute.SOURCE) == null && record.getSearchSourceBuilder() != null) {
            TopQueriesService.setSourceAndTruncation(record, Integer.MAX_VALUE);
        }
        String status = cancelled ? "cancelled" : (failed ? "failed" : "completed");
        addFinishedQuery(new FinishedQueryRecord(record, record.getId(), status, liveQueryId));
    }

    public void addFinishedQuery(FinishedQueryRecord record) {
        synchronized (finishedQueries) {
            if (!started) {
                started = true;
                startIdleCheck();
            }
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
        if (idleTimeoutMs == 0) return false;
        return System.currentTimeMillis() - lastAccessTime > idleTimeoutMs;
    }

    public List<FinishedQueryRecord> getFinishedQueries() {
        synchronized (finishedQueries) {
            lastAccessTime = System.currentTimeMillis();
            removeExpiredQueries();
            return finishedQueries.stream().limit(MAX_RETURNED_QUERIES).map(fq -> fq.record).toList();
        }
    }

    public int size() {
        synchronized (finishedQueries) {
            return finishedQueries.size();
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
}
