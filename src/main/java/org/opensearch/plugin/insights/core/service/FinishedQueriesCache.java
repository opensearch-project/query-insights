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
import java.util.concurrent.locks.ReentrantLock;
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
 * Cache for recently finished queries.
 * Activated on first API read (getFinishedQueries), deactivated on idle timeout.
 *
 * Locking strategy:
 * - All state mutations use a single ReentrantLock.
 * - scheduleWithFixedDelay and task.cancel() are called OUTSIDE the lock
 *   to avoid holding the lock during scheduler interactions.
 * - schedulingInProgress prevents concurrent threads from each scheduling
 *   a duplicate idle-check task during the activation window.
 */
public class FinishedQueriesCache {

    private static final int MAX_FINISHED_QUERIES = 1000;
    private static final int MAX_RETURNED_QUERIES = 50;
    private static final long RETENTION_MS = TimeUnit.MINUTES.toMillis(5);

    private final ArrayDeque<FinishedQuery> finishedQueries = new ArrayDeque<>();
    private long lastAccessTime;
    private volatile long idleTimeoutMs;
    private boolean active = false;
    private boolean schedulingInProgress = false;
    private Scheduler.Cancellable idleCheckTask;
    private final ReentrantLock lock = new ReentrantLock();
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public FinishedQueriesCache(ClusterService clusterService, ThreadPool threadPool) {
        this.idleTimeoutMs = clusterService.getClusterSettings().get(QueryInsightsSettings.LIVE_QUERIES_CACHE_IDLE_TIMEOUT).millis();
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * Activates the cache and returns finished queries.
     * The cache stays active until idle for idleTimeoutMs.
     */
    public List<FinishedQueryRecord> getFinishedQueries() {
        boolean needsSchedule = false;
        long checkIntervalMs = 0;
        List<FinishedQueryRecord> result;

        lock.lock();
        try {
            if (!active) {
                active = true;
                if (!schedulingInProgress && idleCheckTask == null) {
                    schedulingInProgress = true;
                    checkIntervalMs = idleTimeoutMs > 0 ? idleTimeoutMs / 4 : TimeUnit.MINUTES.toMillis(1);
                    needsSchedule = true;
                }
            }
            lastAccessTime = System.currentTimeMillis();
            removeExpiredQueries();
            result = finishedQueries.stream().limit(MAX_RETURNED_QUERIES).map(fq -> fq.record).toList();
        } finally {
            lock.unlock();
        }

        // Schedule outside the lock to avoid holding it during scheduler interaction
        if (needsSchedule) {
            scheduleIdleCheck(checkIntervalMs);
        }
        return result;
    }

    /**
     * Captures a finished query into the cache.
     * No-op if the cache is not active (no client has called getFinishedQueries recently).
     */
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
        // setSourceAndTruncation mutates the record — must be done before acquiring the lock
        // to avoid holding the lock during potentially expensive string operations,
        // but record is not shared after this point so mutation is safe.
        if (record.getAttributes().get(Attribute.SOURCE) == null && record.getSearchSourceBuilder() != null) {
            TopQueriesService.setSourceAndTruncation(record, QueryInsightsSettings.DEFAULT_MAX_SOURCE_LENGTH);
        }
        String status = cancelled ? "cancelled" : (failed ? "failed" : "completed");
        FinishedQueryRecord finished = new FinishedQueryRecord(record, record.getId(), status, liveQueryId);

        lock.lock();
        try {
            if (!active) return;
            lastAccessTime = System.currentTimeMillis();
            removeExpiredQueries();
            finishedQueries.addLast(new FinishedQuery(finished));
            if (finishedQueries.size() > MAX_FINISHED_QUERIES) {
                finishedQueries.removeFirst();
            }
        } finally {
            lock.unlock();
        }
    }

    public void stop() {
        Scheduler.Cancellable task;
        lock.lock();
        try {
            active = false;
            schedulingInProgress = false;
            task = idleCheckTask;
            idleCheckTask = null;
            finishedQueries.clear();
        } finally {
            lock.unlock();
        }
        if (task != null) task.cancel();
    }

    private void scheduleIdleCheck(long intervalMs) {
        Scheduler.Cancellable task = threadPool.scheduleWithFixedDelay(() -> {
            Scheduler.Cancellable toCancel = null;
            lock.lock();
            try {
                if (active && isExpired()) {
                    active = false;
                    schedulingInProgress = false;
                    toCancel = idleCheckTask;
                    idleCheckTask = null;
                    finishedQueries.clear();
                }
            } finally {
                lock.unlock();
            }
            // Cancel outside the lock to avoid deadlock
            if (toCancel != null) toCancel.cancel();
        }, TimeValue.timeValueMillis(intervalMs), QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR);

        lock.lock();
        try {
            if (active) {
                idleCheckTask = task;
            } else {
                // stop() ran between scheduleWithFixedDelay and here
                schedulingInProgress = false;
                task.cancel();
            }
        } finally {
            lock.unlock();
        }
    }

    private void removeExpiredQueries() {
        long currentTime = System.currentTimeMillis();
        while (!finishedQueries.isEmpty() && currentTime - finishedQueries.peekFirst().timestamp > RETENTION_MS) {
            finishedQueries.removeFirst();
        }
    }

    private boolean isExpired() {
        if (idleTimeoutMs == 0) return false;
        return System.currentTimeMillis() - lastAccessTime > idleTimeoutMs;
    }

    public void setIdleTimeout(long idleTimeoutMs) {
        this.idleTimeoutMs = idleTimeoutMs;
    }

    private static class FinishedQuery {
        final FinishedQueryRecord record;
        final long timestamp;

        FinishedQuery(FinishedQueryRecord record) {
            this.record = record;
            this.timestamp = System.currentTimeMillis();
        }
    }
}
