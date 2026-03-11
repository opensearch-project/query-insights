/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;
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
 * Concurrency:
 * - capture() is fully lock-free: AtomicBoolean.active + ConcurrentLinkedDeque.
 * - getFinishedQueries() uses CAS on active to activate exactly once and
 *   AtomicReference to register the idle-check task without locks.
 * - stop() uses CAS to deactivate and AtomicReference.getAndSet to cancel the task.
 */
public class FinishedQueriesCache {

    private static final int MAX_FINISHED_QUERIES = 1000;
    private static final int MAX_RETURNED_QUERIES = 50;
    private static final long RETENTION_MS = TimeUnit.MINUTES.toMillis(5);

    private final ConcurrentLinkedDeque<FinishedQuery> finishedQueries = new ConcurrentLinkedDeque<>();
    private volatile long lastAccessTime;
    private volatile long idleTimeoutMs;
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final AtomicReference<Scheduler.Cancellable> idleCheckTask = new AtomicReference<>();
    private final AtomicInteger size = new AtomicInteger(0);
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    /**
     * Constructor for FinishedQueriesCache.
     *
     * @param clusterService the cluster service for settings and node info
     * @param threadPool the thread pool for scheduling the idle-check task
     */
    public FinishedQueriesCache(ClusterService clusterService, ThreadPool threadPool) {
        this.idleTimeoutMs = clusterService.getClusterSettings().get(QueryInsightsSettings.LIVE_QUERIES_CACHE_IDLE_TIMEOUT).millis();
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * Returns the most recent finished queries, activating the cache on first call.
     * The cache auto-deactivates after idleTimeoutMs of no API access.
     * Returns an empty list if the cache is disabled (idleTimeoutMs == 0).
     *
     * @return list of up to MAX_RETURNED_QUERIES most recent finished query records
     */
    public List<FinishedQueryRecord> getFinishedQueries() {
        if (idleTimeoutMs == 0) return List.of();

        // Activate exactly once via CAS — only the winning thread schedules the idle check
        if (active.compareAndSet(false, true)) {
            lastAccessTime = System.currentTimeMillis();
            long intervalMs = idleTimeoutMs / 4;
            Scheduler.Cancellable task = threadPool.scheduleWithFixedDelay(() -> {
                if (isExpired() && active.compareAndSet(true, false)) {
                    Scheduler.Cancellable t = idleCheckTask.getAndSet(null);
                    finishedQueries.clear();
                    size.set(0);
                    if (t != null) t.cancel();
                }
            }, TimeValue.timeValueMillis(intervalMs), QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR);

            if (!idleCheckTask.compareAndSet(null, task)) {
                // stop() already ran — cancel the task we just scheduled
                task.cancel();
            }
        }

        lastAccessTime = System.currentTimeMillis();

        removeExpiredQueries();
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(finishedQueries.descendingIterator(), Spliterator.ORDERED), false)
            .limit(MAX_RETURNED_QUERIES).map(fq -> fq.record).toList();
    }

    /**
     * Captures a finished query into the cache.
     * This method is fully lock-free and is safe to call on the hot search path.
     * No-op if the cache is disabled (idleTimeoutMs == 0) or not yet activated by an API request.
     *
     * @param record            the completed search query record
     * @param coordinatorTaskId the task ID of the coordinating search task
     */
    public void capture(SearchQueryRecord record, long coordinatorTaskId) {
        if (idleTimeoutMs == 0 || !active.get()) return;

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
            TopQueriesService.setSourceAndTruncation(record, QueryInsightsSettings.DEFAULT_MAX_SOURCE_LENGTH);
        }
        boolean failed = (Boolean) record.getAttributes().getOrDefault(Attribute.FAILED, false);
        boolean cancelled = record.isCancelled();
        String status = cancelled ? "cancelled" : (failed ? "failed" : "completed");

        finishedQueries.addLast(new FinishedQuery(new FinishedQueryRecord(record, record.getId(), status, liveQueryId)));
        if (size.incrementAndGet() > MAX_FINISHED_QUERIES) {
            finishedQueries.pollFirst();
            size.decrementAndGet();
        }
    }

    /**
     * Returns finished queries only if the cache is already active, without activating it.
     * Used by fan-out nodes to avoid activating the cache cluster-wide on every API call.
     *
     * @return list of most recent finished query records, or empty list if cache is inactive
     */
    public List<FinishedQueryRecord> getFinishedQueriesIfActive() {
        if (!active.get()) return List.of();
        removeExpiredQueries();
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(finishedQueries.descendingIterator(), Spliterator.ORDERED), false)
            .limit(MAX_RETURNED_QUERIES).map(fq -> fq.record).toList();
    }

    /**
     * Deactivates the cache, cancels the idle-check task, and clears all stored queries.
     * Called by QueryInsightsService.doStop().
     */
    public void stop() {
        if (active.compareAndSet(true, false)) {
            Scheduler.Cancellable task = idleCheckTask.getAndSet(null);
            finishedQueries.clear();
            size.set(0);
            if (task != null) task.cancel();
        }
    }

    /**
     * Removes queries older than RETENTION_MS from the head of the deque.
     */
    private void removeExpiredQueries() {
        long currentTime = System.currentTimeMillis();
        FinishedQuery head;
        while ((head = finishedQueries.peekFirst()) != null && currentTime - head.timestamp > RETENTION_MS) {
            finishedQueries.pollFirst();
            size.decrementAndGet();
        }
    }

    /**
     * Returns true if the cache has been idle (no API access) for longer than idleTimeoutMs.
     */
    private boolean isExpired() {
        return System.currentTimeMillis() - lastAccessTime > idleTimeoutMs;
    }

    /**
     * Updates the idle timeout. Setting to 0 disables the cache entirely and stops it immediately.
     *
     * @param idleTimeoutMs new idle timeout in milliseconds, or 0 to disable
     */
    public void setIdleTimeout(long idleTimeoutMs) {
        this.idleTimeoutMs = idleTimeoutMs;
        if (idleTimeoutMs == 0) stop();
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
