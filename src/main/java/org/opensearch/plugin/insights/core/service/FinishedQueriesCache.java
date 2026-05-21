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
 * The cache is lazy — it does NOT activate at node startup. It activates on the first
 * API read ({@link #getFinishedQueries()}) and auto-deactivates after {@code idleTimeoutMs}
 * of no API access. This means nodes that never use the finished queries feature pay zero cost.
 *
 * Lifecycle:
 * - clearStopped() is called from doStart to allow lazy activation after a stop/start cycle.
 * - getFinishedQueries() activates the cache on first call and schedules the idle timer.
 * - stop() sets the stopped flag, deactivates, and clears data (called from doStop).
 * - Once stopped, getFinishedQueries() cannot reactivate the cache; only clearStopped() can.
 * - The idle timer can deactivate the cache (active=false), but getFinishedQueries()
 *   can re-enable it as long as the cache is not stopped.
 *
 * Concurrency:
 * - capture() is fully lock-free: AtomicBoolean.active + ConcurrentLinkedDeque.
 * - getFinishedQueries() uses CAS on idleCheckTask to schedule the idle-check exactly once.
 * - stop() unconditionally deactivates and clears; safe to call from any thread.
 */
public class FinishedQueriesCache {

    private static final int MAX_FINISHED_QUERIES = 1000;
    private static final int MAX_RETURNED_QUERIES = 50;
    private static final long RETENTION_MS = TimeUnit.MINUTES.toMillis(5);

    private final ConcurrentLinkedDeque<FinishedQuery> finishedQueries = new ConcurrentLinkedDeque<>();
    private volatile long lastAccessTime;
    private volatile long idleTimeoutMs;
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final AtomicReference<Scheduler.Cancellable> idleCheckTask = new AtomicReference<>();
    private final AtomicInteger approximateSize = new AtomicInteger(0);
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
     * Activates the cache so that capture() starts storing queries.
     * NOT called from doStart() — the cache is lazy and only activates on first API call
     * via {@link #getFinishedQueries()}. This method exists for the setIdleTimeout(non-zero)
     * path to re-enable the cache after it was disabled.
     * Clears the stopped flag so the cache can be reactivated after a stop/start cycle.
     * Safe to call multiple times — idempotent via CAS on active.
     */
    public void activate() {
        if (idleTimeoutMs == 0) return;
        stopped.set(false);
        if (active.compareAndSet(false, true)) {
            lastAccessTime = System.currentTimeMillis();
        }
    }

    /**
     * Clears the stopped flag without activating the cache.
     * Called from doStart() so that the cache CAN be lazily activated by the first API call
     * after a stop/start cycle, without eagerly capturing queries at startup.
     */
    public void clearStopped() {
        stopped.set(false);
    }

    /**
     * Returns the most recent finished queries.
     * Re-activates the cache if it was deactivated by the idle timer, making it self-healing:
     * idle timeout → deactivate → next API call → reactivate.
     * Schedules the idle-check timer on first API call (or after reactivation) so the cache
     * auto-deactivates after idleTimeoutMs of no API access.
     * Returns an empty list if the cache is disabled (idleTimeoutMs == 0).
     *
     * @return list of up to MAX_RETURNED_QUERIES most recent finished query records
     */
    public List<FinishedQueryRecord> getFinishedQueries() {
        if (idleTimeoutMs == 0 || stopped.get()) return List.of();

        // Update lastAccessTime BEFORE reactivating so the idle-check timer sees the fresh
        // timestamp and won't immediately deactivate a just-reactivated cache.
        lastAccessTime = System.currentTimeMillis();

        // Re-activate the cache on API call if it was deactivated by the idle timer.
        // This makes the cache self-healing: idle timeout → deactivate → next API call → reactivate.
        // The stopped flag prevents reactivation after service shutdown (doStop → stop()).
        active.set(true);

        // Schedule the idle-check task exactly once on first API call (or after reactivation).
        // Benign race: two threads may both see null and schedule a task, but the CAS
        // on idleCheckTask ensures only one wins — the loser's task is immediately cancelled.
        if (idleCheckTask.get() == null) {
            long intervalMs = idleTimeoutMs / 4;
            Scheduler.Cancellable task = threadPool.scheduleWithFixedDelay(() -> {
                if (isExpired() && active.compareAndSet(true, false)) {
                    // Clear data first, then cancel the timer. Nulling the reference
                    // allows getFinishedQueries() to schedule a fresh timer on reactivation.
                    // If cancel() doesn't prevent an already-queued tick, the next tick is
                    // a harmless no-op because active is already false (CAS will fail).
                    finishedQueries.clear();
                    approximateSize.set(0);
                    Scheduler.Cancellable self = idleCheckTask.getAndSet(null);
                    if (self != null) self.cancel();
                }
            }, TimeValue.timeValueMillis(intervalMs), QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR);
            if (!idleCheckTask.compareAndSet(null, task)) {
                task.cancel();
            }
        }

        removeExpiredQueries();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(finishedQueries.descendingIterator(), Spliterator.ORDERED), false)
            .limit(MAX_RETURNED_QUERIES)
            .map(fq -> fq.record)
            .toList();
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
        if (idleTimeoutMs == 0 || stopped.get() || !active.get()) return;

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
        boolean failed = (Boolean) record.getAttributes().getOrDefault(Attribute.FAILED, false);
        boolean cancelled = record.isCancelled();
        String status = cancelled ? "cancelled" : (failed ? "failed" : "completed");

        // Build the FinishedQueryRecord copy BEFORE mutating any attributes.
        // setSourceAndTruncation must target the copy, not the original record,
        // because the original is shared with the drain thread (queryRecordsQueue)
        // and mutating its HashMap concurrently is a data race.
        FinishedQueryRecord finished = new FinishedQueryRecord(record, record.getId(), status, liveQueryId);
        if (finished.getAttributes().get(Attribute.SOURCE) == null && finished.getSearchSourceBuilder() != null) {
            TopQueriesService.setSourceAndTruncation(finished, QueryInsightsSettings.DEFAULT_MAX_SOURCE_LENGTH);
        }
        finishedQueries.addLast(new FinishedQuery(finished));
        if (approximateSize.incrementAndGet() > MAX_FINISHED_QUERIES) {
            if (finishedQueries.pollFirst() != null) {
                approximateSize.decrementAndGet();
            }
        }
    }

    /**
     * Returns finished queries only if the cache is already active, without scheduling the idle timer.
     * Used by fan-out nodes in TransportFinishedQueriesAction to avoid activating the cache
     * or resetting the idle timer cluster-wide on every API call.
     * The coordinating node calls {@link #getFinishedQueries()} instead to manage the idle timer.
     *
     * @return list of most recent finished query records, or empty list if cache is inactive
     */
    public List<FinishedQueryRecord> getFinishedQueriesIfActive() {
        if (!active.get()) return List.of();
        removeExpiredQueries();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(finishedQueries.descendingIterator(), Spliterator.ORDERED), false)
            .limit(MAX_RETURNED_QUERIES)
            .map(fq -> fq.record)
            .toList();
    }

    /**
     * Deactivates the cache, cancels the idle-check task, and clears all stored queries.
     * Sets the stopped flag to prevent reactivation by concurrent getFinishedQueries() calls.
     * Only {@link #activate()} (called from doStart) can clear the stopped flag.
     * Called by QueryInsightsService.doStop().
     */
    public void stop() {
        stopped.set(true);
        active.set(false);
        Scheduler.Cancellable task = idleCheckTask.getAndSet(null);
        finishedQueries.clear();
        approximateSize.set(0);
        if (task != null) task.cancel();
    }

    /**
     * Removes queries older than RETENTION_MS from the head of the deque.
     * Uses peekFirst() to check expiry without removing, then pollFirst() to atomically
     * remove the head. Between peek and poll another thread may have already removed the
     * expired head, so the polled element is re-checked: if it turns out to be non-expired,
     * it is re-added to the front via addFirst() and we stop.
     */
    private void removeExpiredQueries() {
        long currentTime = System.currentTimeMillis();
        while (true) {
            FinishedQuery head = finishedQueries.peekFirst();
            if (head == null || currentTime - head.timestamp <= RETENTION_MS) break;
            // Atomically remove the head. Between peek and poll another thread may have
            // already removed the expired element, so poll may return a different (possibly
            // non-expired) element. Check the polled element's timestamp before discarding.
            FinishedQuery removed = finishedQueries.pollFirst();
            if (removed == null) break;
            if (currentTime - removed.timestamp <= RETENTION_MS) {
                // We accidentally removed a non-expired element — put it back.
                finishedQueries.addFirst(removed);
                break;
            }
            approximateSize.decrementAndGet();
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
     * Setting to a non-zero value clears the stopped flag and re-activates the cache so that
     * a 0 → non-zero transition restores the cache without requiring a full service restart.
     *
     * @param idleTimeoutMs new idle timeout in milliseconds, or 0 to disable
     */
    public void setIdleTimeout(long idleTimeoutMs) {
        this.idleTimeoutMs = idleTimeoutMs;
        if (idleTimeoutMs == 0) {
            stop();
        } else {
            activate();
        }
    }

    /**
     * Returns true if the cache is enabled (idle timeout is non-zero).
     */
    public boolean isEnabled() {
        return idleTimeoutMs != 0;
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
