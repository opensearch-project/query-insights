/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Service to manage finished live queries (completed/cancelled) with automatic cleanup after 30 seconds
 */
public class CompletedLiveQueriesService {
    private static final int RETENTION_SECONDS = 30;
    private static final int MAX_COMPLETED_QUERIES = 1000;
    private static volatile CompletedLiveQueriesService instance;
    private final ConcurrentLinkedQueue<CompletedQueryEntry> completedQueries = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "completed-live-queries-cleanup");
        t.setDaemon(true);
        return t;
    });

    private CompletedLiveQueriesService() {
        // Schedule cleanup every 10 seconds
        cleanupExecutor.scheduleAtFixedRate(this::cleanup, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * Get singleton instance
     */
    public static CompletedLiveQueriesService getInstance() {
        if (instance == null) {
            synchronized (CompletedLiveQueriesService.class) {
                if (instance == null) {
                    instance = new CompletedLiveQueriesService();
                }
            }
        }
        return instance;
    }

    /**
     * Add a finished query (completed or cancelled) to the store
     */
    public void addCompletedQuery(SearchQueryRecord record) {
        completedQueries.offer(new CompletedQueryEntry(record, System.currentTimeMillis()));

        // Remove oldest entries if we exceed the maximum limit
        while (completedQueries.size() > MAX_COMPLETED_QUERIES) {
            completedQueries.poll();
        }
    }

    /**
     * Get all finished queries (completed/cancelled) that are still within retention period
     */
    public List<SearchQueryRecord> getCompletedQueries() {
        long cutoffTime = System.currentTimeMillis() - (RETENTION_SECONDS * 1000L);
        return completedQueries.stream()
            .filter(entry -> entry.timestamp > cutoffTime)
            .map(entry -> entry.record)
            .collect(Collectors.toList());
    }

    /**
     * Get the maximum number of finished queries that can be stored
     */
    public int getMaxCompletedQueries() {
        return MAX_COMPLETED_QUERIES;
    }

    /**
     * Get the current number of finished queries in the store
     */
    public int getCurrentSize() {
        return completedQueries.size();
    }

    /**
     * Clean up expired entries
     */
    private void cleanup() {
        long cutoffTime = System.currentTimeMillis() - (RETENTION_SECONDS * 1000L);
        while (!completedQueries.isEmpty() && completedQueries.peek().timestamp <= cutoffTime) {
            completedQueries.poll();
        }
    }

    /**
     * Shutdown the service
     */
    public void shutdown() {
        cleanupExecutor.shutdown();
    }

    /**
     * Reset singleton instance for testing
     */
    public static void resetForTesting() {
        if (instance != null) {
            instance.shutdown();
            instance = null;
        }
    }

    private static class CompletedQueryEntry {
        final SearchQueryRecord record;
        final long timestamp;

        CompletedQueryEntry(SearchQueryRecord record, long timestamp) {
            this.record = record;
            this.timestamp = timestamp;
        }
    }
}
