/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.service;

import java.util.ArrayDeque;
import java.util.List;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Cache for recently finished queries
 */
public class FinishedQueriesCache {

    private static final int MAX_FINISHED_QUERIES = 1000;
    private static final int MAX_RETURNED_QUERIES = 50;

    private final ArrayDeque<FinishedQuery> finishedQueries = new ArrayDeque<>();
    private volatile long lastAccessTime = 0;
    private volatile long retentionPeriodMs;

    public FinishedQueriesCache(long retentionPeriodMs) {
        this.retentionPeriodMs = retentionPeriodMs;
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
            lastAccessTime = System.currentTimeMillis();
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
        return false; // Inactivity timeout handled separately
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

    public void clear() {
        synchronized (finishedQueries) {
            finishedQueries.clear();
        }
    }
}
