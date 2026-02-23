/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.service;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.opensearch.cluster.service.ClusterService;
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

}
