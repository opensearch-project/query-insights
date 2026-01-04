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
    private static final long FINISHED_QUERY_RETENTION_MS = 30_000L; // 30 seconds
    private static final long TRACKING_INACTIVITY_MS = 300_000L; // 5 minutes

    private final ArrayDeque<FinishedQuery> finishedQueries = new ArrayDeque<>();
    private volatile long lastAccessTime = 0;

    private static class FinishedQuery {
        final SearchQueryRecord record;
        final long finishTime;

        FinishedQuery(SearchQueryRecord record, long finishTime) {
            this.record = record;
            this.finishTime = finishTime;
        }
    }

    public void addFinishedQuery(SearchQueryRecord record) {
        if (System.currentTimeMillis() - lastAccessTime > TRACKING_INACTIVITY_MS) {
            return;
        }
        synchronized (finishedQueries) {
            finishedQueries.addLast(new FinishedQuery(record, System.currentTimeMillis()));
            if (finishedQueries.size() > MAX_FINISHED_QUERIES) {
                finishedQueries.removeFirst();
            }
        }
    }

    public List<SearchQueryRecord> getFinishedQueries() {
        lastAccessTime = System.currentTimeMillis();
        synchronized (finishedQueries) {
            while (!finishedQueries.isEmpty() && (lastAccessTime - finishedQueries.peekFirst().finishTime) > FINISHED_QUERY_RETENTION_MS) {
                finishedQueries.removeFirst();
            }
            return finishedQueries.stream()
                .sorted((a, b) -> Long.compare(b.finishTime, a.finishTime))
                .limit(MAX_RETURNED_QUERIES)
                .map(fq -> fq.record)
                .toList();
        }
    }

    public void clear() {
        synchronized (finishedQueries) {
            finishedQueries.clear();
        }
    }
}
