/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.service;

import java.util.List;
import java.util.PriorityQueue;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Cache for recently finished queries
 */
public class FinishedQueriesCache {

    private static final int MAX_FINISHED_QUERIES = 1000;
    private static final int MAX_RETURNED_QUERIES = 50;
    private static final long FINISHED_QUERY_RETENTION_MS = 30_000L;
    private static final long TRACKING_INACTIVITY_MS = 300_000L;

    private final PriorityQueue<FinishedQuery> finishedQueries = new PriorityQueue<>((a, b) -> Long.compare(a.finishTime, b.finishTime));
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
            finishedQueries.offer(new FinishedQuery(record, System.currentTimeMillis()));
            if (finishedQueries.size() > MAX_FINISHED_QUERIES) {
                finishedQueries.poll();
            }
        }
    }

    public List<SearchQueryRecord> getFinishedQueries() {
        lastAccessTime = System.currentTimeMillis();
        synchronized (finishedQueries) {
            while (!finishedQueries.isEmpty() && (lastAccessTime - finishedQueries.peek().finishTime) > FINISHED_QUERY_RETENTION_MS) {
                finishedQueries.poll();
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
