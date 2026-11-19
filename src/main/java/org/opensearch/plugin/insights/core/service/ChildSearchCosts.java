/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * CPU and memory of the searches a running SQL/PPL query has finished, keyed by the query's marker.
 * The query's record takes them when it arrives, and Live Queries reads them while the query runs.
 * A query and its searches are coordinated on the same node, so the totals are complete by then.
 */
public final class ChildSearchCosts {

    /** A query's costs are dropped once none of its searches has ended for this long, well past the SQL query timeout. */
    static final long STALE_AFTER_MS = TimeUnit.HOURS.toMillis(1);

    /** At most this many queries have costs held at once; the stalest go first. */
    static final int MAX_QUERIES = 1000;

    private final Map<String, Cost> costs = new ConcurrentHashMap<>();
    private final LongSupplier clock;

    /** Uses the system clock. */
    public ChildSearchCosts() {
        this(System::currentTimeMillis);
    }

    ChildSearchCosts(final LongSupplier clock) {
        this.clock = clock;
    }

    /**
     * Adds a finished search's CPU and memory to the query with {@code marker}.
     *
     * @param marker      the marker of the query that ran the search
     * @param cpuNanos    the search's CPU time
     * @param memoryBytes the search's memory
     */
    public void add(final String marker, final long cpuNanos, final long memoryBytes) {
        final long now = clock.getAsLong();
        costs.compute(marker, (k, held) -> (held == null ? new Cost() : held).add(cpuNanos, memoryBytes, now));
        if (costs.size() > MAX_QUERIES) {
            removeStale(now);
            if (costs.size() > MAX_QUERIES) {
                dropStalest();
            }
        }
    }

    /**
     * Removes and returns the cost held for the query with {@code marker}.
     *
     * @param marker the query's marker
     * @return the summed cost of the searches the query ran, or {@code null} if none were held
     */
    public Cost take(final String marker) {
        final Cost cost = costs.remove(marker);
        removeStale(clock.getAsLong());
        return cost;
    }

    /**
     * Returns a copy of the cost held so far for the query with {@code marker}, without removing it.
     *
     * @param marker the query's marker
     * @return the cost so far, or {@code null} if none of its searches has ended
     */
    public Cost get(final String marker) {
        final Cost[] copy = new Cost[1];
        costs.computeIfPresent(marker, (k, held) -> {
            copy[0] = held.copy();
            return held;
        });
        return copy[0];
    }

    /** Drops every held cost. */
    void clear() {
        costs.clear();
    }

    /** How many queries have costs held for them. */
    int size() {
        return costs.size();
    }

    private void removeStale(final long now) {
        costs.entrySet().removeIf(e -> now - e.getValue().lastUpdate > STALE_AFTER_MS);
    }

    private void dropStalest() {
        costs.entrySet()
            .stream()
            .min(Comparator.comparingLong(e -> e.getValue().lastUpdate))
            .ifPresent(e -> costs.remove(e.getKey(), e.getValue()));
    }

    /**
     * Summed cost of a query's finished searches. Only changed inside {@code compute}. {@code lastUpdate}
     * is volatile because the stale sweep reads it too.
     */
    public static final class Cost {
        private long cpuNanos;
        private long memoryBytes;
        private int searches;
        private volatile long lastUpdate;

        private Cost add(final long cpu, final long memory, final long now) {
            cpuNanos += cpu;
            memoryBytes += memory;
            searches++;
            lastUpdate = now;
            return this;
        }

        private Cost copy() {
            final Cost copy = new Cost();
            copy.cpuNanos = cpuNanos;
            copy.memoryBytes = memoryBytes;
            copy.searches = searches;
            copy.lastUpdate = lastUpdate;
            return copy;
        }

        /**
         * @return the summed CPU time of the searches, in nanoseconds
         */
        public long getCpuNanos() {
            return cpuNanos;
        }

        /**
         * @return the summed memory of the searches, in bytes
         */
        public long getMemoryBytes() {
            return memoryBytes;
        }

        /**
         * @return how many searches ended
         */
        public int getSearches() {
            return searches;
        }
    }
}
