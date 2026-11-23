/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.opensearch.plugin.insights.rules.model.ChildSearches;
import org.opensearch.plugin.insights.rules.model.FinishedSearches;

/**
 * The searches each running SQL/PPL query has finished, keyed by the query's marker: their summed CPU
 * and memory, the first {@link ChildSearches#MAX_SEARCHES} of them, and whether one read an excluded
 * index. The query's record takes them when it arrives, and Live Queries reads the totals while the
 * query runs. A query and its searches are coordinated on the same node, so they're complete by then.
 */
public final class ChildSearchTracker {

    /** A query's searches are dropped once none has ended for this long, well past the SQL query timeout. */
    static final long STALE_AFTER_MS = TimeUnit.HOURS.toMillis(1);

    /** At most this many queries have searches held at once; the stalest go first. */
    static final int MAX_QUERIES = 1000;

    private final Map<String, Tracked> tracked = new ConcurrentHashMap<>();
    private final LongSupplier clock;

    /** Uses the system clock. */
    public ChildSearchTracker() {
        this(System::currentTimeMillis);
    }

    ChildSearchTracker(final LongSupplier clock) {
        this.clock = clock;
    }

    /**
     * Adds a finished search to the query with {@code marker}.
     *
     * @param marker the marker of the query that ran the search
     * @param search the search
     */
    public void add(final String marker, final ChildSearches.Search search) {
        final long now = clock.getAsLong();
        tracked.compute(marker, (k, held) -> (held == null ? new Tracked() : held).add(search, now));
        if (tracked.size() > MAX_QUERIES) {
            removeStale(now);
            if (tracked.size() > MAX_QUERIES) {
                dropStalest();
            }
        }
    }

    /**
     * Marks the query with {@code marker} as left out: one of its searches read an excluded index.
     *
     * @param marker the query's marker
     */
    public void exclude(final String marker) {
        final long now = clock.getAsLong();
        tracked.compute(marker, (k, held) -> (held == null ? new Tracked() : held).exclude(now));
        if (tracked.size() > MAX_QUERIES) {
            removeStale(now);
            if (tracked.size() > MAX_QUERIES) {
                dropStalest();
            }
        }
    }

    /**
     * Removes and returns the searches held for the query with {@code marker}.
     *
     * @param marker the query's marker
     * @return the searches the query ran, or {@code null} if none were held
     */
    public Tracked take(final String marker) {
        final Tracked searches = tracked.remove(marker);
        removeStale(clock.getAsLong());
        return searches;
    }

    /**
     * Returns how many searches the query with {@code marker} has finished so far, and their cost,
     * without removing them.
     *
     * @param marker the query's marker
     * @return the count and cost so far, or {@code null} if none of its searches has ended
     */
    public FinishedSearches get(final String marker) {
        final FinishedSearches[] soFar = new FinishedSearches[1];
        tracked.computeIfPresent(marker, (k, held) -> {
            soFar[0] = new FinishedSearches(held.count, held.cpuNanos, held.memoryBytes);
            return held;
        });
        return soFar[0];
    }

    /** Drops every held search. */
    void clear() {
        tracked.clear();
    }

    /** How many queries have searches held for them. */
    int size() {
        return tracked.size();
    }

    private void removeStale(final long now) {
        tracked.entrySet().removeIf(e -> now - e.getValue().lastUpdate > STALE_AFTER_MS);
    }

    private void dropStalest() {
        tracked.entrySet()
            .stream()
            .min(Comparator.comparingLong(e -> e.getValue().lastUpdate))
            .ifPresent(e -> tracked.remove(e.getKey(), e.getValue()));
    }

    /**
     * The searches a query has finished. Only changed inside {@code compute}, and read once taken out of
     * the map. {@code lastUpdate} is volatile because the stale sweep reads it too.
     */
    public static final class Tracked {
        private long cpuNanos;
        private long memoryBytes;
        private int count;
        private final List<ChildSearches.Search> searches = new ArrayList<>();
        private boolean excluded;
        private volatile long lastUpdate;

        private Tracked add(final ChildSearches.Search search, final long now) {
            cpuNanos += search.getCpuNanos();
            memoryBytes += search.getMemoryBytes();
            count++;
            if (searches.size() < ChildSearches.MAX_SEARCHES) {
                searches.add(search);
            }
            lastUpdate = now;
            return this;
        }

        private Tracked exclude(final long now) {
            excluded = true;
            lastUpdate = now;
            return this;
        }

        /**
         * @return whether one of the searches read an excluded index, which leaves the query out
         */
        public boolean isExcluded() {
            return excluded;
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
         * @return how many searches there were, and the first of them, for the query's record
         */
        public ChildSearches toChildSearches() {
            return new ChildSearches(count, searches);
        }
    }
}
