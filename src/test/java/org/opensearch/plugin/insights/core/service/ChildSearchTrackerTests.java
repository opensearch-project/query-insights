/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.opensearch.plugin.insights.rules.model.ChildSearches;
import org.opensearch.plugin.insights.rules.model.FinishedSearches;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The searches each running SQL/PPL query has finished, held for the query's own record.
 */
public class ChildSearchTrackerTests extends OpenSearchTestCase {

    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final ChildSearchTracker tracker = new ChildSearchTracker(now::get);

    private static ChildSearches.Search search(long cpuNanos, long memoryBytes) {
        return new ChildSearches.Search(1_700_000_000_000L, List.of("logs"), 1, 5L, cpuNanos, memoryBytes, false, null);
    }

    public void testAQueryTakesItsSearchesOnce() {
        tracker.add("PPL:node-1:1", search(10L, 100L));
        tracker.add("PPL:node-1:1", search(20L, 200L));
        tracker.add("PPL:node-1:2", search(5L, 50L));

        ChildSearchTracker.Tracked taken = tracker.take("PPL:node-1:1");

        assertEquals(30L, taken.getCpuNanos());
        assertEquals(300L, taken.getMemoryBytes());
        assertEquals(new ChildSearches(2, List.of(search(10L, 100L), search(20L, 200L))), taken.toChildSearches());
        assertNull("taken once", tracker.take("PPL:node-1:1"));
        assertEquals(5L, tracker.take("PPL:node-1:2").getCpuNanos());
    }

    public void testAQueryWithoutSearchesHasNone() {
        assertNull(tracker.take("PPL:node-1:9"));
        assertNull(tracker.get("PPL:node-1:9"));
    }

    public void testOnlyTheFirstSearchesAreListedButAllAreCounted() {
        int searches = ChildSearches.MAX_SEARCHES + 20;
        for (int i = 0; i < searches; i++) {
            tracker.add("PPL:node-1:1", search(i, 1L));
        }

        ChildSearchTracker.Tracked taken = tracker.take("PPL:node-1:1");

        ChildSearches listed = taken.toChildSearches();
        assertEquals(searches, listed.getCount());
        assertEquals(ChildSearches.MAX_SEARCHES, listed.getSearches().size());
        assertEquals(search(0L, 1L), listed.getSearches().get(0));
        // The cost covers every search, listed or not.
        assertEquals((long) searches * (searches - 1) / 2, taken.getCpuNanos());
        assertEquals(searches, taken.getMemoryBytes());
    }

    public void testARunningQuerysSearchesAreReadWithoutTakingThem() {
        tracker.add("PPL:node-1:1", search(10L, 100L));
        tracker.add("PPL:node-1:1", search(20L, 200L));

        assertEquals(new FinishedSearches(2, 30L, 300L), tracker.get("PPL:node-1:1"));

        // The query still takes all of them, including those that end later.
        tracker.add("PPL:node-1:1", search(5L, 50L));
        assertEquals(new FinishedSearches(3, 35L, 350L), tracker.get("PPL:node-1:1"));
        ChildSearchTracker.Tracked taken = tracker.take("PPL:node-1:1");
        assertEquals(3, taken.toChildSearches().getCount());
        assertEquals(35L, taken.getCpuNanos());
        assertEquals(350L, taken.getMemoryBytes());
        assertNull(tracker.get("PPL:node-1:1"));
    }

    public void testSearchesOfAQueryThatNeverReportsGoStale() {
        tracker.add("PPL:node-1:1", search(10L, 100L));
        now.addAndGet(ChildSearchTracker.STALE_AFTER_MS + 1);
        tracker.add("PPL:node-1:2", search(5L, 50L));

        // Taking any query's searches sweeps the stale ones.
        tracker.take("PPL:node-1:3");

        assertEquals(1, tracker.size());
        assertNull(tracker.take("PPL:node-1:1"));
        assertNotNull(tracker.take("PPL:node-1:2"));
    }

    public void testARunningQueryKeepsItsSearchesWithinTheStaleTime() {
        tracker.add("PPL:node-1:1", search(10L, 100L));
        now.addAndGet(ChildSearchTracker.STALE_AFTER_MS - 1);
        tracker.take("PPL:node-1:3");

        assertEquals(10L, tracker.take("PPL:node-1:1").getCpuNanos());
    }

    public void testAtMostMaxQueriesAreHeldAndTheStalestGoFirst() {
        for (int i = 0; i < ChildSearchTracker.MAX_QUERIES + 5; i++) {
            now.incrementAndGet();
            tracker.add("PPL:node-1:" + i, search(1L, 1L));
        }

        assertEquals(ChildSearchTracker.MAX_QUERIES, tracker.size());
        assertNull(tracker.take("PPL:node-1:0"));
        assertNull(tracker.take("PPL:node-1:4"));
        assertNotNull(tracker.take("PPL:node-1:5"));
        assertNotNull(tracker.take("PPL:node-1:" + (ChildSearchTracker.MAX_QUERIES + 4)));
    }

    public void testClearDropsEveryHeldSearch() {
        tracker.add("PPL:node-1:1", search(10L, 100L));

        tracker.clear();

        assertEquals(0, tracker.size());
    }
}
