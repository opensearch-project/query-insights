/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.concurrent.atomic.AtomicLong;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The cost of the searches each running SQL/PPL query has finished, held for the query's own record.
 */
public class ChildSearchCostsTests extends OpenSearchTestCase {

    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final ChildSearchCosts costs = new ChildSearchCosts(now::get);

    public void testAQueryTakesTheSummedCostOfItsSearchesOnce() {
        costs.add("PPL:node-1:1", 10L, 100L);
        costs.add("PPL:node-1:1", 20L, 200L);
        costs.add("PPL:node-1:2", 5L, 50L);

        ChildSearchCosts.Cost cost = costs.take("PPL:node-1:1");

        assertEquals(30L, cost.getCpuNanos());
        assertEquals(300L, cost.getMemoryBytes());
        assertEquals(2, cost.getSearches());
        assertNull("taken once", costs.take("PPL:node-1:1"));
        assertEquals(5L, costs.take("PPL:node-1:2").getCpuNanos());
    }

    public void testAQueryWithoutSearchesHasNoCost() {
        assertNull(costs.take("PPL:node-1:9"));
        assertNull(costs.get("PPL:node-1:9"));
    }

    public void testARunningQuerysCostIsReadWithoutTakingIt() {
        costs.add("PPL:node-1:1", 10L, 100L);
        costs.add("PPL:node-1:1", 20L, 200L);

        ChildSearchCosts.Cost soFar = costs.get("PPL:node-1:1");
        assertEquals(2, soFar.getSearches());
        assertEquals(30L, soFar.getCpuNanos());
        assertEquals(300L, soFar.getMemoryBytes());

        // What was read is a copy: a later search doesn't change it, and the query still takes all.
        costs.add("PPL:node-1:1", 5L, 50L);
        assertEquals(2, soFar.getSearches());
        assertEquals(30L, soFar.getCpuNanos());
        assertEquals(3, costs.get("PPL:node-1:1").getSearches());
        ChildSearchCosts.Cost taken = costs.take("PPL:node-1:1");
        assertEquals(3, taken.getSearches());
        assertEquals(35L, taken.getCpuNanos());
        assertEquals(350L, taken.getMemoryBytes());
        assertNull(costs.get("PPL:node-1:1"));
    }

    public void testCostsOfAQueryThatNeverReportsGoStale() {
        costs.add("PPL:node-1:1", 10L, 100L);
        now.addAndGet(ChildSearchCosts.STALE_AFTER_MS + 1);
        costs.add("PPL:node-1:2", 5L, 50L);

        // Taking any query's cost sweeps the stale ones.
        costs.take("PPL:node-1:3");

        assertEquals(1, costs.size());
        assertNull(costs.take("PPL:node-1:1"));
        assertNotNull(costs.take("PPL:node-1:2"));
    }

    public void testARunningQueryKeepsItsCostWithinTheStaleTime() {
        costs.add("PPL:node-1:1", 10L, 100L);
        now.addAndGet(ChildSearchCosts.STALE_AFTER_MS - 1);
        costs.take("PPL:node-1:3");

        assertEquals(10L, costs.take("PPL:node-1:1").getCpuNanos());
    }

    public void testAtMostMaxQueriesAreHeldAndTheStalestGoFirst() {
        for (int i = 0; i < ChildSearchCosts.MAX_QUERIES + 5; i++) {
            now.incrementAndGet();
            costs.add("PPL:node-1:" + i, 1L, 1L);
        }

        assertEquals(ChildSearchCosts.MAX_QUERIES, costs.size());
        assertNull(costs.take("PPL:node-1:0"));
        assertNull(costs.take("PPL:node-1:4"));
        assertNotNull(costs.take("PPL:node-1:5"));
        assertNotNull(costs.take("PPL:node-1:" + (ChildSearchCosts.MAX_QUERIES + 4)));
    }

    public void testClearDropsEveryHeldCost() {
        costs.add("PPL:node-1:1", 10L, 100L);

        costs.clear();

        assertEquals(0, costs.size());
    }
}
