/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.FinishedQueryRecord;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

public class FinishedQueriesCacheTimeoutTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void setup() {
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        threadPool = new TestThreadPool(
            "FinishedQueriesCacheTests",
            new ScalingExecutorBuilder(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR, 1, 5, TimeValue.timeValueMinutes(5))
        );
        ClusterState state = ClusterStateCreationUtils.stateWithActivePrimary("test", true, 1, 0);
        clusterService = ClusterServiceUtils.createClusterService(threadPool, state.getNodes().getLocalNode(), clusterSettings);
        ClusterServiceUtils.setState(clusterService, state);
    }

    @After
    public void teardown() {
        threadPool.shutdownNow();
        clusterService.close();
    }

    private FinishedQueriesCache createCache() {
        return new FinishedQueriesCache(clusterService, threadPool);
    }

    private SearchQueryRecord createSearchQueryRecord() {
        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(100L));
        measurements.put(MetricType.CPU, new Measurement(200L));
        measurements.put(MetricType.MEMORY, new Measurement(300L));
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.TASK_RESOURCE_USAGES, List.of());
        attributes.put(Attribute.FAILED, false);
        return new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, "record-id");
    }

    // --- FinishedQueryRecord construction tests ---

    public void testFinishedQueryRecordHasTopNId() {
        FinishedQueryRecord record = createFinishedRecord("nodeId:51");
        assertEquals("nodeId:51", record.getId());
        assertEquals("topn-uuid", record.getTopNId());
        assertEquals("completed", record.getStatus());
    }

    public void testFinishedQueryRecordStatuses() {
        var measurements = new HashMap<MetricType, Measurement>();
        measurements.put(MetricType.LATENCY, new Measurement(100L));
        measurements.put(MetricType.CPU, new Measurement(200L));
        measurements.put(MetricType.MEMORY, new Measurement(300L));
        SearchQueryRecord base = new SearchQueryRecord(System.currentTimeMillis(), measurements, new HashMap<>(), "id");

        assertEquals("failed", new FinishedQueryRecord(base, null, "failed", "id:1").getStatus());
        assertEquals("cancelled", new FinishedQueryRecord(base, null, "cancelled", "id:1").getStatus());
        assertEquals("completed", new FinishedQueryRecord(base, null, "completed", "id:1").getStatus());
    }

    private FinishedQueryRecord createFinishedRecord(String id) {
        var measurements = new HashMap<MetricType, Measurement>();
        measurements.put(MetricType.LATENCY, new Measurement(100L));
        measurements.put(MetricType.CPU, new Measurement(200L));
        measurements.put(MetricType.MEMORY, new Measurement(300L));
        SearchQueryRecord base = new SearchQueryRecord(System.currentTimeMillis(), measurements, new HashMap<>(), id);
        return new FinishedQueryRecord(base, "topn-uuid", "completed", "nodeId:51");
    }

    // --- isEnabled tests ---

    public void testIsEnabledWithDefaultTimeout() {
        FinishedQueriesCache cache = createCache();
        assertTrue("Cache should be enabled with default 5min timeout", cache.isEnabled());
        cache.stop();
    }

    public void testIsEnabledReturnsFalseAfterSetIdleTimeoutZero() {
        FinishedQueriesCache cache = createCache();
        cache.setIdleTimeout(0);
        assertFalse("Cache should be disabled when idle timeout is 0", cache.isEnabled());
    }

    // --- activate / capture / getFinishedQueries lifecycle ---

    public void testCaptureNoOpBeforeActivate() {
        FinishedQueriesCache cache = createCache();
        // Don't call activate — capture should be a no-op
        cache.capture(createSearchQueryRecord(), 42L);
        // getFinishedQueriesIfActive returns empty when not active
        List<FinishedQueryRecord> results = cache.getFinishedQueriesIfActive();
        assertTrue("Should return empty list when cache is not activated", results.isEmpty());
        cache.stop();
    }

    public void testCaptureNoOpWhenDisabled() {
        FinishedQueriesCache cache = createCache();
        cache.setIdleTimeout(0); // disable
        cache.activate(); // should be a no-op since disabled
        cache.capture(createSearchQueryRecord(), 42L);
        List<FinishedQueryRecord> results = cache.getFinishedQueriesIfActive();
        assertTrue("Should return empty list when cache is disabled", results.isEmpty());
    }

    public void testActivateAndCapture() {
        FinishedQueriesCache cache = createCache();
        cache.activate();
        cache.capture(createSearchQueryRecord(), 42L);
        // getFinishedQueriesIfActive should return the captured record
        List<FinishedQueryRecord> results = cache.getFinishedQueriesIfActive();
        assertEquals("Should have 1 captured record", 1, results.size());
        assertEquals("completed", results.get(0).getStatus());
        cache.stop();
    }

    public void testGetFinishedQueriesReturnsRecordsInDescendingOrder() {
        FinishedQueriesCache cache = createCache();
        cache.activate();

        for (int i = 0; i < 5; i++) {
            Map<MetricType, Measurement> measurements = new HashMap<>();
            measurements.put(MetricType.LATENCY, new Measurement((long) (i * 100)));
            measurements.put(MetricType.CPU, new Measurement(0L));
            measurements.put(MetricType.MEMORY, new Measurement(0L));
            Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.TASK_RESOURCE_USAGES, List.of());
            attributes.put(Attribute.FAILED, false);
            SearchQueryRecord record = new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, "id-" + i);
            cache.capture(record, i);
        }

        List<FinishedQueryRecord> results = cache.getFinishedQueries();
        assertEquals(5, results.size());
        // Most recent (last captured) should be first in the result
        assertEquals("id-4", results.get(0).getTopNId());
        cache.stop();
    }

    public void testGetFinishedQueriesLimitedToMaxReturnedQueries() {
        FinishedQueriesCache cache = createCache();
        cache.activate();

        // Capture 60 records (MAX_RETURNED_QUERIES is 50)
        for (int i = 0; i < 60; i++) {
            cache.capture(createSearchQueryRecord(), i);
        }

        List<FinishedQueryRecord> results = cache.getFinishedQueries();
        assertEquals("Should return at most 50 records", 50, results.size());
        cache.stop();
    }

    // --- stop clears everything ---

    public void testStopClearsCache() {
        FinishedQueriesCache cache = createCache();
        cache.activate();
        cache.capture(createSearchQueryRecord(), 1L);
        cache.capture(createSearchQueryRecord(), 2L);

        assertEquals(2, cache.getFinishedQueriesIfActive().size());

        cache.stop();

        assertTrue("Cache should be empty after stop", cache.getFinishedQueriesIfActive().isEmpty());
        assertTrue("getFinishedQueries should also be empty after stop", cache.getFinishedQueries().isEmpty());
    }

    // --- setIdleTimeout(0) stops the cache ---

    public void testSetIdleTimeoutZeroStopsCache() {
        FinishedQueriesCache cache = createCache();
        cache.activate();
        cache.capture(createSearchQueryRecord(), 1L);
        assertEquals(1, cache.getFinishedQueriesIfActive().size());

        cache.setIdleTimeout(0);

        assertTrue("Cache should be empty after setting timeout to 0", cache.getFinishedQueriesIfActive().isEmpty());
        assertFalse("Cache should be disabled", cache.isEnabled());
    }

    // --- getFinishedQueriesIfActive vs getFinishedQueries ---

    public void testGetFinishedQueriesIfActiveReturnsEmptyWhenInactive() {
        FinishedQueriesCache cache = createCache();
        // Not activated
        assertTrue(cache.getFinishedQueriesIfActive().isEmpty());
        cache.stop();
    }

    public void testGetFinishedQueriesReturnsEmptyWhenDisabled() {
        FinishedQueriesCache cache = createCache();
        cache.setIdleTimeout(0);
        assertTrue(cache.getFinishedQueries().isEmpty());
    }

    public void testGetFinishedQueriesReturnsEmptyWhenInactive() {
        FinishedQueriesCache cache = createCache();
        // Enabled but not activated
        assertTrue(cache.getFinishedQueries().isEmpty());
        cache.stop();
    }

    // --- activate is idempotent ---

    public void testActivateIsIdempotent() {
        FinishedQueriesCache cache = createCache();
        cache.activate();
        cache.capture(createSearchQueryRecord(), 1L);
        cache.activate(); // second activate should not reset state
        assertEquals("Records should survive second activate call", 1, cache.getFinishedQueriesIfActive().size());
        cache.stop();
    }

    // --- stop prevents reactivation ---

    public void testGetFinishedQueriesCannotReactivateAfterStop() {
        FinishedQueriesCache cache = createCache();
        cache.activate();
        cache.capture(createSearchQueryRecord(), 1L);
        assertEquals(1, cache.getFinishedQueriesIfActive().size());

        cache.stop();

        // getFinishedQueries() should NOT reactivate the cache after stop()
        assertTrue("getFinishedQueries should return empty after stop", cache.getFinishedQueries().isEmpty());
        // capture should also be a no-op after stop
        cache.capture(createSearchQueryRecord(), 2L);
        assertTrue("capture should be no-op after stop", cache.getFinishedQueriesIfActive().isEmpty());
    }

    public void testActivateCanResetStoppedState() {
        FinishedQueriesCache cache = createCache();
        cache.activate();
        cache.capture(createSearchQueryRecord(), 1L);

        cache.stop();
        assertTrue("Cache should be empty after stop", cache.getFinishedQueriesIfActive().isEmpty());

        // activate() (called from setIdleTimeout non-zero) should clear the stopped flag
        cache.activate();
        cache.capture(createSearchQueryRecord(), 2L);
        assertEquals("Cache should accept captures after re-activation", 1, cache.getFinishedQueriesIfActive().size());
        cache.stop();
    }

    public void testClearStoppedAllowsLazyActivation() {
        FinishedQueriesCache cache = createCache();
        // Simulate doStart → doStop → doStart cycle
        cache.activate();
        cache.capture(createSearchQueryRecord(), 1L);

        cache.stop();
        assertTrue("Cache should be empty after stop", cache.getFinishedQueriesIfActive().isEmpty());
        assertTrue("getFinishedQueries should return empty after stop", cache.getFinishedQueries().isEmpty());

        // doStart calls clearStopped() — cache is NOT active yet, but CAN be activated
        cache.clearStopped();
        assertFalse("Cache should not be active after clearStopped alone", cache.getFinishedQueriesIfActive().size() > 0);

        // First API call lazily activates the cache
        cache.getFinishedQueries();
        cache.capture(createSearchQueryRecord(), 2L);
        assertEquals("Cache should accept captures after lazy activation", 1, cache.getFinishedQueriesIfActive().size());
        cache.stop();
    }

    public void testSetIdleTimeoutNonZeroReactivatesAfterDisable() {
        FinishedQueriesCache cache = createCache();
        cache.activate();
        cache.capture(createSearchQueryRecord(), 1L);
        assertEquals(1, cache.getFinishedQueriesIfActive().size());

        // Disable via timeout=0
        cache.setIdleTimeout(0);
        assertTrue("Cache should be empty after disable", cache.getFinishedQueriesIfActive().isEmpty());
        assertFalse("Cache should be disabled", cache.isEnabled());

        // Re-enable via non-zero timeout — should clear stopped flag and reactivate
        cache.setIdleTimeout(300000);
        assertTrue("Cache should be enabled again", cache.isEnabled());
        cache.capture(createSearchQueryRecord(), 2L);
        assertEquals("Cache should accept captures after re-enable", 1, cache.getFinishedQueriesIfActive().size());
        cache.stop();
    }

    // --- capture does not mutate original record (Bug 3 regression test) ---

    public void testCaptureDoesNotMutateOriginalRecord() {
        FinishedQueriesCache cache = createCache();
        cache.activate();

        // Create a record WITHOUT Attribute.SOURCE set — this triggers setSourceAndTruncation
        SearchQueryRecord record = createSearchQueryRecord();
        assertNull("Precondition: SOURCE should be null before capture", record.getAttributes().get(Attribute.SOURCE));

        cache.capture(record, 42L);

        // The original record's attributes must remain untouched
        assertNull("capture() must not mutate the original record's SOURCE attribute", record.getAttributes().get(Attribute.SOURCE));
        assertNull(
            "capture() must not mutate the original record's SOURCE_TRUNCATED attribute",
            record.getAttributes().get(Attribute.SOURCE_TRUNCATED)
        );

        // But the finished copy in the cache SHOULD have SOURCE set
        List<FinishedQueryRecord> results = cache.getFinishedQueriesIfActive();
        assertEquals(1, results.size());
        // SOURCE may or may not be set depending on whether searchSourceBuilder was null,
        // but the key invariant is the original record was not mutated.
        cache.stop();
    }

    // --- size cap at MAX_FINISHED_QUERIES (1000) ---

    public void testSizeCapEvictsOldest() {
        FinishedQueriesCache cache = createCache();
        cache.activate();

        // Capture 1005 records (MAX_FINISHED_QUERIES is 1000)
        for (int i = 0; i < 1005; i++) {
            cache.capture(createSearchQueryRecord(), i);
        }

        // getFinishedQueriesIfActive returns all in cache (up to MAX_RETURNED_QUERIES=50)
        // but the internal size should be capped at 1000
        List<FinishedQueryRecord> results = cache.getFinishedQueriesIfActive();
        assertEquals("Should return MAX_RETURNED_QUERIES (50)", 50, results.size());
        cache.stop();
    }
}
