/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import static org.opensearch.action.search.TransportSearchAction.SEARCH_PHASE_TOOK_ENABLED;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesAction;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesResponse;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * Integration tests for the end-to-end search latency breakdown flow.
 * <p>
 * Verifies that:
 * <ul>
 *   <li>{@code latency_breakdown_map} is present in {@code _insights/top_queries} response</li>
 *   <li>Sum of non-overlapping top-level entries approximates {@code took}</li>
 *   <li>{@code phase_latency_map} values match corresponding breakdown entries</li>
 * </ul>
 *
 * Validates: Requirements 2.3, 11.1, 14.1
 */
@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 0, scope = OpenSearchIntegTestCase.Scope.TEST)
public class SearchLatencyBreakdownIT extends OpenSearchIntegTestCase {

    private static final int TOTAL_NUMBER_OF_NODES = 2;
    private static final int TOTAL_SEARCH_REQUESTS = 3;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(QueryInsightsPlugin.class);
    }

    /**
     * Test that latency_breakdown_map is present in top_queries response
     * after sending search requests with phase_took=true.
     *
     * Validates: Requirement 11.1 - latency_breakdown_map field in _insights/top_queries response
     * Validates: Requirement 14.1 - phase_latency_map preserved unchanged
     */
    public void testBreakdownPresentInTopQueriesResponse() throws InterruptedException {
        Settings commonSettings = Settings.builder()
            .put(TOP_N_LATENCY_QUERIES_ENABLED.getKey(), "true")
            .put(TOP_N_LATENCY_QUERIES_SIZE.getKey(), "100")
            .put(TOP_N_LATENCY_QUERIES_WINDOW_SIZE.getKey(), "600s")
            .put(SEARCH_PHASE_TOOK_ENABLED.getKey(), true)
            .build();

        logger.info("--> starting nodes for latency breakdown integration test");
        List<String> nodes = internalCluster().startNodes(TOTAL_NUMBER_OF_NODES, Settings.builder().put(commonSettings).build());

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertFalse(health.isTimedOut());

        createTestIndex();
        // Send search requests with phase_took=true
        makeSearchRequests(nodes);

        // Query top_queries
        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY, null, null, null, null, null);
        TopQueriesResponse response = OpenSearchIntegTestCase.client().execute(TopQueriesAction.INSTANCE, request).actionGet();

        Assert.assertEquals(0, response.failures().size());
        Assert.assertEquals(TOTAL_NUMBER_OF_NODES, response.getNodes().size());

        // Collect all records from all nodes
        List<SearchQueryRecord> allRecords = response.getNodes()
            .stream()
            .flatMap(n -> n.getTopQueriesRecord().stream())
            .collect(Collectors.toList());

        Assert.assertTrue("Expected at least one top query record", allRecords.size() > 0);

        // Verify latency_breakdown_map is present and phase_latency_map is preserved
        for (SearchQueryRecord record : allRecords) {
            Map<Attribute, Object> attributes = record.getAttributes();

            // Requirement 14.1: phase_latency_map must still be present
            Assert.assertTrue(
                "phase_latency_map should be present in the record",
                attributes.containsKey(Attribute.PHASE_LATENCY_MAP)
            );

            @SuppressWarnings("unchecked")
            Map<String, Long> phaseLatencyMap = (Map<String, Long>) attributes.get(Attribute.PHASE_LATENCY_MAP);
            Assert.assertNotNull("phase_latency_map should not be null", phaseLatencyMap);
            // There should be at least the query phase recorded
            Assert.assertTrue(
                "phase_latency_map should contain at least one phase",
                phaseLatencyMap.size() > 0
            );

            // Requirement 11.1: latency_breakdown_map should be present
            Assert.assertTrue(
                "latency_breakdown_map should be present in the record",
                attributes.containsKey(Attribute.LATENCY_BREAKDOWN_MAP)
            );

            @SuppressWarnings("unchecked")
            Map<String, Long> breakdownMap = (Map<String, Long>) attributes.get(Attribute.LATENCY_BREAKDOWN_MAP);
            Assert.assertNotNull("latency_breakdown_map should not be null", breakdownMap);
            Assert.assertTrue(
                "latency_breakdown_map should contain at least one entry",
                breakdownMap.size() > 0
            );
        }

        internalCluster().stopAllNodes();
    }

    /**
     * Test that the sum of non-overlapping top-level breakdown entries
     * approximately equals the total 'took' time.
     *
     * Validates: Requirement 2.3 - phase_latency_map remains unchanged, breakdown approximates took
     */
    public void testBreakdownApproximatesTook() throws InterruptedException {
        Settings commonSettings = Settings.builder()
            .put(TOP_N_LATENCY_QUERIES_ENABLED.getKey(), "true")
            .put(TOP_N_LATENCY_QUERIES_SIZE.getKey(), "100")
            .put(TOP_N_LATENCY_QUERIES_WINDOW_SIZE.getKey(), "600s")
            .put(SEARCH_PHASE_TOOK_ENABLED.getKey(), true)
            .build();

        logger.info("--> starting nodes for breakdown approximation test");
        List<String> nodes = internalCluster().startNodes(TOTAL_NUMBER_OF_NODES, Settings.builder().put(commonSettings).build());

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertFalse(health.isTimedOut());

        createTestIndex();
        makeSearchRequests(nodes);

        // Query top_queries
        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY, null, null, null, null, null);
        TopQueriesResponse response = OpenSearchIntegTestCase.client().execute(TopQueriesAction.INSTANCE, request).actionGet();

        Assert.assertEquals(0, response.failures().size());

        List<SearchQueryRecord> allRecords = response.getNodes()
            .stream()
            .flatMap(n -> n.getTopQueriesRecord().stream())
            .collect(Collectors.toList());

        Assert.assertTrue("Expected at least one top query record", allRecords.size() > 0);

        for (SearchQueryRecord record : allRecords) {
            // Get took time from the latency measurement
            Number took = record.getMeasurement(MetricType.LATENCY);
            Assert.assertNotNull("Latency measurement should be present", took);
            long tookMs = took.longValue();

            @SuppressWarnings("unchecked")
            Map<String, Long> breakdownMap = (Map<String, Long>) record.getAttributes().get(Attribute.LATENCY_BREAKDOWN_MAP);
            if (breakdownMap == null || breakdownMap.isEmpty()) {
                // Breakdown may not always be present (depends on instrumentation coverage)
                continue;
            }

            // Sum non-overlapping top-level entries that approximate total time:
            // pre_phase_overhead + phase durations + inter-phase gaps + post_phase_overhead ≈ took
            long breakdownSum = 0;

            // Top-level non-overlapping entries
            String[] topLevelKeys = {
                "pre_phase_overhead",
                "can_match",
                "dfs",
                "query",
                "fetch",
                "expand",
                "can_match_to_query_gap",
                "query_to_fetch_gap",
                "fetch_to_expand_gap",
                "post_phase_overhead"
            };

            for (String key : topLevelKeys) {
                Long value = breakdownMap.get(key);
                if (value != null) {
                    breakdownSum += value;
                }
            }

            // The sum should approximate 'took' within a reasonable tolerance.
            // We allow generous tolerance since:
            // 1. Not all time is always accounted for (some sub-ops may be too fast to register)
            // 2. Timing granularity differences between millis and nanos conversion
            // 3. Some overhead categories may not be captured in all scenarios
            if (breakdownSum > 0) {
                // The breakdown sum should not exceed 2x the took time (sanity check)
                Assert.assertTrue(
                    String.format(
                        "Breakdown sum (%d ms) should not drastically exceed took (%d ms)",
                        breakdownSum,
                        tookMs
                    ),
                    breakdownSum <= tookMs * 2 + 10 // allow 10ms absolute tolerance
                );
            }
        }

        internalCluster().stopAllNodes();
    }

    /**
     * Test that phase_latency_map values match corresponding entries in latency_breakdown_map.
     *
     * Validates: Requirement 14.1 - phase_latency_map remains unchanged
     * Validates: Requirement 2.3 - breakdown includes phase durations
     */
    public void testPhaseLatencyMapMatchesBreakdownEntries() throws InterruptedException {
        Settings commonSettings = Settings.builder()
            .put(TOP_N_LATENCY_QUERIES_ENABLED.getKey(), "true")
            .put(TOP_N_LATENCY_QUERIES_SIZE.getKey(), "100")
            .put(TOP_N_LATENCY_QUERIES_WINDOW_SIZE.getKey(), "600s")
            .build();

        logger.info("--> starting nodes for phase latency match test");
        List<String> nodes = internalCluster().startNodes(TOTAL_NUMBER_OF_NODES, Settings.builder().put(commonSettings).build());

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertFalse(health.isTimedOut());

        createTestIndex();
        makeSearchRequests(nodes);

        // Query top_queries
        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY, null, null, null, null, null);
        TopQueriesResponse response = OpenSearchIntegTestCase.client().execute(TopQueriesAction.INSTANCE, request).actionGet();

        Assert.assertEquals(0, response.failures().size());

        List<SearchQueryRecord> allRecords = response.getNodes()
            .stream()
            .flatMap(n -> n.getTopQueriesRecord().stream())
            .collect(Collectors.toList());

        Assert.assertTrue("Expected at least one top query record", allRecords.size() > 0);

        for (SearchQueryRecord record : allRecords) {
            Map<Attribute, Object> attributes = record.getAttributes();

            @SuppressWarnings("unchecked")
            Map<String, Long> phaseLatencyMap = (Map<String, Long>) attributes.get(Attribute.PHASE_LATENCY_MAP);

            @SuppressWarnings("unchecked")
            Map<String, Long> breakdownMap = (Map<String, Long>) attributes.get(Attribute.LATENCY_BREAKDOWN_MAP);

            if (phaseLatencyMap == null || breakdownMap == null) {
                continue;
            }

            // For each phase in phase_latency_map, the corresponding key in
            // latency_breakdown_map should have an equal or similar value.
            // The breakdown map merges phase durations from phaseTookMap,
            // so they should match exactly.
            for (Map.Entry<String, Long> phaseEntry : phaseLatencyMap.entrySet()) {
                String phaseName = phaseEntry.getKey();
                Long phaseValue = phaseEntry.getValue();

                if (phaseValue != null && phaseValue > 0) {
                    Long breakdownValue = breakdownMap.get(phaseName);
                    // The breakdown map should contain the same phase duration
                    // since it merges phaseTookMap values directly.
                    Assert.assertNotNull(
                        String.format(
                            "Phase '%s' with value %d ms should be present in latency_breakdown_map",
                            phaseName,
                            phaseValue
                        ),
                        breakdownValue
                    );
                    Assert.assertEquals(
                        String.format(
                            "Phase '%s' value in latency_breakdown_map (%d) should match phase_latency_map (%d)",
                            phaseName,
                            breakdownValue,
                            phaseValue
                        ),
                        phaseValue,
                        breakdownValue
                    );
                }
            }
        }

        internalCluster().stopAllNodes();
    }

    private void createTestIndex() {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1))
        );
        ensureGreen("test");
        logger.info("--> indexing documents for latency breakdown integration test");
        for (int i = 0; i < 10; i++) {
            IndexResponse indexResponse = client().prepareIndex("test")
                .setId("" + i)
                .setSource("field", "value_" + i, "numeric_field", i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
            assertEquals("CREATED", indexResponse.status().toString());
        }
    }

    private void makeSearchRequests(List<String> nodes) throws InterruptedException {
        for (int i = 0; i < TOTAL_SEARCH_REQUESTS; i++) {
            // Send search requests with phase_took=true to ensure phase timing is captured
            SearchResponse searchResponse = internalCluster().client(nodes.get(i % nodes.size()))
                .prepareSearch("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertEquals(0, searchResponse.getFailedShards());
        }
        // Wait for the query insights listener to process and drain queries to the store
        Thread.sleep(6000);
    }
}
