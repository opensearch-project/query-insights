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
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.ChildSearches;
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

/** Finished SQL/PPL queries are listed under their Live Queries id, with the searches they ran. */
public class FinishedQueriesCachePplQueryTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private FinishedQueriesCache cache;

    @Before
    public void setup() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        threadPool = new TestThreadPool(
            "FinishedQueriesCachePplQueryTests",
            new ScalingExecutorBuilder(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR, 1, 5, TimeValue.timeValueMinutes(5))
        );
        ClusterState state = ClusterStateCreationUtils.stateWithActivePrimary("test", true, 1, 0);
        clusterService = ClusterServiceUtils.createClusterService(threadPool, state.getNodes().getLocalNode(), clusterSettings);
        ClusterServiceUtils.setState(clusterService, state);
        cache = new FinishedQueriesCache(clusterService, threadPool);
        cache.activate();
    }

    @After
    public void teardown() {
        cache.stop();
        threadPool.shutdownNow();
        clusterService.close();
    }

    public void testPplQueryIsRecordedUnderItsLiveQueryId() {
        SearchQueryRecord query = pplQuery(100L, 1000L, true);

        cache.capture(query, "node-1:42");

        List<FinishedQueryRecord> finished = cache.getFinishedQueriesIfActive();
        assertEquals(1, finished.size());
        assertEquals("node-1:42", finished.get(0).getId());
        assertEquals("links to the query's Top N record", query.getId(), finished.get(0).getTopNId());
        assertEquals("failed", finished.get(0).getStatus());
    }

    public void testAQueryIsListedWithTheSearchesItRan() {
        SearchQueryRecord query = pplQuery(300L, 3000L, false);
        ChildSearches searches = new ChildSearches(
            2,
            List.of(
                new ChildSearches.Search(1L, List.of("logs"), 1, 5L, 100L, 1000L, false, null),
                new ChildSearches.Search(2L, List.of("logs"), 1, 6L, 200L, 2000L, false, null)
            )
        );
        query.addAttribute(Attribute.CHILD_SEARCHES, searches);

        cache.capture(query, "node-1:42");

        for (List<FinishedQueryRecord> finished : List.of(cache.getFinishedQueries(), cache.getFinishedQueriesIfActive())) {
            assertEquals(List.of("node-1:42"), ids(finished));
            assertEquals(searches, finished.get(0).getAttributes().get(Attribute.CHILD_SEARCHES));
            assertTrue(finished.get(0).toString().contains("\"child_searches\":{\"count\":2,"));
        }
    }

    public void testAQuerysTextIsListedAsQueryTextWithNoSource() throws Exception {
        SearchQueryRecord query = pplQuery(100L, 1000L, false);
        query.addAttribute(Attribute.QUERY_TEXT, "source=logs | head 5");
        query.addAttribute(Attribute.SOURCE_TRUNCATED, false);

        cache.capture(query, "node-1:42");

        // Readers parse source as JSON; a 3.10 Dashboards fails on PPL text there.
        for (List<FinishedQueryRecord> finished : List.of(cache.getFinishedQueries(), cache.getFinishedQueriesIfActive())) {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, finished.get(0).toString())) {
                Map<String, Object> fields = parser.map();
                assertEquals("source=logs | head 5", fields.get("query_text"));
                assertFalse(fields.containsKey("source"));
            }
        }
    }

    /**
     * A cache keeps PPL queries only while it's active, so an idle node's cache doesn't fill with
     * entries nothing evicts. Each test then activates the cache, which keeps what it holds, so an
     * entry stored while it wasn't collecting would show.
     */
    public void testACacheThatWasNeverActivatedKeepsNoPplQuery() {
        FinishedQueriesCache neverActivated = new FinishedQueriesCache(clusterService, threadPool);
        try {
            neverActivated.capture(pplQuery(100L, 1000L, false), "node-1:42");

            neverActivated.activate();
            assertEquals(List.of(), neverActivated.getFinishedQueriesIfActive());
        } finally {
            neverActivated.stop();
        }
    }

    public void testAStoppedCacheKeepsNoPplQuery() {
        cache.stop();

        cache.capture(pplQuery(100L, 1000L, false), "node-1:42");

        cache.activate();
        assertEquals(List.of(), cache.getFinishedQueriesIfActive());
    }

    public void testACacheWithAnIdleTimeoutOfZeroKeepsNoPplQuery() {
        cache.setIdleTimeout(0);

        cache.capture(pplQuery(100L, 1000L, false), "node-1:42");

        cache.setIdleTimeout(TimeValue.timeValueMinutes(5).millis());
        assertEquals(List.of(), cache.getFinishedQueriesIfActive());
    }

    public void testAQueryKeepsTheCostItArrivesWith() {
        // The query's record already includes its searches' cost, added when it was reported.
        SearchQueryRecord query = pplQuery(110L, 1100L, false);

        cache.capture(query, "node-1:42");

        for (List<FinishedQueryRecord> finished : List.of(cache.getFinishedQueries(), cache.getFinishedQueriesIfActive())) {
            assertEquals(110L, cpu(finished.get(0)));
            assertEquals(1100L, memory(finished.get(0)));
            assertEquals("completed", finished.get(0).getStatus());
        }
    }

    private static List<String> ids(List<FinishedQueryRecord> records) {
        return records.stream().map(FinishedQueryRecord::getId).toList();
    }

    private static SearchQueryRecord pplQuery(long cpu, long memory, boolean failed) {
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.QUERY_SOURCE, "PPL");
        attributes.put(Attribute.FAILED, failed);
        return new SearchQueryRecord(System.currentTimeMillis(), measurements(cpu, memory), attributes, null, null, null);
    }

    private static Map<MetricType, Measurement> measurements(long cpu, long memory) {
        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(50L, AggregationType.NONE));
        measurements.put(MetricType.CPU, new Measurement(cpu, AggregationType.NONE));
        measurements.put(MetricType.MEMORY, new Measurement(memory, AggregationType.NONE));
        return measurements;
    }

    private static long cpu(SearchQueryRecord record) {
        return record.getMeasurement(MetricType.CPU).longValue();
    }

    private static long memory(SearchQueryRecord record) {
        return record.getMeasurement(MetricType.MEMORY).longValue();
    }
}
