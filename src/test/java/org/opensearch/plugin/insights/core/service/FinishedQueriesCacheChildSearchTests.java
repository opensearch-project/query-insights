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
import org.opensearch.plugin.insights.rules.model.AggregationType;
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

/** Finished SQL/PPL queries are listed under their Live Queries id, without their searches. */
public class FinishedQueriesCacheChildSearchTests extends OpenSearchTestCase {

    private static final String MARKER = "PPL:node-1:42";

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private FinishedQueriesCache cache;

    @Before
    public void setup() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        threadPool = new TestThreadPool(
            "FinishedQueriesCacheChildSearchTests",
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

    public void testSearchesOfAQueryAreNotListed() {
        // A query reading in batches runs many searches; they must not crowd the list while it runs.
        for (int i = 0; i < 200; i++) {
            cache.capture(search("batch-" + i, MARKER, 1L, 10L), 100L + i);
        }
        cache.capture(search("search", null, 3L, 30L), 10L);
        cache.capture(pplQuery(300L, 3000L, false), "node-1:42");
        String localNode = clusterService.localNode().getId();

        for (List<FinishedQueryRecord> finished : List.of(cache.getFinishedQueries(), cache.getFinishedQueriesIfActive())) {
            assertEquals(List.of("node-1:42", localNode + ":10"), ids(finished));
        }
    }

    public void testAQueryKeepsTheCostItArrivesWith() {
        // The query's record already includes its searches' cost, added when it was reported.
        cache.capture(search("child", MARKER, 10L, 100L), 7L);
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
        attributes.put(Attribute.PARENT_MARKER, MARKER);
        attributes.put(Attribute.FAILED, failed);
        return new SearchQueryRecord(System.currentTimeMillis(), measurements(cpu, memory), attributes, null, null, null);
    }

    /** A search; with a marker, one that a SQL/PPL query ran. */
    private static SearchQueryRecord search(String id, String derivedFrom, long cpu, long memory) {
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.FAILED, false);
        if (derivedFrom != null) {
            attributes.put(Attribute.QUERY_SOURCE, "DSL");
            attributes.put(Attribute.DERIVED_FROM, derivedFrom);
            attributes.put(Attribute.IS_CHILD, true);
        }
        return new SearchQueryRecord(System.currentTimeMillis(), measurements(cpu, memory), attributes, id);
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
