/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.healthStats.QueryInsightsHealthStats;
import org.opensearch.plugin.insights.rules.model.healthStats.TopQueriesHealthStats;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * Unit Tests for {@link QueryInsightsService}.
 */
public class QueryInsightsServiceTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private final Client client = mock(Client.class);
    private final NamedXContentRegistry namedXContentRegistry = mock(NamedXContentRegistry.class);
    private QueryInsightsService queryInsightsService;
    private QueryInsightsService queryInsightsServiceSpy;

    @Before
    public void setup() {
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        this.threadPool = new TestThreadPool(
            "QueryInsightsHealthStatsTests",
            new ScalingExecutorBuilder(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR, 1, 5, TimeValue.timeValueMinutes(5))
        );
        queryInsightsService = new QueryInsightsService(
            new ClusterService(settings, clusterSettings, threadPool),
            threadPool,
            client,
            NoopMetricsRegistry.INSTANCE,
            namedXContentRegistry
        );
        queryInsightsService.enableCollection(MetricType.LATENCY, true);
        queryInsightsService.enableCollection(MetricType.CPU, true);
        queryInsightsService.enableCollection(MetricType.MEMORY, true);
        queryInsightsServiceSpy = spy(queryInsightsService);

        MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );
        OperationalMetricsCounter.initialize("cluster", metricsRegistry);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testAddRecordToLimitAndDrain() {
        SearchQueryRecord record = QueryInsightsTestUtils.generateQueryInsightRecords(1, 1, System.currentTimeMillis(), 0).get(0);
        for (int i = 0; i < QueryInsightsSettings.QUERY_RECORD_QUEUE_CAPACITY; i++) {
            assertTrue(queryInsightsService.addRecord(record));
        }
        // exceed capacity
        assertFalse(queryInsightsService.addRecord(record));
        queryInsightsService.drainRecords();
        assertEquals(
            QueryInsightsSettings.DEFAULT_TOP_N_SIZE,
            queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false, null, null, null).size()
        );
    }

    public void testClose() {
        try {
            queryInsightsService.doClose();
        } catch (Exception e) {
            fail("No exception expected when closing query insights service");
        }
    }

    public void testSearchQueryMetricsEnabled() {
        // Initially, searchQueryMetricsEnabled should be false and searchQueryCategorizer should be null
        assertFalse(queryInsightsService.isSearchQueryMetricsFeatureEnabled());
        assertNotNull(queryInsightsService.getSearchQueryCategorizer());

        // Enable search query metrics
        queryInsightsService.enableSearchQueryMetricsFeature(true);

        // Assert that searchQueryMetricsEnabled is true and searchQueryCategorizer is initialized
        assertTrue(queryInsightsService.isSearchQueryMetricsFeatureEnabled());
        assertNotNull(queryInsightsService.getSearchQueryCategorizer());

        // Disable search query metrics
        queryInsightsService.enableSearchQueryMetricsFeature(false);

        // Assert that searchQueryMetricsEnabled is false and searchQueryCategorizer is not null
        assertFalse(queryInsightsService.isSearchQueryMetricsFeatureEnabled());
        assertNotNull(queryInsightsService.getSearchQueryCategorizer());

    }

    public void testAddRecordGroupBySimilarityWithDifferentGroups() {

        int numberOfRecordsRequired = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightsRecordsWithMeasurement(
            numberOfRecordsRequired,
            MetricType.LATENCY,
            5
        );

        queryInsightsService.setGrouping(GroupingType.SIMILARITY.getValue());
        assertEquals(queryInsightsService.getGrouping(), GroupingType.SIMILARITY);

        for (int i = 0; i < numberOfRecordsRequired; i++) {
            assertTrue(queryInsightsService.addRecord(records.get(i)));
        }
        // exceed capacity but handoff to grouping
        assertTrue(queryInsightsService.addRecord(records.get(numberOfRecordsRequired - 1)));

        queryInsightsService.drainRecords();

        assertEquals(
            QueryInsightsSettings.DEFAULT_TOP_N_SIZE,
            queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false, null, null, null).size()
        );
    }

    public void testAddRecordGroupBySimilarityWithOneGroup() {
        int numberOfRecordsRequired = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightsRecordsWithMeasurement(
            numberOfRecordsRequired,
            MetricType.LATENCY,
            5
        );
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);

        queryInsightsService.setGrouping(GroupingType.SIMILARITY.getValue());
        assertEquals(queryInsightsService.getGrouping(), GroupingType.SIMILARITY);

        for (int i = 0; i < numberOfRecordsRequired; i++) {
            assertTrue(queryInsightsService.addRecord(records.get(i)));
        }
        // exceed capacity but handoff to grouping service
        assertTrue(queryInsightsService.addRecord(records.get(numberOfRecordsRequired - 1)));

        queryInsightsService.drainRecords();
        assertEquals(1, queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false, null, null, null).size());
    }

    public void testAddRecordGroupBySimilarityWithTwoGroups() {
        List<SearchQueryRecord> records1 = QueryInsightsTestUtils.generateQueryInsightRecords(2, 2, System.currentTimeMillis(), 0);
        QueryInsightsTestUtils.populateHashcode(records1, 1);

        List<SearchQueryRecord> records2 = QueryInsightsTestUtils.generateQueryInsightRecords(2, 2, System.currentTimeMillis(), 0);
        QueryInsightsTestUtils.populateHashcode(records2, 2);

        queryInsightsService.setGrouping(GroupingType.SIMILARITY.getValue());
        assertEquals(queryInsightsService.getGrouping(), GroupingType.SIMILARITY);

        for (int i = 0; i < 2; i++) {
            assertTrue(queryInsightsService.addRecord(records1.get(i)));
            assertTrue(queryInsightsService.addRecord(records2.get(i)));
        }

        queryInsightsService.drainRecords();
        assertEquals(2, queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false, null, null, null).size());
    }

    public void testGetHealthStats() {
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        queryInsightsService.addRecord(records.get(0));
        QueryInsightsHealthStats healthStats = queryInsightsService.getHealthStats();
        assertNotNull(healthStats);
        assertEquals(threadPool.info(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR), healthStats.getThreadPoolInfo());
        assertEquals(1, healthStats.getQueryRecordsQueueSize());
        Map<MetricType, TopQueriesHealthStats> topQueriesHealthStatsMap = healthStats.getTopQueriesHealthStats();
        assertNotNull(topQueriesHealthStatsMap);
        assertEquals(3, topQueriesHealthStatsMap.size());
        assertTrue(topQueriesHealthStatsMap.containsKey(MetricType.LATENCY));
        assertTrue(topQueriesHealthStatsMap.containsKey(MetricType.CPU));
        assertTrue(topQueriesHealthStatsMap.containsKey(MetricType.MEMORY));
    }
}
