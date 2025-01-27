/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.plugin.insights.core.service.QueryInsightsService.QUERY_INSIGHTS_INDEX_TAG_NAME;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.TOP_QUERIES_INDEX_TAG_VALUE;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.TOP_QUERIES_LOCAL_INDEX_EXPORTER_ID;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.ENTRY_COUNT;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.EVICTIONS;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.HIT_COUNT;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.MISS_COUNT;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.SIZE_IN_BYTES;
import static org.opensearch.plugin.insights.core.utils.ExporterReaderUtils.generateLocalIndexDateHash;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.exporter.LocalIndexExporter;
import org.opensearch.plugin.insights.core.exporter.SinkType;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.healthStats.QueryInsightsHealthStats;
import org.opensearch.plugin.insights.rules.model.healthStats.TopQueriesHealthStats;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * Unit Tests for {@link QueryInsightsService}.
 */
public class QueryInsightsServiceTests extends OpenSearchTestCase {
    private final DateTimeFormatter format = DateTimeFormatter.ofPattern("YYYY.MM.dd", Locale.ROOT);
    private ThreadPool threadPool;
    private final Client client = mock(Client.class);
    private final NamedXContentRegistry namedXContentRegistry = mock(NamedXContentRegistry.class);
    private QueryInsightsService queryInsightsService;
    private QueryInsightsService newQueryInsightsService;
    private QueryInsightsService queryInsightsServiceSpy;
    private final AdminClient adminClient = mock(AdminClient.class);
    private final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
    private ClusterService clusterService;
    private ClusterService newClusterService;
    private LocalIndexExporter localIndexExporter;

    @Before
    public void setup() {
        localIndexExporter = mock(LocalIndexExporter.class);
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        this.threadPool = new TestThreadPool(
            "QueryInsightsHealthStatsTests",
            new ScalingExecutorBuilder(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR, 1, 5, TimeValue.timeValueMinutes(5))
        );
        clusterService = new ClusterService(settings, clusterSettings, threadPool);
        queryInsightsService = new QueryInsightsService(
            clusterService,
            threadPool,
            client,
            NoopMetricsRegistry.INSTANCE,
            namedXContentRegistry
        );
        queryInsightsService.enableCollection(MetricType.LATENCY, true);
        queryInsightsService.enableCollection(MetricType.CPU, true);
        queryInsightsService.enableCollection(MetricType.MEMORY, true);
        queryInsightsService.setQueryShapeGenerator(new QueryShapeGenerator(clusterService));
        queryInsightsServiceSpy = spy(queryInsightsService);

        MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );
        OperationalMetricsCounter.initialize("cluster", metricsRegistry);

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (clusterService != null) {
            IOUtils.close(clusterService);
        }
        if (newClusterService != null) {
            IOUtils.close(newClusterService);
        }
        if (queryInsightsService != null) {
            queryInsightsService.doClose();
        }
        if (newClusterService != null) {
            IOUtils.close(newClusterService);
        }
        if (newQueryInsightsService != null) {
            newQueryInsightsService.doClose();
        }

        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
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
        Map<String, Long> fieldTypeCacheStats = healthStats.getFieldTypeCacheStats();
        assertNotNull(fieldTypeCacheStats);
        assertEquals(5, fieldTypeCacheStats.size());
        assertTrue(fieldTypeCacheStats.containsKey(SIZE_IN_BYTES));
        assertTrue(fieldTypeCacheStats.containsKey(ENTRY_COUNT));
        assertTrue(fieldTypeCacheStats.containsKey(EVICTIONS));
        assertTrue(fieldTypeCacheStats.containsKey(HIT_COUNT));
        assertTrue(fieldTypeCacheStats.containsKey(MISS_COUNT));
    }

    public void testDeleteAllTopNIndices() {
        // Create 9 top_queries-* indices
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        for (int i = 1; i < 10; i++) {
            String indexName = "top_queries-2024.01.0"
                + i
                + "-"
                + generateLocalIndexDateHash(ZonedDateTime.of(2024, 1, i, 0, 0, 0, 0, ZoneId.of("UTC")).toLocalDate());
            long creationTime = Instant.now().minus(i, ChronoUnit.DAYS).toEpochMilli();

            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(SETTING_CREATION_DATE, creationTime)
                )
                .putMapping(
                    new MappingMetadata("_doc", Map.of("_meta", Map.of(QUERY_INSIGHTS_INDEX_TAG_NAME, TOP_QUERIES_INDEX_TAG_VALUE)))
                )
                .build();
            indexMetadataMap.put(indexName, indexMetadata);
        }
        // Create 5 user indices
        for (int i = 0; i < 5; i++) {
            String indexName = "my_index-" + i;
            long creationTime = Instant.now().minus(i, ChronoUnit.DAYS).toEpochMilli();

            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(SETTING_CREATION_DATE, creationTime)
                )
                .putMapping(
                    new MappingMetadata("_doc", Map.of("_meta", Map.of(QUERY_INSIGHTS_INDEX_TAG_NAME, TOP_QUERIES_INDEX_TAG_VALUE)))
                )
                .build();
            indexMetadataMap.put(indexName, indexMetadata);
        }

        queryInsightsService.deleteAllTopNIndices(client, indexMetadataMap, localIndexExporter);
        // All 10 indices should be deleted
        verify(localIndexExporter, times(9)).deleteSingleIndex(any(), any());
    }

    public void testDeleteExpiredTopNIndices() throws InterruptedException {
        // Create a new cluster state with expired index mappings
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        // Create a mock cluster state with expired indices
        ClusterState state = ClusterStateCreationUtils.stateWithActivePrimary("", true, 1 + randomInt(3), randomInt(2));
        RoutingTable.Builder routingTable = RoutingTable.builder(state.routingTable());
        // Create 9 top_queries-* indices with creation dates older than the retention period
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        for (int i = 1; i < 10; i++) {
            String indexName = "top_queries-2023.01.0"
                + i
                + "-"
                + generateLocalIndexDateHash(ZonedDateTime.now(ZoneOffset.UTC).toLocalDate());
            long creationTime = Instant.now().minus(i + 100, ChronoUnit.DAYS).toEpochMilli(); // Ensure indices are expired
            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(SETTING_CREATION_DATE, creationTime)
                )
                .putMapping(
                    new MappingMetadata("_doc", Map.of("_meta", Map.of(QUERY_INSIGHTS_INDEX_TAG_NAME, TOP_QUERIES_INDEX_TAG_VALUE)))
                )
                .build();
            indexMetadataMap.put(indexName, indexMetadata);
            routingTable.addAsRecovery(indexMetadata);
        }
        // Update the cluster state with the new indices
        ClusterState updatedState = ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).indices(indexMetadataMap).build())
            .routingTable(routingTable.build())
            .build();
        // Create a new cluster service with the updated state
        newClusterService = ClusterServiceUtils.createClusterService(threadPool, state.getNodes().getLocalNode(), clusterSettings);
        ClusterServiceUtils.setState(newClusterService, updatedState);
        // Initialize the QueryInsightsService with the new cluster service
        newQueryInsightsService = new QueryInsightsService(
            newClusterService,
            threadPool,
            client,
            NoopMetricsRegistry.INSTANCE,
            namedXContentRegistry
        );
        newQueryInsightsService.enableCollection(MetricType.LATENCY, true);
        newQueryInsightsService.enableCollection(MetricType.CPU, true);
        newQueryInsightsService.enableCollection(MetricType.MEMORY, true);
        newQueryInsightsService.setQueryShapeGenerator(new QueryShapeGenerator(newClusterService));
        // Create a local index exporter with a retention period of 7 days
        newQueryInsightsService.queryInsightsExporterFactory.createExporter(
            TOP_QUERIES_LOCAL_INDEX_EXPORTER_ID,
            SinkType.LOCAL_INDEX,
            "YYYY.MM.dd",
            ""
        );
        CountDownLatch latch = new CountDownLatch(9);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(indicesAdminClient).delete(any(), any());
        // Call the method under test
        newQueryInsightsService.deleteExpiredTopNIndices();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        // Verify that the correct number of indices are deleted
        // Default retention is 7 days, so all 9 indices should be deleted
        verify(client, times(9)).admin();
        verify(adminClient, times(9)).indices();
        verify(indicesAdminClient, times(9)).delete(any(), any());
    }
}
