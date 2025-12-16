/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.plugin.insights.core.service.QueryInsightsService.QUERY_INSIGHTS_INDEX_TAG_NAME;
import static org.opensearch.plugin.insights.core.service.QueryInsightsService.getInitialDelay;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.TOP_QUERIES_EXPORTER_ID;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.TOP_QUERIES_INDEX_TAG_VALUE;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.ENTRY_COUNT;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.EVICTIONS;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.HIT_COUNT;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.MISS_COUNT;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.SIZE_IN_BYTES;
import static org.opensearch.plugin.insights.core.utils.ExporterReaderUtils.generateLocalIndexDateHash;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
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
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.exporter.DebugExporter;
import org.opensearch.plugin.insights.core.exporter.LocalIndexExporter;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterFactory;
import org.opensearch.plugin.insights.core.exporter.SinkType;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReader;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReaderFactory;
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
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;
import org.opensearch.transport.client.IndicesAdminClient;

/**
 * Unit Tests for {@link QueryInsightsService}.
 */
public class QueryInsightsServiceTests extends OpenSearchTestCase {
    private final DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT);
    private ThreadPool threadPool;
    private final Client client = mock(Client.class);
    private final NamedXContentRegistry namedXContentRegistry = mock(NamedXContentRegistry.class);
    private QueryInsightsService queryInsightsService;
    private QueryInsightsService queryInsightsServiceSpy;
    private final AdminClient adminClient = mock(AdminClient.class);
    private final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
    private final ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
    private ClusterService clusterService;
    private LocalIndexExporter mockLocalIndexExporter;
    private LocalIndexLifecycleManager mockLocalIndexLifecycleManagerSpy;
    private DebugExporter mockDebugExporter;
    private QueryInsightsReader mockReader;
    private QueryInsightsExporterFactory queryInsightsExporterFactory;
    private QueryInsightsReaderFactory queryInsightsReaderFactory;

    @Before
    public void setup() {
        queryInsightsExporterFactory = mock(QueryInsightsExporterFactory.class);
        queryInsightsReaderFactory = mock(QueryInsightsReaderFactory.class);
        mockLocalIndexExporter = mock(LocalIndexExporter.class);
        mockDebugExporter = mock(DebugExporter.class);
        mockReader = mock(QueryInsightsReader.class);
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        this.threadPool = new TestThreadPool(
            "QueryInsightsServiceTests",
            new ScalingExecutorBuilder(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR, 1, 5, TimeValue.timeValueMinutes(5))
        );
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        mockLocalIndexLifecycleManagerSpy = spy(
            new LocalIndexLifecycleManager(threadPool, client, QueryInsightsSettings.DEFAULT_DELETE_AFTER_VALUE)
        );
        clusterService = new ClusterService(settings, clusterSettings, threadPool);
        queryInsightsService = new QueryInsightsService(
            clusterService,
            threadPool,
            client,
            NoopMetricsRegistry.INSTANCE,
            namedXContentRegistry,
            queryInsightsExporterFactory,
            queryInsightsReaderFactory
        );
        queryInsightsService.enableCollection(MetricType.LATENCY, true);
        queryInsightsService.enableCollection(MetricType.CPU, true);
        queryInsightsService.enableCollection(MetricType.MEMORY, true);
        queryInsightsService.setQueryShapeGenerator(new QueryShapeGenerator(clusterService));
        queryInsightsService.setLocalIndexLifecycleManager(mockLocalIndexLifecycleManagerSpy);
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
        if (clusterService != null) {
            IOUtils.close(clusterService);
        }
        if (queryInsightsService != null) {
            queryInsightsService.doClose();
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
            queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false, null, null, null, null).size()
        );
    }

    public void testDoStart() throws IOException {
        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(Map.of());
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.getFirst();
        try {
            updatedQueryInsightsService.doStart();
        } catch (Exception e) {
            fail(String.format(Locale.ROOT, "No exception expected when starting query insights service: %s", e.getMessage()));
        }
        assertEquals(1, updatedQueryInsightsService.scheduledFutures.size());
        assertFalse(updatedQueryInsightsService.scheduledFutures.getFirst().isCancelled());
        assertNotNull(updatedQueryInsightsService.deleteIndicesScheduledFuture);
        assertFalse(updatedQueryInsightsService.deleteIndicesScheduledFuture.isCancelled());

        updatedQueryInsightsService.doStop();
        IOUtils.close(updatedService.get(1));
        updatedQueryInsightsService.doClose();
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
            queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false, null, null, null, null).size()
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
        assertEquals(
            1,
            queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false, null, null, null, null).size()
        );
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
        assertEquals(
            2,
            queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false, null, null, null, null).size()
        );
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

    public void testDeleteAllTopNIndices() throws IOException, InterruptedException {
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
        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(indexMetadataMap);
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);
        ClusterService updatedClusterService = (ClusterService) updatedService.get(1);

        CountDownLatch latch = new CountDownLatch(9);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(mockLocalIndexLifecycleManagerSpy).deleteSingleIndex(any(), any());

        // Call the method under test
        updatedQueryInsightsService.getLocalIndexLifecycleManager().deleteAllTopNIndices();

        latch.await(5, TimeUnit.SECONDS);

        // Clean up resources
        IOUtils.close(updatedClusterService);
        updatedQueryInsightsService.doClose();

        // All 10 top_queries-* indices should be deleted, while none of the users indices should be deleted
        verify(mockLocalIndexLifecycleManagerSpy, times(9)).deleteSingleIndex(any(), any());
    }

    public void testDeleteExpiredTopNIndices() throws InterruptedException, IOException {
        logger.info("starting testDeleteExpiredTopNIndices");
        // Test with a new cluster state with expired index mappings
        // Create 9 top_queries-* indices with creation dates older than the retention period
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        for (int i = 1; i < 10; i++) {
            LocalDate date = LocalDate.of(2023, 1, i);
            String indexName = "top_queries-" + date.format(format) + "-" + generateLocalIndexDateHash(date);
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
        // Create some non Query Insights indices
        for (String indexName : List.of("logs-1", "logs-2", "top_queries-2023.01.01-12345", "top_queries-2023.01.02-12345")) {
            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(SETTING_CREATION_DATE, Instant.now().minus(100, ChronoUnit.DAYS).toEpochMilli())
                )
                .build();
            indexMetadataMap.put(indexName, indexMetadata);
        }

        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(indexMetadataMap);
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);
        ClusterService updatedClusterService = (ClusterService) updatedService.get(1);

        final int expectedIndicesDeleted = 2;
        CountDownLatch latch = new CountDownLatch(expectedIndicesDeleted);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(mockLocalIndexLifecycleManagerSpy).deleteSingleIndex(any(), any());

        // Call the method under test
        updatedQueryInsightsService.getLocalIndexLifecycleManager().deleteExpiredTopNIndices();
        latch.await(5, TimeUnit.SECONDS);

        // Clean up resources
        IOUtils.close(updatedClusterService);
        updatedQueryInsightsService.doClose();

        verify(mockLocalIndexLifecycleManagerSpy, times(expectedIndicesDeleted)).deleteSingleIndex(any(), any());
    }

    public void testvalidateDeleteAfter() {
        LocalIndexLifecycleManager localIndexLifecycleManager = queryInsightsService.getLocalIndexLifecycleManager();
        localIndexLifecycleManager.validateDeleteAfter(7);
        localIndexLifecycleManager.validateDeleteAfter(180);
        localIndexLifecycleManager.validateDeleteAfter(0);
        assertThrows(IllegalArgumentException.class, () -> { localIndexLifecycleManager.validateDeleteAfter(-1); });
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            localIndexLifecycleManager.validateDeleteAfter(181);
        });
        assertEquals("Invalid delete_after_days setting [181], value should be an integer between 0 and 180.", exception.getMessage());
    }

    public void testSetDeleteAfterToNotZero() {
        queryInsightsServiceSpy.getLocalIndexLifecycleManager().setDeleteAfterAndDelete(100);
        verify(mockLocalIndexLifecycleManagerSpy, times(0)).deleteAllTopNIndices();
    }

    public void testSetDeleteAfterToZero() {
        queryInsightsServiceSpy.getLocalIndexLifecycleManager().setDeleteAfterAndDelete(0);
        verify(mockLocalIndexLifecycleManagerSpy, times(1)).deleteAllTopNIndices();
    }

    public void testSetExporterAndReaderType_SwitchFromLocalIndexToNone() throws IOException {
        // Mock current exporter and reader
        queryInsightsServiceSpy.sinkType = SinkType.LOCAL_INDEX;
        // Mock current exporter and reader
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(mockLocalIndexExporter);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);

        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.NONE);
        // Verify local indices are not deleted
        verify(mockLocalIndexLifecycleManagerSpy, times(0)).deleteSingleIndex(any(), any());
        // Verify exporter and reader are closed
        verify(queryInsightsExporterFactory, times(1)).closeExporter(mockLocalIndexExporter);
        verify(queryInsightsReaderFactory, times(1)).closeReader(mockReader);
        // Verify exporter is set to NONE
        assertEquals(SinkType.NONE, queryInsightsServiceSpy.sinkType);
    }

    public void testSetExporterAndReaderType_SwitchFromLocalIndexToDebug() throws IOException {
        // Mock current exporter and reader
        queryInsightsServiceSpy.sinkType = SinkType.LOCAL_INDEX;
        // Mock current exporter and reader
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(mockLocalIndexExporter);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);

        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.DEBUG);
        // Verify local indices are not deleted
        verify(mockLocalIndexLifecycleManagerSpy, times(0)).deleteSingleIndex(any(), any());
        // Verify exporter and reader are closed
        verify(queryInsightsExporterFactory, times(1)).closeExporter(mockLocalIndexExporter);
        verify(queryInsightsReaderFactory, times(1)).closeReader(mockReader);
        // Verify exporter is set to NONE
        assertEquals(SinkType.DEBUG, queryInsightsServiceSpy.sinkType);
    }

    public void testSetExporterAndReaderType_SwitchFromNoneToLocalIndex() throws IOException {
        queryInsightsServiceSpy.sinkType = SinkType.NONE;
        // Mock current exporter and reader
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(null);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(null);
        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.LOCAL_INDEX);
        // Verify new local index exporter setup
        // 2 times, one for initialization, one for the above method call
        verify(queryInsightsExporterFactory, times(2)).createLocalIndexExporter(
            eq(TopQueriesService.TOP_QUERIES_EXPORTER_ID),
            anyString(),
            anyString()
        );
        verify(queryInsightsReaderFactory, times(2)).createLocalIndexReader(
            eq(TopQueriesService.TOP_QUERIES_READER_ID),
            anyString(),
            eq(namedXContentRegistry)
        );
        // Verify local indices are not deleted
        verify(mockLocalIndexLifecycleManagerSpy, times(0)).deleteSingleIndex(any(), any());
        // Verify exporter and reader are closed
        verify(queryInsightsExporterFactory, times(0)).closeExporter(any());
        verify(queryInsightsReaderFactory, times(0)).closeReader(any());
        assertEquals(SinkType.LOCAL_INDEX, queryInsightsServiceSpy.sinkType);
    }

    public void testSetExporterAndReaderType_SwitchFromNoneToDebug() throws IOException {
        queryInsightsServiceSpy.sinkType = SinkType.NONE;
        // Mock current exporter and reader
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(null);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(null);

        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.DEBUG);
        // Verify new local index exporter setup
        verify(queryInsightsExporterFactory, times(1)).createDebugExporter(eq(TopQueriesService.TOP_QUERIES_EXPORTER_ID));
        // Verify local indices are not deleted
        verify(mockLocalIndexLifecycleManagerSpy, times(0)).deleteSingleIndex(any(), any());
        // Verify exporter and reader are closed
        verify(queryInsightsExporterFactory, times(0)).closeExporter(any());
        verify(queryInsightsReaderFactory, times(0)).closeReader(any());
        assertEquals(SinkType.DEBUG, queryInsightsServiceSpy.sinkType);
    }

    public void testSetExporterAndReaderType_SwitchFromDebugToLocalIndex() throws IOException {
        queryInsightsServiceSpy.sinkType = SinkType.DEBUG;
        // Mock current exporter and reader
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(mockDebugExporter);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);

        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.LOCAL_INDEX);
        // Verify new local index exporter setup
        // 2 times, one for initialization, one for the above method call
        verify(queryInsightsExporterFactory, times(2)).createLocalIndexExporter(
            eq(TopQueriesService.TOP_QUERIES_EXPORTER_ID),
            anyString(),
            anyString()
        );
        verify(queryInsightsReaderFactory, times(2)).createLocalIndexReader(
            eq(TopQueriesService.TOP_QUERIES_READER_ID),
            anyString(),
            eq(namedXContentRegistry)
        );
        // Verify local indices are not deleted
        verify(mockLocalIndexLifecycleManagerSpy, times(0)).deleteSingleIndex(any(), any());
        // Verify exporter and reader are closed
        verify(queryInsightsExporterFactory, times(1)).closeExporter(mockDebugExporter);
        verify(queryInsightsReaderFactory, times(1)).closeReader(mockReader);
        assertEquals(SinkType.LOCAL_INDEX, queryInsightsServiceSpy.sinkType);
    }

    public void testSetExporterAndReaderType_SwitchFromDebugToNone() throws IOException {
        queryInsightsServiceSpy.sinkType = SinkType.DEBUG;
        // Mock current exporter and reader
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(mockDebugExporter);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);
        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.NONE);

        // Verify local indices are not deleted
        verify(mockLocalIndexLifecycleManagerSpy, times(0)).deleteSingleIndex(any(), any());
        // Verify exporter and reader are closed
        verify(queryInsightsExporterFactory, times(1)).closeExporter(mockDebugExporter);
        verify(queryInsightsReaderFactory, times(1)).closeReader(mockReader);
        assertEquals(SinkType.NONE, queryInsightsServiceSpy.sinkType);
    }

    public void testSetExporterAndReaderType_CloseWithException() throws IOException {
        queryInsightsServiceSpy.sinkType = SinkType.LOCAL_INDEX;
        // Mock current exporter that throws an exception when closing
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(mockLocalIndexExporter);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);
        doThrow(new IOException("Exporter close error")).when(queryInsightsExporterFactory).closeExporter(mockLocalIndexExporter);
        doThrow(new IOException("Reader close error")).when(queryInsightsReaderFactory).closeReader(mockReader);
        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.DEBUG);
        // Verify exception handling
        verify(queryInsightsExporterFactory, times(1)).closeExporter(mockLocalIndexExporter);
        verify(queryInsightsReaderFactory, times(1)).closeReader(any());
        // Ensure new exporter is still created
        verify(queryInsightsExporterFactory, times(1)).createDebugExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID);
    }

    public void testGetInitialDelay() {
        //// First Test Case (Normal Case) ////
        Instant instantNormal = Instant.parse("2025-03-26T10:15:30Z");
        Instant startOfNextDayNormal = instantNormal.truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.DAYS);
        final long expectedDelayNormal = Duration.between(instantNormal, startOfNextDayNormal).toMillis();
        assertEquals(expectedDelayNormal, getInitialDelay(instantNormal));

        //// Second Test Case (Edge Case: Midnight UTC) ////
        Instant instantEdge = Instant.parse("2025-03-26T00:00:00Z");
        Instant startOfNextDayEdge = instantEdge.plus(1, ChronoUnit.DAYS);
        final long expectedDelayEdge = Duration.between(instantEdge, startOfNextDayEdge).toMillis();
        assertEquals(expectedDelayEdge, getInitialDelay(instantEdge));
    }

    public void testEnableCollectionClearsQueueWhenAllFeaturesDisabled() {
        // Add some records to the queue
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(5);
        for (SearchQueryRecord record : records) {
            queryInsightsService.addRecord(record);
        }

        // Verify queue has records
        QueryInsightsHealthStats healthStats = queryInsightsService.getHealthStats();
        assertEquals(5, healthStats.getQueryRecordsQueueSize());

        // Disable all features one by one
        queryInsightsService.enableCollection(MetricType.LATENCY, false);
        queryInsightsService.enableCollection(MetricType.CPU, false);

        // Queue should not be cleared yet as MEMORY is still enabled
        healthStats = queryInsightsService.getHealthStats();
        assertEquals(5, healthStats.getQueryRecordsQueueSize());

        // Disable the last feature
        queryInsightsService.enableCollection(MetricType.MEMORY, false);

        // Now the queue should be cleared
        healthStats = queryInsightsService.getHealthStats();
        assertEquals(0, healthStats.getQueryRecordsQueueSize());
    }

    public void testEnableCollectionDoesNotClearQueueWhenOtherFeaturesEnabled() {
        // Add some records to the queue
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(5);
        for (SearchQueryRecord record : records) {
            queryInsightsService.addRecord(record);
        }

        // Verify queue has records
        QueryInsightsHealthStats healthStats = queryInsightsService.getHealthStats();
        assertEquals(5, healthStats.getQueryRecordsQueueSize());

        // Disable only LATENCY, but keep CPU and MEMORY enabled
        queryInsightsService.enableCollection(MetricType.LATENCY, false);

        // Queue should NOT be cleared as other features are still enabled
        healthStats = queryInsightsService.getHealthStats();
        assertEquals(5, healthStats.getQueryRecordsQueueSize());

        // Disable CPU as well, but keep MEMORY enabled
        queryInsightsService.enableCollection(MetricType.CPU, false);

        // Queue should still NOT be cleared
        healthStats = queryInsightsService.getHealthStats();
        assertEquals(5, healthStats.getQueryRecordsQueueSize());
    }

    // Util functions
    private List<AbstractLifecycleComponent> createQueryInsightsServiceWithIndexState(Map<String, IndexMetadata> indexMetadataMap) {
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        // Create a mock cluster state with expired indices
        ClusterState state = ClusterStateCreationUtils.stateWithActivePrimary("", true, 1 + randomInt(3), randomInt(2));
        RoutingTable.Builder routingTable = RoutingTable.builder(state.routingTable());
        indexMetadataMap.forEach((indexName, indexMetadata) -> { routingTable.addAsRecovery(indexMetadata); });
        // Update the cluster state with the new indices
        ClusterState updatedState = ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).indices(indexMetadataMap).build())
            .routingTable(routingTable.build())
            .build();
        // Create a new cluster service with the updated state
        ClusterService updatedClusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            state.getNodes().getLocalNode(),
            clusterSettings
        );
        ClusterServiceUtils.setState(updatedClusterService, updatedState);

        ClusterStateResponse mockClusterStateResponse = mock(ClusterStateResponse.class);
        when(mockClusterStateResponse.getState()).thenReturn(updatedState);

        doAnswer(invocation -> {
            ActionListener<ClusterStateResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(mockClusterStateResponse);
            return null;
        }).when(clusterAdminClient).state(any(ClusterStateRequest.class), any(ActionListener.class));

        // Initialize the QueryInsightsService with the new cluster service
        QueryInsightsService updatedQueryInsightsService = new QueryInsightsService(
            updatedClusterService,
            threadPool,
            client,
            NoopMetricsRegistry.INSTANCE,
            namedXContentRegistry,
            new QueryInsightsExporterFactory(client, clusterService),
            new QueryInsightsReaderFactory(client)
        );
        updatedQueryInsightsService.enableCollection(MetricType.LATENCY, true);
        updatedQueryInsightsService.enableCollection(MetricType.CPU, true);
        updatedQueryInsightsService.enableCollection(MetricType.MEMORY, true);
        updatedQueryInsightsService.setQueryShapeGenerator(new QueryShapeGenerator(updatedClusterService));
        updatedQueryInsightsService.setLocalIndexLifecycleManager(mockLocalIndexLifecycleManagerSpy);
        // Create a local index exporter with a retention period of 7 days
        updatedQueryInsightsService.queryInsightsExporterFactory.createLocalIndexExporter(TOP_QUERIES_EXPORTER_ID, "YYYY.MM.dd", "");
        return List.of(updatedQueryInsightsService, updatedClusterService);
    }
}
