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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.plugin.insights.core.exporter.LocalIndexExporter.generateLocalIndexDateHash;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.isTopQueriesIndex;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.validateExporterDeleteAfter;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterFactory;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReaderFactory;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.healthStats.TopQueriesHealthStats;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

/**
 * Unit Tests for {@link QueryInsightsService}.
 */
public class TopQueriesServiceTests extends OpenSearchTestCase {
    private TopQueriesService topQueriesService;
    private final Client client = mock(Client.class);
    private final ThreadPool threadPool = mock(ThreadPool.class);
    private final QueryInsightsExporterFactory queryInsightsExporterFactory = mock(QueryInsightsExporterFactory.class);
    private final QueryInsightsReaderFactory queryInsightsReaderFactory = mock(QueryInsightsReaderFactory.class);
    private final AdminClient adminClient = mock(AdminClient.class);
    private final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);

    @Before
    public void setup() {
        topQueriesService = new TopQueriesService(
            client,
            MetricType.LATENCY,
            threadPool,
            queryInsightsExporterFactory,
            queryInsightsReaderFactory
        );
        topQueriesService.setTopNSize(Integer.MAX_VALUE);
        topQueriesService.setWindowSize(new TimeValue(Long.MAX_VALUE));
        topQueriesService.setEnabled(true);

        MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );
        OperationalMetricsCounter.initialize("cluster", metricsRegistry);

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
    }

    public void testIngestQueryDataWithLargeWindow() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        topQueriesService.consumeRecords(records);
        assertTrue(
            QueryInsightsTestUtils.checkRecordsEqualsWithoutOrder(
                topQueriesService.getTopQueriesRecords(false, null, null, null),
                records,
                MetricType.LATENCY
            )
        );
    }

    public void testRollingWindows() {
        List<SearchQueryRecord> records;
        // Create 5 records at Now - 10 minutes to make sure they belong to the last window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(5, 5, System.currentTimeMillis() - 1000 * 60 * 10, 0);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(0, topQueriesService.getTopQueriesRecords(true, null, null, null).size());

        // Create 10 records at now + 1 minute, to make sure they belong to the current window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(10, 10, System.currentTimeMillis() + 1000 * 60, 0);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(10, topQueriesService.getTopQueriesRecords(true, null, null, null).size());
    }

    public void testSmallNSize() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        topQueriesService.setTopNSize(1);
        topQueriesService.consumeRecords(records);
        assertEquals(1, topQueriesService.getTopQueriesRecords(false, null, null, null).size());
    }

    public void testValidateTopNSize() {
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.validateTopNSize(QueryInsightsSettings.MAX_N_SIZE + 1); });
    }

    public void testValidateNegativeTopNSize() {
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.validateTopNSize(-1); });
    }

    public void testGetTopQueriesWhenNotEnabled() {
        topQueriesService.setEnabled(false);
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.getTopQueriesRecords(false, null, null, null); });
    }

    public void testValidateWindowSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            topQueriesService.validateWindowSize(new TimeValue(QueryInsightsSettings.MAX_WINDOW_SIZE.getSeconds() + 1, TimeUnit.SECONDS));
        });
        assertThrows(IllegalArgumentException.class, () -> {
            topQueriesService.validateWindowSize(new TimeValue(QueryInsightsSettings.MIN_WINDOW_SIZE.getSeconds() - 1, TimeUnit.SECONDS));
        });
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.validateWindowSize(new TimeValue(2, TimeUnit.DAYS)); });
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.validateWindowSize(new TimeValue(7, TimeUnit.MINUTES)); });
    }

    private static void runUntilTimeoutOrFinish(DeterministicTaskQueue deterministicTaskQueue, long duration) {
        final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + duration;
        while (deterministicTaskQueue.getCurrentTimeMillis() < endTime
            && (deterministicTaskQueue.hasRunnableTasks() || deterministicTaskQueue.hasDeferredTasks())) {
            if (deterministicTaskQueue.hasDeferredTasks() && randomBoolean()) {
                deterministicTaskQueue.advanceTime();
            } else if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            }
        }
    }

    public void testRollingWindowsWithSameGroup() {
        topQueriesService.setGrouping(GroupingType.SIMILARITY);
        List<SearchQueryRecord> records;
        // Create 5 records at Now - 10 minutes to make sure they belong to the last window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(5, 5, System.currentTimeMillis() - 1000 * 60 * 10, 0);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(0, topQueriesService.getTopQueriesRecords(true, null, null, null).size());

        // Create 10 records at now + 1 minute, to make sure they belong to the current window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(10, 10, System.currentTimeMillis() + 1000 * 60, 0);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(10, topQueriesService.getTopQueriesRecords(true, null, null, null).size());
    }

    public void testRollingWindowsWithDifferentGroup() {
        topQueriesService.setGrouping(GroupingType.SIMILARITY);
        List<SearchQueryRecord> records;
        // Create 5 records at Now - 10 minutes to make sure they belong to the last window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(5, 5, System.currentTimeMillis() - 1000 * 60 * 10, 0);
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);

        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(0, topQueriesService.getTopQueriesRecords(true, null, null, null).size());

        // Create 10 records at now + 1 minute, to make sure they belong to the current window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(10, 10, System.currentTimeMillis() + 1000 * 60, 0);
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(1, topQueriesService.getTopQueriesRecords(true, null, null, null).size());
    }

    public void testGetHealthStats_EmptyService() {
        TopQueriesHealthStats healthStats = topQueriesService.getHealthStats();
        // Validate the health stats
        assertNotNull(healthStats);
        assertEquals(0, healthStats.getTopQueriesHeapSize());
        assertNotNull(healthStats.getQueryGrouperHealthStats());
        assertEquals(0, healthStats.getQueryGrouperHealthStats().getQueryGroupCount());
        assertEquals(0, healthStats.getQueryGrouperHealthStats().getQueryGroupHeapSize());
    }

    public void testGetHealthStats_WithData() {
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        topQueriesService.consumeRecords(records);
        TopQueriesHealthStats healthStats = topQueriesService.getHealthStats();
        assertNotNull(healthStats);
        assertEquals(2, healthStats.getTopQueriesHeapSize()); // Since we added two records
        assertNotNull(healthStats.getQueryGrouperHealthStats());
        // Assuming no grouping by default, expect QueryGroupCount to be 0
        assertEquals(0, healthStats.getQueryGrouperHealthStats().getQueryGroupCount());
    }

    public void testValidateExporterDeleteAfter() {
        validateExporterDeleteAfter(7);
        validateExporterDeleteAfter(180);
        validateExporterDeleteAfter(1);
        assertThrows(IllegalArgumentException.class, () -> { validateExporterDeleteAfter(-1); });
        assertThrows(IllegalArgumentException.class, () -> { validateExporterDeleteAfter(0); });
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> { validateExporterDeleteAfter(181); });
        assertEquals(
            "Invalid exporter delete_after_days setting [181], value should be an integer between 1 and 180.",
            exception.getMessage()
        );
    }

    public void testDeleteAllTopNIndices() {
        // Create 9 top_queries-* indices
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        for (int i = 1; i < 10; i++) {
            String indexName = "top_queries-2024.01.0" + i + "-" + generateLocalIndexDateHash();
            long creationTime = Instant.now().minus(i, ChronoUnit.DAYS).toEpochMilli();

            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(SETTING_CREATION_DATE, creationTime)
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
                .build();
            indexMetadataMap.put(indexName, indexMetadata);
        }

        topQueriesService.deleteAllTopNIndices(indexMetadataMap);
        // All 10 indices should be delete
        verify(client, times(9)).admin();
        verify(adminClient, times(9)).indices();
        verify(indicesAdminClient, times(9)).delete(any(), any());
    }

    public void testIsTopQueriesIndex() {
        assertTrue(isTopQueriesIndex("top_queries-2024.01.01-01234"));
        assertTrue(isTopQueriesIndex("top_queries-2025.12.12-99999"));

        assertFalse(isTopQueriesIndex("top_queries-2024.01.01-012345"));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01-0123w"));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01"));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.32-01234"));
        assertFalse(isTopQueriesIndex("top_queries-01234"));
        assertFalse(isTopQueriesIndex("top_querie-2024.01.01-01234"));
        assertFalse(isTopQueriesIndex("2024.01.01-01234"));
    }

    public void testTopQueriesForId() {
        // Generate the records for id-1 and id-2
        final List<SearchQueryRecord> records1 = QueryInsightsTestUtils.generateQueryInsightRecords(2, "id-1");
        final List<SearchQueryRecord> records2 = QueryInsightsTestUtils.generateQueryInsightRecords(2, "id-2");

        records1.addAll(records2);
        topQueriesService.consumeRecords(records1);

        // Validate that the records for "id-1" are correctly retrieved
        assertTrue(
            QueryInsightsTestUtils.checkRecordsEqualsWithoutOrder(
                topQueriesService.getTopQueriesRecords(false, null, null, "id-1"),
                records1.stream().filter(record -> "id-1".equals(record.getId())).collect(Collectors.toList()),
                MetricType.LATENCY
            )
        );

        // Validate that the records for "id-2" are correctly retrieved
        assertTrue(
            QueryInsightsTestUtils.checkRecordsEqualsWithoutOrder(
                topQueriesService.getTopQueriesRecords(false, null, null, "id-2"),
                records1.stream().filter(record -> "id-2".equals(record.getId())).collect(Collectors.toList()),
                MetricType.LATENCY
            )
        );
    }
}
