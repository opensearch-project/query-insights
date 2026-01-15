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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.plugin.insights.core.service.QueryInsightsService.QUERY_INSIGHTS_INDEX_TAG_NAME;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.TOP_QUERIES_INDEX_TAG_VALUE;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.isTopQueriesIndex;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.Version;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterFactory;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReader;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReaderFactory;
import org.opensearch.plugin.insights.core.utils.ExporterReaderUtils;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.healthStats.TopQueriesHealthStats;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

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
                topQueriesService.getTopQueriesRecords(false, null, null, null, null),
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
        assertEquals(0, topQueriesService.getTopQueriesRecords(true, null, null, null, null).size());

        // Create 10 records at now + 1 minute, to make sure they belong to the current window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(10, 10, System.currentTimeMillis() + 1000 * 60, 0);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(10, topQueriesService.getTopQueriesRecords(true, null, null, null, null).size());
    }

    public void testSmallNSize() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        topQueriesService.setTopNSize(1);
        topQueriesService.consumeRecords(records);
        assertEquals(1, topQueriesService.getTopQueriesRecords(false, null, null, null, null).size());
    }

    public void testValidateTopNSize() {
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.validateTopNSize(QueryInsightsSettings.MAX_N_SIZE + 1); });
    }

    public void testValidateNegativeTopNSize() {
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.validateTopNSize(-1); });
    }

    public void testGetTopQueriesWhenNotEnabled() {
        topQueriesService.setEnabled(false);
        assertEquals(0, topQueriesService.getTopQueriesRecords(false, null, null, null, null).size());
    }

    public void testSetEnabledFalseClearsSnapshots() {
        // Add records to the service
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(5);
        topQueriesService.consumeRecords(records);

        // Verify records are present
        assertEquals(5, topQueriesService.getTopQueriesRecords(false, null, null, null, null).size());

        // Disable the service
        topQueriesService.setEnabled(false);

        // Re-enable to check if snapshots were cleared
        topQueriesService.setEnabled(true);

        // Snapshots should be empty after disabling
        assertEquals(0, topQueriesService.getTopQueriesRecords(false, null, null, null, null).size());
    }

    public void testSetEnabledFalseReturnsEmptyResults() {
        // Add records to the service while enabled
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(5);
        topQueriesService.consumeRecords(records);

        // Verify records are present
        assertEquals(5, topQueriesService.getTopQueriesRecords(false, null, null, null, null).size());

        // Disable the service
        topQueriesService.setEnabled(false);

        // Should return empty results immediately when disabled
        assertEquals(0, topQueriesService.getTopQueriesRecords(false, null, null, null, null).size());

        // Also test with includeLastWindow = true
        assertEquals(0, topQueriesService.getTopQueriesRecords(true, null, null, null, null).size());
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
        assertEquals(0, topQueriesService.getTopQueriesRecords(true, null, null, null, null).size());

        // Create 10 records at now + 1 minute, to make sure they belong to the current window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(10, 10, System.currentTimeMillis() + 1000 * 60, 0);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(10, topQueriesService.getTopQueriesRecords(true, null, null, null, null).size());
    }

    public void testRollingWindowsWithDifferentGroup() {
        topQueriesService.setGrouping(GroupingType.SIMILARITY);
        List<SearchQueryRecord> records;
        // Create 5 records at Now - 10 minutes to make sure they belong to the last window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(5, 5, System.currentTimeMillis() - 1000 * 60 * 10, 0);
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);

        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(0, topQueriesService.getTopQueriesRecords(true, null, null, null, null).size());

        // Create 10 records at now + 1 minute, to make sure they belong to the current window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(10, 10, System.currentTimeMillis() + 1000 * 60, 0);
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(1, topQueriesService.getTopQueriesRecords(true, null, null, null, null).size());
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

    private IndexMetadata createValidIndexMetadata(String indexName) {
        // valid index metadata
        long creationTime = Instant.now().toEpochMilli();
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put("index.version.created", Version.CURRENT.id)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put(SETTING_CREATION_DATE, creationTime)
            )
            .putMapping(new MappingMetadata("_doc", Map.of("_meta", Map.of(QUERY_INSIGHTS_INDEX_TAG_NAME, TOP_QUERIES_INDEX_TAG_VALUE))))
            .build();
    }

    public void testIsTopQueriesIndexWithValidMetaData() {
        // Generate correct hash values for test dates
        LocalDate date1 = LocalDate.parse("2024.01.01", DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT));
        LocalDate date2 = LocalDate.parse("2025.12.12", DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT));
        String hash1 = ExporterReaderUtils.generateLocalIndexDateHash(date1);
        String hash2 = ExporterReaderUtils.generateLocalIndexDateHash(date2);

        // Test with valid index names and correct hash values
        assertTrue(isTopQueriesIndex("top_queries-2024.01.01-" + hash1, createValidIndexMetadata("top_queries-2024.01.01-" + hash1)));
        assertTrue(isTopQueriesIndex("top_queries-2025.12.12-" + hash2, createValidIndexMetadata("top_queries-2025.12.12-" + hash2)));

        // Test with invalid index names (wrong hash, format issues, etc.)
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01-012345", createValidIndexMetadata("top_queries-2024.01.01-012345")));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01-0123w", createValidIndexMetadata("top_queries-2024.01.01-0123w")));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01", createValidIndexMetadata("top_queries-2024.01.01")));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.32-" + hash1, createValidIndexMetadata("top_queries-2024.01.32-" + hash1)));
        assertFalse(isTopQueriesIndex("top_queries-01234", createValidIndexMetadata("top_queries-01234")));
        assertFalse(isTopQueriesIndex("top_querie-2024.01.01-" + hash1, createValidIndexMetadata("top_querie-2024.01.01-" + hash1)));
        assertFalse(isTopQueriesIndex("2024.01.01-" + hash1, createValidIndexMetadata("2024.01.01-" + hash1)));
        assertFalse(isTopQueriesIndex("any_index", createValidIndexMetadata("any_index")));
        assertFalse(isTopQueriesIndex("", createValidIndexMetadata("")));
        assertFalse(isTopQueriesIndex("_customer_index", createValidIndexMetadata("_customer_index")));

        // Test with a valid date but incorrect hash
        String wrongHash = hash1.equals("00000") ? "11111" : "00000";
        assertFalse(
            isTopQueriesIndex("top_queries-2024.01.01-" + wrongHash, createValidIndexMetadata("top_queries-2024.01.01-" + wrongHash))
        );
    }

    private IndexMetadata createIndexMetadataWithEmptyMapping(String indexName) {
        long creationTime = Instant.now().toEpochMilli();
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put("index.version.created", Version.CURRENT.id)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put(SETTING_CREATION_DATE, creationTime)
            )
            .build();
    }

    public void testIsTopQueriesIndexWithEmptyMetaData() {
        // Generate correct hash values for test dates
        LocalDate date1 = LocalDate.parse("2024.01.01", DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT));
        LocalDate date2 = LocalDate.parse("2025.12.12", DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT));
        String hash1 = ExporterReaderUtils.generateLocalIndexDateHash(date1);
        String hash2 = ExporterReaderUtils.generateLocalIndexDateHash(date2);

        assertFalse(
            isTopQueriesIndex("top_queries-2024.01.01-" + hash1, createIndexMetadataWithEmptyMapping("top_queries-2024.01.01-" + hash1))
        );
        assertFalse(
            isTopQueriesIndex("top_queries-2025.12.12-" + hash2, createIndexMetadataWithEmptyMapping("top_queries-2025.12.12-" + hash2))
        );
        assertFalse(
            isTopQueriesIndex("top_queries-2024.01.01-012345", createIndexMetadataWithEmptyMapping("top_queries-2024.01.01-012345"))
        );
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01-0123w", createIndexMetadataWithEmptyMapping("top_queries-2024.01.01-0123w")));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01", createIndexMetadataWithEmptyMapping("top_queries-2024.01.01")));
        assertFalse(
            isTopQueriesIndex("top_queries-2024.01.32-" + hash1, createIndexMetadataWithEmptyMapping("top_queries-2024.01.32-" + hash1))
        );
        assertFalse(isTopQueriesIndex("top_queries-01234", createIndexMetadataWithEmptyMapping("top_queries-01234")));
        assertFalse(
            isTopQueriesIndex("top_querie-2024.01.01-" + hash1, createIndexMetadataWithEmptyMapping("top_querie-2024.01.01-" + hash1))
        );
        assertFalse(isTopQueriesIndex("2024.01.01-" + hash1, createIndexMetadataWithEmptyMapping("2024.01.01-" + hash1)));
        assertFalse(isTopQueriesIndex("any_index", createIndexMetadataWithEmptyMapping("any_index")));
        assertFalse(isTopQueriesIndex("", createIndexMetadataWithEmptyMapping("")));
        assertFalse(isTopQueriesIndex("_customer_index", createIndexMetadataWithEmptyMapping("_customer_index")));
    }

    private IndexMetadata createIndexMetadataWithDifferentValue(String indexName) {
        long creationTime = Instant.now().toEpochMilli();
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put("index.version.created", Version.CURRENT.id)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put(SETTING_CREATION_DATE, creationTime)
            )
            .putMapping(new MappingMetadata("_doc", Map.of("_meta", Map.of(QUERY_INSIGHTS_INDEX_TAG_NAME, "someOtherTag"))))
            .build();
    }

    public void testIsTopQueriesIndexWithDifferentMetaData() {
        // Generate correct hash values for test dates
        LocalDate date1 = LocalDate.parse("2024.01.01", DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT));
        LocalDate date2 = LocalDate.parse("2025.12.12", DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT));
        String hash1 = ExporterReaderUtils.generateLocalIndexDateHash(date1);
        String hash2 = ExporterReaderUtils.generateLocalIndexDateHash(date2);

        assertFalse(
            isTopQueriesIndex("top_queries-2024.01.01-" + hash1, createIndexMetadataWithDifferentValue("top_queries-2024.01.01-" + hash1))
        );
        assertFalse(
            isTopQueriesIndex("top_queries-2025.12.12-" + hash2, createIndexMetadataWithDifferentValue("top_queries-2025.12.12-" + hash2))
        );
        assertFalse(
            isTopQueriesIndex("top_queries-2024.01.01-012345", createIndexMetadataWithDifferentValue("top_queries-2024.01.01-012345"))
        );
        assertFalse(
            isTopQueriesIndex("top_queries-2024.01.01-0123w", createIndexMetadataWithDifferentValue("top_queries-2024.01.01-0123w"))
        );
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01", createIndexMetadataWithDifferentValue("top_queries-2024.01.01")));
        assertFalse(
            isTopQueriesIndex("top_queries-2024.01.32-" + hash1, createIndexMetadataWithDifferentValue("top_queries-2024.01.32-" + hash1))
        );
        assertFalse(isTopQueriesIndex("top_queries-01234", createIndexMetadataWithDifferentValue("top_queries-01234")));
        assertFalse(
            isTopQueriesIndex("top_querie-2024.01.01-" + hash1, createIndexMetadataWithDifferentValue("top_querie-2024.01.01-" + hash1))
        );
        assertFalse(isTopQueriesIndex("2024.01.01-" + hash1, createIndexMetadataWithDifferentValue("2024.01.01-" + hash1)));
        assertFalse(isTopQueriesIndex("any_index", createIndexMetadataWithDifferentValue("any_index")));
        assertFalse(isTopQueriesIndex("", createIndexMetadataWithDifferentValue("")));
        assertFalse(isTopQueriesIndex("_customer_index", createIndexMetadataWithDifferentValue("_customer_index")));
    }

    private IndexMetadata createIndexMetadataWithExtraValue(String indexName) {
        long creationTime = Instant.now().toEpochMilli();
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put("index.version.created", Version.CURRENT.id)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put(SETTING_CREATION_DATE, creationTime)
            )
            .putMapping(new MappingMetadata("_doc", Map.of("_meta", Map.of("test", "someOtherTag"))))
            .build();
    }

    public void testIsTopQueriesIndexWithExtraMetaData() {
        // Generate correct hash values for test dates
        LocalDate date1 = LocalDate.parse("2024.01.01", DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT));
        LocalDate date2 = LocalDate.parse("2025.12.12", DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT));
        String hash1 = ExporterReaderUtils.generateLocalIndexDateHash(date1);
        String hash2 = ExporterReaderUtils.generateLocalIndexDateHash(date2);

        assertFalse(
            isTopQueriesIndex("top_queries-2024.01.01-" + hash1, createIndexMetadataWithExtraValue("top_queries-2024.01.01-" + hash1))
        );
        assertFalse(
            isTopQueriesIndex("top_queries-2025.12.12-" + hash2, createIndexMetadataWithExtraValue("top_queries-2025.12.12-" + hash2))
        );
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01-012345", createIndexMetadataWithExtraValue("top_queries-2024.01.01-012345")));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01-0123w", createIndexMetadataWithExtraValue("top_queries-2024.01.01-0123w")));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01", createIndexMetadataWithExtraValue("top_queries-2024.01.01")));
        assertFalse(
            isTopQueriesIndex("top_queries-2024.01.32-" + hash1, createIndexMetadataWithExtraValue("top_queries-2024.01.32-" + hash1))
        );
        assertFalse(isTopQueriesIndex("top_queries-01234", createIndexMetadataWithExtraValue("top_queries-01234")));
        assertFalse(
            isTopQueriesIndex("top_querie-2024.01.01-" + hash1, createIndexMetadataWithExtraValue("top_querie-2024.01.01-" + hash1))
        );
        assertFalse(isTopQueriesIndex("2024.01.01-" + hash1, createIndexMetadataWithExtraValue("2024.01.01-" + hash1)));
        assertFalse(isTopQueriesIndex("any_index", createIndexMetadataWithExtraValue("any_index")));
        assertFalse(isTopQueriesIndex("", createIndexMetadataWithExtraValue("")));
        assertFalse(isTopQueriesIndex("_customer_index", createIndexMetadataWithExtraValue("_customer_index")));
    }

    public void testIsTopQueriesIndexWithNullMetaData() {
        // Generate correct hash values for test dates
        LocalDate date1 = LocalDate.parse("2024.01.01", DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT));
        LocalDate date2 = LocalDate.parse("2025.12.12", DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT));
        String hash1 = ExporterReaderUtils.generateLocalIndexDateHash(date1);
        String hash2 = ExporterReaderUtils.generateLocalIndexDateHash(date2);

        assertFalse(isTopQueriesIndex("top_queries-2024.01.01-" + hash1, null));
        assertFalse(isTopQueriesIndex("top_queries-2025.12.12-" + hash2, null));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01-012345", null));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01-0123w", null));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.01", null));
        assertFalse(isTopQueriesIndex("top_queries-2024.01.32-" + hash1, null));
        assertFalse(isTopQueriesIndex("top_queries-01234", null));
        assertFalse(isTopQueriesIndex("top_querie-2024.01.01-" + hash1, null));
        assertFalse(isTopQueriesIndex("2024.01.01-" + hash1, null));
        assertFalse(isTopQueriesIndex("any_index", null));
        assertFalse(isTopQueriesIndex("", null));
        assertFalse(isTopQueriesIndex("_customer_index", null));
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
                topQueriesService.getTopQueriesRecords(false, null, null, "id-1", null),
                records1.stream().filter(record -> "id-1".equals(record.getId())).collect(Collectors.toList()),
                MetricType.LATENCY
            )
        );

        // Validate that the records for "id-2" are correctly retrieved
        assertTrue(
            QueryInsightsTestUtils.checkRecordsEqualsWithoutOrder(
                topQueriesService.getTopQueriesRecords(false, null, null, "id-2", null),
                records1.stream().filter(record -> "id-2".equals(record.getId())).collect(Collectors.toList()),
                MetricType.LATENCY
            )
        );
    }

    public void testTopQueriesVerbose() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        topQueriesService.consumeRecords(records);

        // verbose = null
        List<SearchQueryRecord> results = topQueriesService.getTopQueriesRecords(false, null, null, null, null);
        for (SearchQueryRecord record : results) {
            assertNotNull(record.getAttributes().get(Attribute.TASK_RESOURCE_USAGES));
            assertNotNull(record.getAttributes().get(Attribute.SOURCE));
            assertNotNull(record.getAttributes().get(Attribute.PHASE_LATENCY_MAP));
        }

        // verbose = true
        results = topQueriesService.getTopQueriesRecords(false, null, null, null, true);
        for (SearchQueryRecord record : results) {
            assertNotNull(record.getAttributes().get(Attribute.TASK_RESOURCE_USAGES));
            assertNotNull(record.getAttributes().get(Attribute.SOURCE));
            assertNotNull(record.getAttributes().get(Attribute.PHASE_LATENCY_MAP));
        }

        // verbose = false
        results = topQueriesService.getTopQueriesRecords(false, null, null, null, false);
        for (SearchQueryRecord record : results) {
            assertNull(record.getAttributes().get(Attribute.TASK_RESOURCE_USAGES));
            assertNull(record.getAttributes().get(Attribute.SOURCE));
            assertNull(record.getAttributes().get(Attribute.PHASE_LATENCY_MAP));
        }
    }

    public void testUpdateTopNMap() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(
            5,
            5,
            System.currentTimeMillis() - 1000 * 60 * 10,
            0
        );
        // Update window size to simulate start of new window in consumeRecords
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);

        List<SearchQueryRecord> results = topQueriesService.getTopQueriesRecords(false, null, null, null, null);
        for (SearchQueryRecord record : results) {
            @SuppressWarnings("unchecked")
            Map<String, Boolean> topNMap = (Map<String, Boolean>) record.getAttributes().get(Attribute.TOP_N_QUERY);

            assertTrue(topNMap.get(MetricType.LATENCY.toString()));
            assertFalse(topNMap.get(MetricType.CPU.toString()));
            assertFalse(topNMap.get(MetricType.MEMORY.toString()));
        }
    }

    public void testTimeFilterIncludesSomeRecords() {
        // Set a reasonable window size instead of Long.MAX_VALUE to avoid window calculation issues
        topQueriesService.setWindowSize(TimeValue.timeValueHours(1));

        // Force initialize windowStart
        topQueriesService.consumeRecords(new ArrayList<>());

        // Use current time to ensure records are in the current window
        long currentTime = System.currentTimeMillis();

        List<SearchQueryRecord> records = new ArrayList<>();
        // Records that should be included by the filter
        records.addAll(
            QueryInsightsTestUtils.generateQueryInsightRecords(
                3,
                3,
                currentTime - TimeValue.timeValueMinutes(2).getMillis(),
                0 // 2 minutes ago
            )
        );
        records.addAll(
            QueryInsightsTestUtils.generateQueryInsightRecords(
                2,
                2,
                currentTime + TimeValue.timeValueMinutes(1).getMillis(),
                0 // 1 minute in the future
            )
        );
        // Records older than the filter window
        records.addAll(
            QueryInsightsTestUtils.generateQueryInsightRecords(
                2,
                2,
                currentTime - TimeValue.timeValueMinutes(15).getMillis(),
                0 // 15 minutes ago
            )
        );

        topQueriesService.consumeRecords(records);

        String from = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime - TimeValue.timeValueMinutes(5).getMillis()), ZoneOffset.UTC)
            .toString();
        String to = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime + TimeValue.timeValueMinutes(5).getMillis()), ZoneOffset.UTC)
            .toString();

        // Include both current and history windows since records may end up in either snapshot
        // depending on timing differences between when currentTime is captured and when consumeRecords() runs
        List<SearchQueryRecord> filteredResults = topQueriesService.getTopQueriesRecords(true, from, to, null, null);
        assertEquals(5, filteredResults.size());
    }

    public void testTimeFilterIncludesNoRecords() {
        // Set a reasonable window size instead of Long.MAX_VALUE to avoid window calculation issues
        topQueriesService.setWindowSize(TimeValue.timeValueHours(1));

        // Use current time to ensure records are in the current window
        long currentTime = System.currentTimeMillis();

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(
            5,
            5,
            currentTime - TimeValue.timeValueMinutes(20).getMillis(), // 20 minutes ago
            0
        );
        topQueriesService.consumeRecords(records);

        // From 10 minutes ago
        String from = ZonedDateTime.ofInstant(
            Instant.ofEpochMilli(currentTime - TimeValue.timeValueMinutes(10).getMillis()),
            ZoneOffset.UTC
        ).toString();
        // To 5 minutes ago
        String to = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime - TimeValue.timeValueMinutes(5).getMillis()), ZoneOffset.UTC)
            .toString();

        List<SearchQueryRecord> result = topQueriesService.getTopQueriesRecords(false, from, to, null, null);
        assertEquals(0, result.size());
    }

    @SuppressWarnings("unchecked")
    public void testGetTopQueriesRecordsFromIndexSuccess() {
        QueryInsightsReader mockReader = mock(QueryInsightsReader.class);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);

        long currentTime = System.currentTimeMillis();
        String from = ZonedDateTime.ofInstant(
            Instant.ofEpochMilli(currentTime - TimeValue.timeValueMinutes(10).getMillis()),
            ZoneOffset.UTC
        ).toString();
        String to = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime + TimeValue.timeValueMinutes(10).getMillis()), ZoneOffset.UTC)
            .toString();

        // Generate 5 records that the reader would return (time filtering is done at query level)
        List<SearchQueryRecord> mockRecords = QueryInsightsTestUtils.generateQueryInsightRecords(5, 5, currentTime, 0);

        doAnswer(invocation -> {
            ActionListener<List<SearchQueryRecord>> listener = invocation.getArgument(5);
            listener.onResponse(new ArrayList<>(mockRecords));
            return null;
        }).when(mockReader).read(eq(from), eq(to), eq(null), eq(true), eq(MetricType.LATENCY), any(ActionListener.class));

        ArgumentCaptor<List<SearchQueryRecord>> listCaptor = ArgumentCaptor.forClass(List.class);
        ActionListener<List<SearchQueryRecord>> mockListener = mock(ActionListener.class);

        topQueriesService.getTopQueriesRecordsFromIndex(from, to, null, true, mockListener);

        verify(mockListener).onResponse(listCaptor.capture());
        List<SearchQueryRecord> capturedList = listCaptor.getValue();

        assertEquals(5, capturedList.size());
    }

    @SuppressWarnings("unchecked")
    public void testGetTopQueriesRecordsFromIndexSuccessWithIdFilter() {
        QueryInsightsReader mockReader = mock(QueryInsightsReader.class);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);

        long currentTime = System.currentTimeMillis();
        String from = ZonedDateTime.ofInstant(
            Instant.ofEpochMilli(currentTime - TimeValue.timeValueMinutes(10).getMillis()),
            ZoneOffset.UTC
        ).toString();
        String to = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime + TimeValue.timeValueMinutes(10).getMillis()), ZoneOffset.UTC)
            .toString();
        String targetId = "target-id-123";

        // Generate 5 records that the reader would return (time filtering is done at query level)
        List<SearchQueryRecord> mockRecords = new ArrayList<>();
        List<SearchQueryRecord> generatedForTargetId = QueryInsightsTestUtils.generateQueryInsightRecords(2, 2, currentTime, 0);
        for (SearchQueryRecord rec : generatedForTargetId) {
            SearchQueryRecord spyRec = spy(rec);
            when(spyRec.getId()).thenReturn(targetId);
            mockRecords.add(spyRec);
        }
        List<SearchQueryRecord> generatedForOtherId = QueryInsightsTestUtils.generateQueryInsightRecords(3, 3, currentTime, 0);
        for (int i = 0; i < generatedForOtherId.size(); i++) {
            SearchQueryRecord spyRec = spy(generatedForOtherId.get(i));
            when(spyRec.getId()).thenReturn("other-id-" + i);
            mockRecords.add(spyRec);
        }

        doAnswer(invocation -> {
            ActionListener<List<SearchQueryRecord>> listener = invocation.getArgument(5);
            String readerIdFilter = invocation.getArgument(2);
            List<SearchQueryRecord> recordsToReturn = mockRecords.stream()
                .filter(r -> readerIdFilter == null || readerIdFilter.equals(r.getId()))
                .toList();
            listener.onResponse(new java.util.ArrayList<>(recordsToReturn));
            return null;
        }).when(mockReader).read(eq(from), eq(to), eq(targetId), eq(null), eq(MetricType.LATENCY), any(ActionListener.class));

        ArgumentCaptor<List<SearchQueryRecord>> listCaptor = ArgumentCaptor.forClass(List.class);
        ActionListener<List<SearchQueryRecord>> mockListener = mock(ActionListener.class);

        topQueriesService.getTopQueriesRecordsFromIndex(from, to, targetId, null, mockListener);

        verify(mockListener).onResponse(listCaptor.capture());
        List<SearchQueryRecord> capturedList = listCaptor.getValue();
        assertEquals(2, capturedList.size());
        for (SearchQueryRecord record : capturedList) {
            assertEquals(targetId, record.getId());
        }
    }

    @SuppressWarnings("unchecked")
    public void testGetTopQueriesRecordsFromIndexVerboseFalse() {
        QueryInsightsReader mockReader = mock(QueryInsightsReader.class);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);

        long currentTime = System.currentTimeMillis();
        String from = ZonedDateTime.ofInstant(
            Instant.ofEpochMilli(currentTime - TimeValue.timeValueMinutes(10).getMillis()),
            ZoneOffset.UTC
        ).toString();
        String to = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime + TimeValue.timeValueMinutes(10).getMillis()), ZoneOffset.UTC)
            .toString();

        List<SearchQueryRecord> mockRecords = QueryInsightsTestUtils.generateQueryInsightRecords(2, 2, currentTime, 0);

        doAnswer(invocation -> {
            ActionListener<List<SearchQueryRecord>> listener = invocation.getArgument(5);
            // Simulate reader returning simplified records if verbose is false
            listener.onResponse(mockRecords.stream().map(SearchQueryRecord::copyAndSimplifyRecord).collect(Collectors.toList()));
            return null;
        }).when(mockReader).read(eq(from), eq(to), eq(null), eq(false), eq(MetricType.LATENCY), any(ActionListener.class));

        ArgumentCaptor<List<SearchQueryRecord>> listCaptor = ArgumentCaptor.forClass(List.class);
        ActionListener<List<SearchQueryRecord>> mockListener = mock(ActionListener.class);

        topQueriesService.getTopQueriesRecordsFromIndex(from, to, null, false, mockListener);

        verify(mockListener).onResponse(listCaptor.capture());
        List<SearchQueryRecord> capturedList = listCaptor.getValue();
        assertEquals(2, capturedList.size());
        for (SearchQueryRecord record : capturedList) {
            assertNull(record.getAttributes().get(Attribute.TASK_RESOURCE_USAGES));
            assertNull(record.getAttributes().get(Attribute.SOURCE));
            assertNull(record.getAttributes().get(Attribute.PHASE_LATENCY_MAP));
        }
    }

    @SuppressWarnings("unchecked")
    public void testGetTopQueriesRecordsFromIndex_readerReturnsEmpty() {
        QueryInsightsReader mockReader = mock(QueryInsightsReader.class);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);

        String from = ZonedDateTime.now().minusHours(1).toString();
        String to = ZonedDateTime.now().toString();

        doAnswer(invocation -> {
            ActionListener<List<SearchQueryRecord>> listener = invocation.getArgument(5);
            listener.onResponse(new java.util.ArrayList<>());
            return null;
        }).when(mockReader).read(anyString(), anyString(), any(), any(), any(MetricType.class), any(ActionListener.class));

        ArgumentCaptor<List<SearchQueryRecord>> listCaptor = ArgumentCaptor.forClass(List.class);
        ActionListener<List<SearchQueryRecord>> mockListener = mock(ActionListener.class);

        topQueriesService.getTopQueriesRecordsFromIndex(from, to, null, true, mockListener);

        verify(mockListener).onResponse(listCaptor.capture());
        assertTrue(listCaptor.getValue().isEmpty());
    }

    @SuppressWarnings("unchecked")
    public void testGetTopQueriesRecordsFromIndexReaderFails() {
        QueryInsightsReader mockReader = mock(QueryInsightsReader.class);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);

        String from = ZonedDateTime.now().minusHours(1).toString();
        String to = ZonedDateTime.now().toString();
        Exception testException = new RuntimeException("Reader failed");

        doAnswer(invocation -> {
            ActionListener<List<SearchQueryRecord>> listener = invocation.getArgument(5);
            listener.onFailure(testException);
            return null;
        }).when(mockReader).read(anyString(), anyString(), any(), any(), any(MetricType.class), any(ActionListener.class));

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        ActionListener<List<SearchQueryRecord>> mockListener = mock(ActionListener.class);

        topQueriesService.getTopQueriesRecordsFromIndex(from, to, null, true, mockListener);

        verify(mockListener).onFailure(exceptionCaptor.capture());
        assertEquals(testException, exceptionCaptor.getValue());
    }

    @SuppressWarnings("unchecked")
    public void testGetTopQueriesRecordsFromIndexNoReaderAvailable() {
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(null);

        String from = ZonedDateTime.now().minusHours(1).toString();
        String to = ZonedDateTime.now().toString();

        ArgumentCaptor<List<SearchQueryRecord>> listCaptor = ArgumentCaptor.forClass(List.class);
        ActionListener<List<SearchQueryRecord>> mockListener = mock(ActionListener.class);

        topQueriesService.getTopQueriesRecordsFromIndex(from, to, null, true, mockListener);

        verify(mockListener).onResponse(listCaptor.capture());
        assertTrue(listCaptor.getValue().isEmpty());
    }
}
