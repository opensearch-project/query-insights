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
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterFactory;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReaderFactory;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

/**
 * Unit Tests for {@link QueryInsightsService}.
 */
public class TopQueriesServiceTests extends OpenSearchTestCase {
    private TopQueriesService topQueriesService;
    private final ThreadPool threadPool = mock(ThreadPool.class);
    private final QueryInsightsExporterFactory queryInsightsExporterFactory = mock(QueryInsightsExporterFactory.class);
    private final QueryInsightsReaderFactory queryInsightsReaderFactory = mock(QueryInsightsReaderFactory.class);

    @Before
    public void setup() {
        topQueriesService = new TopQueriesService(MetricType.LATENCY, threadPool, queryInsightsExporterFactory, queryInsightsReaderFactory);
        topQueriesService.setTopNSize(Integer.MAX_VALUE);
        topQueriesService.setWindowSize(new TimeValue(Long.MAX_VALUE));
        topQueriesService.setEnabled(true);
    }

    public void testSetAndGetTopNSize() {
        int newSize = 5;
        topQueriesService.setTopNSize(newSize);
        assertEquals(newSize, topQueriesService.getTopNSize());
    }

    public void testConsumeRecords() {
        // Prepare some mock SearchQueryRecords
        SearchQueryRecord record1 = mock(SearchQueryRecord.class);
        when(record1.getTimestamp()).thenReturn(System.currentTimeMillis());
        when(record1.getMeasurements()).thenReturn(Collections.singletonMap(MetricType.LATENCY, new Measurement(1000L)));
        when(record1.getMeasurement(any())).thenReturn(1000L);
        SearchQueryRecord record2 = mock(SearchQueryRecord.class);
        when(record2.getTimestamp()).thenReturn(System.currentTimeMillis());
        when(record2.getMeasurements()).thenReturn(Collections.singletonMap(MetricType.LATENCY, new Measurement(2000L)));
        when(record2.getMeasurement(any())).thenReturn(2000L);
        // Consume records
        topQueriesService.consumeRecords(List.of(record1, record2));
        // Verify that the topQueriesStore contains the records
        List<SearchQueryRecord> snapshot = topQueriesService.getTopQueriesCurrentSnapshot();
        assertEquals(2, snapshot.size());
    }

    public void testGetTopQueriesRecords() {
        topQueriesService.setEnabled(true);
        // Prepare some mock SearchQueryRecords and add to service
        SearchQueryRecord record1 = mock(SearchQueryRecord.class);
        when(record1.getTimestamp()).thenReturn(System.currentTimeMillis());
        when(record1.getMeasurements()).thenReturn(Collections.singletonMap(MetricType.LATENCY, new Measurement(1000L)));
        topQueriesService.consumeRecords(List.of(record1));
        // Fetch records
        List<SearchQueryRecord> result = topQueriesService.getTopQueriesRecords(false, null, null);
        // Verify the result
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    public void testIngestQueryDataWithLargeWindow() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        topQueriesService.consumeRecords(records);
        assertTrue(
            QueryInsightsTestUtils.checkRecordsEqualsWithoutOrder(
                topQueriesService.getTopQueriesRecords(false, null, null),
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
        assertEquals(0, topQueriesService.getTopQueriesRecords(true, null, null).size());

        // Create 10 records at now + 1 minute, to make sure they belong to the current window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(10, 10, System.currentTimeMillis() + 1000 * 60, 0);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(10, topQueriesService.getTopQueriesRecords(true, null, null).size());
    }

    public void testSmallNSize() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        topQueriesService.setTopNSize(1);
        topQueriesService.consumeRecords(records);
        assertEquals(1, topQueriesService.getTopQueriesRecords(false, null, null).size());
    }

    public void testValidateTopNSize() {
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.validateTopNSize(QueryInsightsSettings.MAX_N_SIZE + 1); });
    }

    public void testValidateNegativeTopNSize() {
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.validateTopNSize(-1); });
    }

    public void testGetTopQueriesWhenNotEnabled() {
        topQueriesService.setEnabled(false);
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.getTopQueriesRecords(false, null, null); });
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
        assertEquals(0, topQueriesService.getTopQueriesRecords(true, null, null).size());

        // Create 10 records at now + 1 minute, to make sure they belong to the current window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(10, 10, System.currentTimeMillis() + 1000 * 60, 0);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(10, topQueriesService.getTopQueriesRecords(true, null, null).size());
    }

    public void testRollingWindowsWithDifferentGroup() {
        topQueriesService.setGrouping(GroupingType.SIMILARITY);
        List<SearchQueryRecord> records;
        // Create 5 records at Now - 10 minutes to make sure they belong to the last window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(5, 5, System.currentTimeMillis() - 1000 * 60 * 10, 0);
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);

        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(0, topQueriesService.getTopQueriesRecords(true, null, null).size());

        // Create 10 records at now + 1 minute, to make sure they belong to the current window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(10, 10, System.currentTimeMillis() + 1000 * 60, 0);
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(1, topQueriesService.getTopQueriesRecords(true, null, null).size());
    }
}
