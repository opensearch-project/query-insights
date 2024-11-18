/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.grouper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import org.junit.Before;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.healthStats.QueryGrouperHealthStats;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit Tests for {@link MinMaxHeapQueryGrouper}.
 */
public class MinMaxHeapQueryGrouperTests extends OpenSearchTestCase {
    private MinMaxHeapQueryGrouper minMaxHeapQueryGrouper;
    private PriorityBlockingQueue<SearchQueryRecord> topQueriesStore = new PriorityBlockingQueue<>(
        100,
        (a, b) -> SearchQueryRecord.compare(a, b, MetricType.LATENCY)
    );

    @Before
    public void setup() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.DEFAULT_AGGREGATION_TYPE, 10);
    }

    public void testWithAllDifferentHashcodes() {
        int numOfRecords = 10;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);
        SearchQueryRecord groupedRecord;
        Set<Integer> hashcodeSet = new HashSet<>();
        for (SearchQueryRecord record : records) {
            groupedRecord = minMaxHeapQueryGrouper.add(record);
            int hashcode = (int) groupedRecord.getAttributes().get(Attribute.QUERY_HASHCODE);
            hashcodeSet.add(hashcode);
        }
        assertEquals(numOfRecords, hashcodeSet.size());
    }

    public void testWithAllSameHashcodes() {
        int numOfRecords = 10;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord groupedRecord;
        Set<Integer> hashcodeSet = new HashSet<>();
        for (SearchQueryRecord record : records) {
            groupedRecord = minMaxHeapQueryGrouper.add(record);
            int hashcode = (int) groupedRecord.getAttributes().get(Attribute.QUERY_HASHCODE);
            hashcodeSet.add(hashcode);
        }
        assertEquals(1, hashcodeSet.size());
    }

    public void testDrain() {
        int numOfRecords = 10;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);
        for (SearchQueryRecord record : records) {
            minMaxHeapQueryGrouper.add(record);
        }
        int groupsBeforeDrain = minMaxHeapQueryGrouper.numberOfGroups();
        minMaxHeapQueryGrouper.drain();
        int groupsAfterDrain = minMaxHeapQueryGrouper.numberOfGroups();

        assertEquals(numOfRecords, groupsBeforeDrain);
        assertEquals(0, groupsAfterDrain);
    }

    public void testChangeTopNSize() {
        int numOfRecords = 15;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            minMaxHeapQueryGrouper.add(record);
        }

        assertEquals(10, minMaxHeapQueryGrouper.numberOfTopGroups()); // Initially expects top 10 groups

        minMaxHeapQueryGrouper.updateTopNSize(5);
        minMaxHeapQueryGrouper.drain(); // Clear previous state

        for (SearchQueryRecord record : records) {
            minMaxHeapQueryGrouper.add(record);
        }

        assertEquals(5, minMaxHeapQueryGrouper.numberOfTopGroups()); // After update, expects top 5 groups
    }

    public void testEmptyPriorityQueues() {
        int groupsBeforeDrain = minMaxHeapQueryGrouper.numberOfGroups();
        assertEquals(0, groupsBeforeDrain);

        minMaxHeapQueryGrouper.drain();
        int groupsAfterDrain = minMaxHeapQueryGrouper.numberOfGroups();
        assertEquals(0, groupsAfterDrain); // No groups should be present after draining
    }

    public void testAddRemoveFromMaxHeap() {
        int numOfRecords = 15;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            minMaxHeapQueryGrouper.add(record);
        }

        assertTrue(minMaxHeapQueryGrouper.numberOfTopGroups() <= 10); // Should be at most 10 in the min heap

        minMaxHeapQueryGrouper.updateTopNSize(5); // Change size to 5
        minMaxHeapQueryGrouper.drain(); // Clear previous state

        for (SearchQueryRecord record : records) {
            minMaxHeapQueryGrouper.add(record);
        }

        assertEquals(5, minMaxHeapQueryGrouper.numberOfTopGroups()); // Should be exactly 5 in the min heap
    }

    public void testInvalidGroupingType() {
        MinMaxHeapQueryGrouper invalidGroupingService = new MinMaxHeapQueryGrouper(
            MetricType.LATENCY,
            GroupingType.NONE,
            AggregationType.DEFAULT_AGGREGATION_TYPE,
            topQueriesStore,
            10
        );
        SearchQueryRecord record = QueryInsightsTestUtils.generateQueryInsightRecords(1).get(0);
        expectThrows(IllegalArgumentException.class, () -> invalidGroupingService.add(record));
    }

    public void testLargeNumberOfRecords() {
        int numOfRecords = 1000;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            minMaxHeapQueryGrouper.add(record);
        }

        assertTrue(minMaxHeapQueryGrouper.numberOfTopGroups() <= 10); // Should be at most 10 in the min heap
    }

    public void testChangeGroupingType() {
        int numOfRecords = 10;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            minMaxHeapQueryGrouper.add(record);
        }

        int groupsBeforeChange = minMaxHeapQueryGrouper.numberOfGroups();
        assertTrue(groupsBeforeChange > 0);

        minMaxHeapQueryGrouper.setGroupingType(GroupingType.NONE); // Changing to NONE should clear groups

        int groupsAfterChange = minMaxHeapQueryGrouper.numberOfGroups();
        assertEquals(0, groupsAfterChange); // Expect no groups after changing to NONE
    }

    public void testDrainWithMultipleGroupingTypes() {
        int numOfRecords = 20;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            minMaxHeapQueryGrouper.add(record);
        }

        int groupsBeforeDrain = minMaxHeapQueryGrouper.numberOfGroups();
        assertTrue(groupsBeforeDrain > 0);

        minMaxHeapQueryGrouper.setGroupingType(GroupingType.SIMILARITY);
        minMaxHeapQueryGrouper.drain();

        int groupsAfterDrain = minMaxHeapQueryGrouper.numberOfGroups();
        assertEquals(0, groupsAfterDrain); // After drain, groups should be cleared
    }

    public void testVaryingTopNSize() {
        int numOfRecords = 30;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            minMaxHeapQueryGrouper.add(record);
        }

        minMaxHeapQueryGrouper.updateTopNSize(15);
        minMaxHeapQueryGrouper.drain(); // Clear previous state

        for (SearchQueryRecord record : records) {
            minMaxHeapQueryGrouper.add(record);
        }

        assertEquals(15, minMaxHeapQueryGrouper.numberOfTopGroups()); // Should reflect the updated top N size
    }

    public void testAddMeasurementSumAggregationLatency() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.SUM, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        for (SearchQueryRecord record : records) {
            aggregatedRecord = minMaxHeapQueryGrouper.add(record);
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.LATENCY).longValue();
        }

        assertEquals(expectedSum, aggregatedRecord.getMeasurement(MetricType.LATENCY));
    }

    public void testAddMeasurementAverageAggregationLatency() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        int expectedCount = 0;
        for (SearchQueryRecord record : records) {
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.LATENCY).longValue();
            aggregatedRecord = minMaxHeapQueryGrouper.add(record);
            expectedCount += 1;
        }

        long expectedAverage = (long) expectedSum / expectedCount;
        assertEquals(expectedAverage, aggregatedRecord.getMeasurement(MetricType.LATENCY));
    }

    public void testAddMeasurementNoneAggregationLatency() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.NONE, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord lastRecord = null;

        Number expectedValue = 0;
        for (SearchQueryRecord record : records) {
            expectedValue = record.getMeasurement(MetricType.LATENCY).longValue();
            lastRecord = minMaxHeapQueryGrouper.add(record);
        }

        assertEquals(expectedValue, lastRecord.getMeasurement(MetricType.LATENCY));
    }

    public void testAddMeasurementSumAggregationCpu() {
        minMaxHeapQueryGrouper = new MinMaxHeapQueryGrouper(
            MetricType.CPU,
            GroupingType.SIMILARITY,
            AggregationType.SUM,
            topQueriesStore,
            10
        );
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        for (SearchQueryRecord record : records) {
            aggregatedRecord = minMaxHeapQueryGrouper.add(record);
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.CPU).longValue();
        }

        assertEquals(expectedSum, aggregatedRecord.getMeasurement(MetricType.CPU));
    }

    public void testAddMeasurementAverageAggregationCpu() {
        minMaxHeapQueryGrouper = new MinMaxHeapQueryGrouper(
            MetricType.CPU,
            GroupingType.SIMILARITY,
            AggregationType.AVERAGE,
            topQueriesStore,
            10
        );
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        int expectedCount = 0;
        for (SearchQueryRecord record : records) {
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.CPU).longValue();
            aggregatedRecord = minMaxHeapQueryGrouper.add(record);
            expectedCount += 1;
        }

        long expectedAverage = (long) expectedSum / expectedCount;
        assertEquals(expectedAverage, aggregatedRecord.getMeasurement(MetricType.CPU));
    }

    public void testAddMeasurementNoneAggregationCpu() {
        minMaxHeapQueryGrouper = new MinMaxHeapQueryGrouper(
            MetricType.CPU,
            GroupingType.SIMILARITY,
            AggregationType.NONE,
            topQueriesStore,
            10
        );
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord lastRecord = null;

        Number expectedValue = 0;
        for (SearchQueryRecord record : records) {
            expectedValue = record.getMeasurement(MetricType.CPU).longValue();
            lastRecord = minMaxHeapQueryGrouper.add(record);
        }

        assertEquals(expectedValue, lastRecord.getMeasurement(MetricType.CPU));
    }

    public void testNoneGroupingTypeIllegalArgumentException() {
        minMaxHeapQueryGrouper = new MinMaxHeapQueryGrouper(MetricType.CPU, GroupingType.NONE, AggregationType.NONE, topQueriesStore, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        for (SearchQueryRecord record : records) {
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.CPU).longValue();
            assertThrows(IllegalArgumentException.class, () -> { minMaxHeapQueryGrouper.add(record); });
        }
    }

    // New query group not existing added to MIN
    public void testNewGroupAddedToMin() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            2,
            MetricType.LATENCY,
            List.of(1000L, 1100L)
        );

        for (List<SearchQueryRecord> recordList : allRecords) {
            for (SearchQueryRecord record : recordList) {
                minMaxHeapQueryGrouper.add(record);
            }
        }

        assertEquals(2, minMaxHeapQueryGrouper.numberOfTopGroups());
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // New query group not existing added to MIN and overflows to MAX
    public void testNewGroupOverflowsMinToMax() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            2,
            MetricType.LATENCY,
            List.of(1000L, 1100L, 900L)
        );

        for (List<SearchQueryRecord> recordList : allRecords) {
            for (SearchQueryRecord record : recordList) {
                minMaxHeapQueryGrouper.add(record);
            }
        }

        assertEquals(2, minMaxHeapQueryGrouper.numberOfTopGroups());
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // New query group not existing added to MIN and causes other group to overflow to MAX
    public void testNewGroupCausesOtherGroupOverflowMinToMax() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            2,
            MetricType.LATENCY,
            List.of(1000L, 1100L, 1200L)
        );

        for (List<SearchQueryRecord> recordList : allRecords) {
            for (SearchQueryRecord record : recordList) {
                minMaxHeapQueryGrouper.add(record);
            }
        }

        assertEquals(2, minMaxHeapQueryGrouper.numberOfTopGroups());
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1200L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // Existing query group update to MIN increases average
    public void testExistingGroupUpdateToMinIncreaseAverage() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(1100L, 1200L, 1000L)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(1300L)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                minMaxHeapQueryGrouper.add(record);
            }
        }

        assertEquals(2, minMaxHeapQueryGrouper.numberOfTopGroups());
        assertEquals(1200L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1200L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // Existing query group update to MIN decrease average - stay in MIN
    public void testExistingGroupUpdateToMinDecreaseAverageStayInMin() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(1100L, 600L, 1000L)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(700L)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                minMaxHeapQueryGrouper.add(record);
            }
        }

        assertEquals(2, minMaxHeapQueryGrouper.numberOfTopGroups());
        assertEquals(900L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // Existing query group update to MIN decrease average - overflows to MAX
    public void testExistingGroupUpdateToMinDecreaseAverageOverflowsToMax() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(1199L, 1100L, 1000L)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(1L)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                minMaxHeapQueryGrouper.add(record);
            }
        }

        assertEquals(2, minMaxHeapQueryGrouper.numberOfTopGroups());
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // Existing query group update to MAX increases average - stay in MAX
    public void testExistingGroupUpdateToMaxIncreaseAverageStayInMax() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(900L, 975L, 950L)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(920L)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                minMaxHeapQueryGrouper.add(record);
            }
        }

        assertEquals(2, minMaxHeapQueryGrouper.numberOfTopGroups());
        assertEquals(950L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(975L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // Existing query group update to MAX increases average - promote to MIN
    public void testExistingGroupUpdateToMaxIncreaseAveragePromoteToMin() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(900L, 975L, 950L)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(1100L)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                minMaxHeapQueryGrouper.add(record);
            }
        }

        assertEquals(2, minMaxHeapQueryGrouper.numberOfTopGroups());
        assertEquals(975L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // Existing query group update to MAX decrease average
    public void testExistingGroupUpdateToMaxDecreaseAverage() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(900L, 975L, 950L)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(800L)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                minMaxHeapQueryGrouper.add(record);
            }
        }

        assertEquals(2, minMaxHeapQueryGrouper.numberOfTopGroups());
        assertEquals(950L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(975L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    public void testSwitchGroupingTypeToNone() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(900L, 975L, 950L)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(800L)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                minMaxHeapQueryGrouper.add(record);
            }
        }

        assertEquals(2, minMaxHeapQueryGrouper.numberOfTopGroups());
        assertEquals(950L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(975L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));

        minMaxHeapQueryGrouper.setGroupingType(GroupingType.NONE);
        assertEquals(0, minMaxHeapQueryGrouper.numberOfTopGroups());

        assertThrows(IllegalArgumentException.class, () -> { minMaxHeapQueryGrouper.add(allRecords1.get(0).get(0)); });
    }

    public void testMultipleQueryGroupsUpdates() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(900L, 1000L, 1000L)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(800L, 400L, 1200L)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                minMaxHeapQueryGrouper.add(record);
            }
        }

        assertEquals(2, minMaxHeapQueryGrouper.numberOfTopGroups());
        assertEquals(850L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(3, minMaxHeapQueryGrouper.numberOfGroups());
    }

    public void testMaxGroupLimitReached() {
        minMaxHeapQueryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 1);

        minMaxHeapQueryGrouper.setMaxGroups(1);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(900L, 1000L, 1000L)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(800L, 400L, 1200L)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                minMaxHeapQueryGrouper.add(record);
            }
        }

        assertEquals(1, minMaxHeapQueryGrouper.numberOfTopGroups());
        assertEquals(850L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(2, minMaxHeapQueryGrouper.numberOfGroups());
    }

    public void testGetHealthStatsWithEmptyGrouper() {
        QueryGrouperHealthStats healthStats = minMaxHeapQueryGrouper.getHealthStats();
        // Expecting 0 group count and 0 heap size since no groups have been added
        assertEquals(0, healthStats.getQueryGroupCount());
        assertEquals(0, healthStats.getQueryGroupHeapSize());
    }

    public void testGetHealthStatsWithGroups() {
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        minMaxHeapQueryGrouper.add(records.get(0));
        minMaxHeapQueryGrouper.add(records.get(1));
        QueryGrouperHealthStats healthStats = minMaxHeapQueryGrouper.getHealthStats();
        // Verify that group count stats reflect the correct number of total groups
        assertEquals(2, healthStats.getQueryGroupCount());
        assertEquals(0, healthStats.getQueryGroupHeapSize());
    }

    private MinMaxHeapQueryGrouper getQueryGroupingService(AggregationType aggregationType, int topNSize) {
        return new MinMaxHeapQueryGrouper(MetricType.LATENCY, GroupingType.SIMILARITY, aggregationType, topQueriesStore, topNSize);
    }

    public void testAttributeTypeSetToGroup() {
        int numOfRecords = 10;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            assertEquals(GroupingType.NONE, record.getAttributes().get(Attribute.GROUP_BY));
        }
        SearchQueryRecord groupedRecord;
        for (SearchQueryRecord record : records) {
            groupedRecord = minMaxHeapQueryGrouper.add(record);
            assertEquals(GroupingType.SIMILARITY, groupedRecord.getAttributes().get(Attribute.GROUP_BY));
        }
    }
}
