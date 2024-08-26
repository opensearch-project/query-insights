/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import org.junit.Before;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit Tests for {@link QueryGrouper}.
 */
public class QueryGrouperTests extends OpenSearchTestCase {
    private QueryGrouper queryGrouper;
    private PriorityQueue<SearchQueryRecord> topQueriesStore = new PriorityQueue<>(
        100,
        (a, b) -> SearchQueryRecord.compare(a, b, MetricType.LATENCY)
    );

    @Before
    public void setup() {
        queryGrouper = getQueryGroupingService(AggregationType.DEFAULT_AGGREGATION_TYPE, 10);
    }

    public void testWithAllDifferentHashcodes() {
        int numOfRecords = 10;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);
        SearchQueryRecord groupedRecord;
        Set<Integer> hashcodeSet = new HashSet<>();
        for (SearchQueryRecord record : records) {
            groupedRecord = queryGrouper.addQueryToGroup(record);
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
            groupedRecord = queryGrouper.addQueryToGroup(record);
            int hashcode = (int) groupedRecord.getAttributes().get(Attribute.QUERY_HASHCODE);
            hashcodeSet.add(hashcode);
        }
        assertEquals(1, hashcodeSet.size());
    }

    public void testDrain() {
        int numOfRecords = 10;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);
        for (SearchQueryRecord record : records) {
            queryGrouper.addQueryToGroup(record);
        }
        int groupsBeforeDrain = queryGrouper.numberOfGroups();
        queryGrouper.drain();
        int groupsAfterDrain = queryGrouper.numberOfGroups();

        assertEquals(numOfRecords, groupsBeforeDrain);
        assertEquals(0, groupsAfterDrain);
    }

    public void testChangeTopNSize() {
        int numOfRecords = 15;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            queryGrouper.addQueryToGroup(record);
        }

        assertEquals(10, queryGrouper.numberOfTopGroups()); // Initially expects top 10 groups

        queryGrouper.updateTopNSize(5);
        queryGrouper.drain(); // Clear previous state

        for (SearchQueryRecord record : records) {
            queryGrouper.addQueryToGroup(record);
        }

        assertEquals(5, queryGrouper.numberOfTopGroups()); // After update, expects top 5 groups
    }

    public void testEmptyPriorityQueues() {
        int groupsBeforeDrain = queryGrouper.numberOfGroups();
        assertEquals(0, groupsBeforeDrain);

        queryGrouper.drain();
        int groupsAfterDrain = queryGrouper.numberOfGroups();
        assertEquals(0, groupsAfterDrain); // No groups should be present after draining
    }

    public void testAddRemoveFromMaxHeap() {
        int numOfRecords = 15;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            queryGrouper.addQueryToGroup(record);
        }

        assertTrue(queryGrouper.numberOfTopGroups() <= 10); // Should be at most 10 in the min heap

        queryGrouper.updateTopNSize(5); // Change size to 5
        queryGrouper.drain(); // Clear previous state

        for (SearchQueryRecord record : records) {
            queryGrouper.addQueryToGroup(record);
        }

        assertEquals(5, queryGrouper.numberOfTopGroups()); // Should be exactly 5 in the min heap
    }

    public void testInvalidGroupingType() {
        QueryGrouper invalidGroupingService = new QueryGrouper(
            MetricType.LATENCY,
            GroupingType.NONE,
            AggregationType.DEFAULT_AGGREGATION_TYPE,
            topQueriesStore,
            10
        );
        SearchQueryRecord record = QueryInsightsTestUtils.generateQueryInsightRecords(1).get(0);
        expectThrows(IllegalArgumentException.class, () -> invalidGroupingService.addQueryToGroup(record));
    }

    public void testLargeNumberOfRecords() {
        int numOfRecords = 1000;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            queryGrouper.addQueryToGroup(record);
        }

        assertTrue(queryGrouper.numberOfTopGroups() <= 10); // Should be at most 10 in the min heap
    }

    public void testChangeGroupingType() {
        int numOfRecords = 10;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            queryGrouper.addQueryToGroup(record);
        }

        int groupsBeforeChange = queryGrouper.numberOfGroups();
        assertTrue(groupsBeforeChange > 0);

        queryGrouper.setGroupingType(GroupingType.NONE); // Changing to NONE should clear groups

        int groupsAfterChange = queryGrouper.numberOfGroups();
        assertEquals(0, groupsAfterChange); // Expect no groups after changing to NONE
    }

    public void testDrainWithMultipleGroupingTypes() {
        int numOfRecords = 20;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            queryGrouper.addQueryToGroup(record);
        }

        int groupsBeforeDrain = queryGrouper.numberOfGroups();
        assertTrue(groupsBeforeDrain > 0);

        queryGrouper.setGroupingType(GroupingType.SIMILARITY);
        queryGrouper.drain();

        int groupsAfterDrain = queryGrouper.numberOfGroups();
        assertEquals(0, groupsAfterDrain); // After drain, groups should be cleared
    }

    public void testVaryingTopNSize() {
        int numOfRecords = 30;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            queryGrouper.addQueryToGroup(record);
        }

        queryGrouper.updateTopNSize(15);
        queryGrouper.drain(); // Clear previous state

        for (SearchQueryRecord record : records) {
            queryGrouper.addQueryToGroup(record);
        }

        assertEquals(15, queryGrouper.numberOfTopGroups()); // Should reflect the updated top N size
    }

    public void testAddMeasurementSumAggregationLatency() {
        queryGrouper = getQueryGroupingService(AggregationType.SUM, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        for (SearchQueryRecord record : records) {
            aggregatedRecord = queryGrouper.addQueryToGroup(record);
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.LATENCY).longValue();
        }

        assertEquals(expectedSum, aggregatedRecord.getMeasurement(MetricType.LATENCY));
    }

    public void testAddMeasurementAverageAggregationLatency() {
        queryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        int expectedCount = 0;
        for (SearchQueryRecord record : records) {
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.LATENCY).longValue();
            aggregatedRecord = queryGrouper.addQueryToGroup(record);
            expectedCount += 1;
        }

        long expectedAverage = (long) expectedSum / expectedCount;
        assertEquals(expectedAverage, aggregatedRecord.getMeasurement(MetricType.LATENCY));
    }

    public void testAddMeasurementNoneAggregationLatency() {
        queryGrouper = getQueryGroupingService(AggregationType.NONE, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord lastRecord = null;

        Number expectedValue = 0;
        for (SearchQueryRecord record : records) {
            expectedValue = record.getMeasurement(MetricType.LATENCY).longValue();
            lastRecord = queryGrouper.addQueryToGroup(record);
        }

        assertEquals(expectedValue, lastRecord.getMeasurement(MetricType.LATENCY));
    }

    public void testAddMeasurementSumAggregationCpu() {
        queryGrouper = new QueryGrouper(MetricType.CPU, GroupingType.SIMILARITY, AggregationType.SUM, topQueriesStore, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        for (SearchQueryRecord record : records) {
            aggregatedRecord = queryGrouper.addQueryToGroup(record);
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.CPU).longValue();
        }

        assertEquals(expectedSum, aggregatedRecord.getMeasurement(MetricType.CPU));
    }

    public void testAddMeasurementAverageAggregationCpu() {
        queryGrouper = new QueryGrouper(MetricType.CPU, GroupingType.SIMILARITY, AggregationType.AVERAGE, topQueriesStore, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        int expectedCount = 0;
        for (SearchQueryRecord record : records) {
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.CPU).longValue();
            aggregatedRecord = queryGrouper.addQueryToGroup(record);
            expectedCount += 1;
        }

        long expectedAverage = (long) expectedSum / expectedCount;
        assertEquals(expectedAverage, aggregatedRecord.getMeasurement(MetricType.CPU));
    }

    public void testAddMeasurementNoneAggregationCpu() {
        queryGrouper = new QueryGrouper(MetricType.CPU, GroupingType.SIMILARITY, AggregationType.NONE, topQueriesStore, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord lastRecord = null;

        Number expectedValue = 0;
        for (SearchQueryRecord record : records) {
            expectedValue = record.getMeasurement(MetricType.CPU).longValue();
            lastRecord = queryGrouper.addQueryToGroup(record);
        }

        assertEquals(expectedValue, lastRecord.getMeasurement(MetricType.CPU));
    }

    public void testNoneGroupingTypeIllegalArgumentException() {
        queryGrouper = new QueryGrouper(MetricType.CPU, GroupingType.NONE, AggregationType.NONE, topQueriesStore, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        for (SearchQueryRecord record : records) {
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.CPU).longValue();
            assertThrows(IllegalArgumentException.class, () -> { queryGrouper.addQueryToGroup(record); });
        }
    }

    // 1. New query group not existing added to MIN
    public void testNewGroupAddedToMin() {
        queryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            2,
            MetricType.LATENCY,
            List.of(1000L, 1100L)
        );

        for (List<SearchQueryRecord> recordList : allRecords) {
            for (SearchQueryRecord record : recordList) {
                queryGrouper.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGrouper.numberOfTopGroups());
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 2. New query group not existing added to MIN and overflows to MAX
    public void testNewGroupOverflowsMinToMax() {
        queryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            2,
            MetricType.LATENCY,
            List.of(1000L, 1100L, 900L)
        );

        for (List<SearchQueryRecord> recordList : allRecords) {
            for (SearchQueryRecord record : recordList) {
                queryGrouper.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGrouper.numberOfTopGroups());
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 3. New query group not existing added to MIN and causes other group to overflow to MAX
    public void testNewGroupCausesOtherGroupOverflowMinToMax() {
        queryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            2,
            MetricType.LATENCY,
            List.of(1000L, 1100L, 1200L)
        );

        for (List<SearchQueryRecord> recordList : allRecords) {
            for (SearchQueryRecord record : recordList) {
                queryGrouper.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGrouper.numberOfTopGroups());
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1200L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 4. Existing query group update to MIN increases average
    public void testExistingGroupUpdateToMinIncreaseAverage() {
        queryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

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
                queryGrouper.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGrouper.numberOfTopGroups());
        assertEquals(1200L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1200L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 5. Existing query group update to MIN decrease average - stay in MIN
    public void testExistingGroupUpdateToMinDecreaseAverageStayInMin() {
        queryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

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
                queryGrouper.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGrouper.numberOfTopGroups());
        assertEquals(900L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 6. Existing query group update to MIN decrease average - overflows to MAX
    public void testExistingGroupUpdateToMinDecreaseAverageOverflowsToMax() {
        queryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

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
                queryGrouper.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGrouper.numberOfTopGroups());
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 7. Existing query group update to MAX increases average - stay in MAX
    public void testExistingGroupUpdateToMaxIncreaseAverageStayInMax() {
        queryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

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
                queryGrouper.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGrouper.numberOfTopGroups());
        assertEquals(950L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(975L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 8. Existing query group update to MAX increases average - promote to MIN
    public void testExistingGroupUpdateToMaxIncreaseAveragePromoteToMin() {
        queryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

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
                queryGrouper.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGrouper.numberOfTopGroups());
        assertEquals(975L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 9. Existing query group update to MAX decrease average
    public void testExistingGroupUpdateToMaxDecreaseAverage() {
        queryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

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
                queryGrouper.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGrouper.numberOfTopGroups());
        assertEquals(950L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(975L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    public void testSwitchGroupingTypeToNone() {
        queryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

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
                queryGrouper.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGrouper.numberOfTopGroups());
        assertEquals(950L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(975L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));

        queryGrouper.setGroupingType(GroupingType.NONE);
        assertEquals(0, queryGrouper.numberOfTopGroups());

        assertThrows(IllegalArgumentException.class, () -> { queryGrouper.addQueryToGroup(allRecords1.get(0).get(0)); });
    }

    public void testMultipleQueryGroupsUpdates() {
        queryGrouper = getQueryGroupingService(AggregationType.AVERAGE, 2);

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
                queryGrouper.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGrouper.numberOfTopGroups());
        assertEquals(850L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    private QueryGrouper getQueryGroupingService(AggregationType aggregationType, int topNSize) {
        return new QueryGrouper(MetricType.LATENCY, GroupingType.SIMILARITY, aggregationType, topQueriesStore, topNSize);
    }
}
