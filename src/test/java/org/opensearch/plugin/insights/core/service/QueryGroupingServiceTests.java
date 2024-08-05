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
 * Unit Tests for {@link QueryGroupingService}.
 */
public class QueryGroupingServiceTests extends OpenSearchTestCase {
    private QueryGroupingService queryGroupingService;
    private PriorityQueue<SearchQueryRecord> topQueriesStore = new PriorityQueue<>(
        100,
        (a, b) -> SearchQueryRecord.compare(a, b, MetricType.LATENCY)
    );

    @Before
    public void setup() {
        queryGroupingService = getQueryGroupingService(AggregationType.DEFUALT_AGGREGATION_TYPE, 10);
    }

    public void testWithAllDifferentHashcodes() {
        int numOfRecords = 10;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);
        SearchQueryRecord groupedRecord;
        Set<Integer> hashcodeSet = new HashSet<>();
        for (SearchQueryRecord record : records) {
            groupedRecord = queryGroupingService.addQueryToGroup(record);
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
            groupedRecord = queryGroupingService.addQueryToGroup(record);
            int hashcode = (int) groupedRecord.getAttributes().get(Attribute.QUERY_HASHCODE);
            hashcodeSet.add(hashcode);
        }
        assertEquals(1, hashcodeSet.size());
    }

    public void testDrain() {
        int numOfRecords = 10;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);
        for (SearchQueryRecord record : records) {
            queryGroupingService.addQueryToGroup(record);
        }
        int groupsBeforeDrain = queryGroupingService.numberOfGroups();
        queryGroupingService.drain();
        int groupsAfterDrain = queryGroupingService.numberOfGroups();

        assertEquals(numOfRecords, groupsBeforeDrain);
        assertEquals(0, groupsAfterDrain);
    }

    public void testChangeTopNSize() {
        int numOfRecords = 15;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            queryGroupingService.addQueryToGroup(record);
        }

        assertEquals(10, queryGroupingService.numberOfTopGroups()); // Initially expects top 10 groups

        queryGroupingService.updateTopNSize(5);
        queryGroupingService.drain(); // Clear previous state

        for (SearchQueryRecord record : records) {
            queryGroupingService.addQueryToGroup(record);
        }

        assertEquals(5, queryGroupingService.numberOfTopGroups()); // After update, expects top 5 groups
    }

    public void testEmptyPriorityQueues() {
        int groupsBeforeDrain = queryGroupingService.numberOfGroups();
        assertEquals(0, groupsBeforeDrain);

        queryGroupingService.drain();
        int groupsAfterDrain = queryGroupingService.numberOfGroups();
        assertEquals(0, groupsAfterDrain); // No groups should be present after draining
    }

    public void testAddRemoveFromMaxHeap() {
        int numOfRecords = 15;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            queryGroupingService.addQueryToGroup(record);
        }

        assertTrue(queryGroupingService.numberOfTopGroups() <= 10); // Should be at most 10 in the min heap

        queryGroupingService.updateTopNSize(5); // Change size to 5
        queryGroupingService.drain(); // Clear previous state

        for (SearchQueryRecord record : records) {
            queryGroupingService.addQueryToGroup(record);
        }

        assertEquals(5, queryGroupingService.numberOfTopGroups()); // Should be exactly 5 in the min heap
    }

    public void testInvalidGroupingType() {
        QueryGroupingService invalidGroupingService = new QueryGroupingService(
            MetricType.LATENCY,
            GroupingType.NONE,
            AggregationType.DEFUALT_AGGREGATION_TYPE,
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
            queryGroupingService.addQueryToGroup(record);
        }

        assertTrue(queryGroupingService.numberOfTopGroups() <= 10); // Should be at most 10 in the min heap
    }

    public void testChangeGroupingType() {
        int numOfRecords = 10;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            queryGroupingService.addQueryToGroup(record);
        }

        int groupsBeforeChange = queryGroupingService.numberOfGroups();
        assertTrue(groupsBeforeChange > 0);

        queryGroupingService.setGroupingType(GroupingType.NONE); // Changing to NONE should clear groups

        int groupsAfterChange = queryGroupingService.numberOfGroups();
        assertEquals(0, groupsAfterChange); // Expect no groups after changing to NONE
    }

    public void testDrainWithMultipleGroupingTypes() {
        int numOfRecords = 20;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            queryGroupingService.addQueryToGroup(record);
        }

        int groupsBeforeDrain = queryGroupingService.numberOfGroups();
        assertTrue(groupsBeforeDrain > 0);

        queryGroupingService.setGroupingType(GroupingType.SIMILARITY);
        queryGroupingService.drain();

        int groupsAfterDrain = queryGroupingService.numberOfGroups();
        assertEquals(0, groupsAfterDrain); // After drain, groups should be cleared
    }

    public void testVaryingTopNSize() {
        int numOfRecords = 30;
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords);

        for (SearchQueryRecord record : records) {
            queryGroupingService.addQueryToGroup(record);
        }

        queryGroupingService.updateTopNSize(15);
        queryGroupingService.drain(); // Clear previous state

        for (SearchQueryRecord record : records) {
            queryGroupingService.addQueryToGroup(record);
        }

        assertEquals(15, queryGroupingService.numberOfTopGroups()); // Should reflect the updated top N size
    }

    public void testAddMeasurementSumAggregationLatency() {
        queryGroupingService = getQueryGroupingService(AggregationType.SUM, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        for (SearchQueryRecord record : records) {
            aggregatedRecord = queryGroupingService.addQueryToGroup(record);
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.LATENCY).longValue();
        }

        assertEquals(expectedSum, aggregatedRecord.getMeasurement(MetricType.LATENCY));
    }

    public void testAddMeasurementAverageAggregationLatency() {
        queryGroupingService = getQueryGroupingService(AggregationType.AVERAGE, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        int expectedCount = 0;
        for (SearchQueryRecord record : records) {
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.LATENCY).longValue();
            aggregatedRecord = queryGroupingService.addQueryToGroup(record);
            expectedCount += 1;
        }

        long expectedAverage = (long) expectedSum / expectedCount;
        assertEquals(expectedAverage, aggregatedRecord.getMeasurement(MetricType.LATENCY));
    }

    public void testAddMeasurementNoneAggregationLatency() {
        queryGroupingService = getQueryGroupingService(AggregationType.NONE, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        for (SearchQueryRecord record : records) {
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.LATENCY).longValue();
            aggregatedRecord = queryGroupingService.addQueryToGroup(record);
        }

        assertEquals(expectedSum, aggregatedRecord.getMeasurement(MetricType.LATENCY));
    }

    public void testAddMeasurementSumAggregationCpu() {
        queryGroupingService = new QueryGroupingService(MetricType.CPU, GroupingType.SIMILARITY, AggregationType.SUM, topQueriesStore, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        for (SearchQueryRecord record : records) {
            aggregatedRecord = queryGroupingService.addQueryToGroup(record);
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.CPU).longValue();
        }

        assertEquals(expectedSum, aggregatedRecord.getMeasurement(MetricType.CPU));
    }

    public void testAddMeasurementAverageAggregationCpu() {
        queryGroupingService = new QueryGroupingService(
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
            aggregatedRecord = queryGroupingService.addQueryToGroup(record);
            expectedCount += 1;
        }

        long expectedAverage = (long) expectedSum / expectedCount;
        assertEquals(expectedAverage, aggregatedRecord.getMeasurement(MetricType.CPU));
    }

    public void testAddMeasurementNoneAggregationCpu() {
        queryGroupingService = new QueryGroupingService(MetricType.CPU, GroupingType.SIMILARITY, AggregationType.NONE, topQueriesStore, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        for (SearchQueryRecord record : records) {
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.CPU).longValue();
            aggregatedRecord = queryGroupingService.addQueryToGroup(record);
        }

        assertEquals(expectedSum, aggregatedRecord.getMeasurement(MetricType.CPU));
    }

    public void testNoneGroupingTypeIllegalArgumentException() {
        queryGroupingService = new QueryGroupingService(MetricType.CPU, GroupingType.NONE, AggregationType.NONE, topQueriesStore, 10);
        int numOfRecords = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(numOfRecords, AggregationType.NONE);

        // Set all records to have the same hashcode for aggregation
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);
        SearchQueryRecord aggregatedRecord = null;

        Number expectedSum = 0;
        for (SearchQueryRecord record : records) {
            expectedSum = expectedSum.longValue() + record.getMeasurement(MetricType.CPU).longValue();
            assertThrows(IllegalArgumentException.class, () -> { queryGroupingService.addQueryToGroup(record); });
        }
    }

    // 1. New query group not existing added to MIN
    public void testNewGroupAddedToMin() {
        queryGroupingService = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            2,
            MetricType.LATENCY,
            List.of(1000, 1100)
        );

        for (List<SearchQueryRecord> recordList : allRecords) {
            for (SearchQueryRecord record : recordList) {
                queryGroupingService.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGroupingService.numberOfTopGroups());
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 2. New query group not existing added to MIN and overflows to MAX
    public void testNewGroupOverflowsMinToMax() {
        queryGroupingService = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            2,
            MetricType.LATENCY,
            List.of(1000, 1100, 900)
        );

        for (List<SearchQueryRecord> recordList : allRecords) {
            for (SearchQueryRecord record : recordList) {
                queryGroupingService.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGroupingService.numberOfTopGroups());
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 3. New query group not existing added to MIN and causes other group to overflow to MAX
    public void testNewGroupCausesOtherGroupOverflowMinToMax() {
        queryGroupingService = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            2,
            MetricType.LATENCY,
            List.of(1000, 1100, 1200)
        );

        for (List<SearchQueryRecord> recordList : allRecords) {
            for (SearchQueryRecord record : recordList) {
                queryGroupingService.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGroupingService.numberOfTopGroups());
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1200L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 4. Existing query group update to MIN increases average
    public void testExistingGroupUpdateToMinIncreaseAverage() {
        queryGroupingService = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(1100, 1200, 1000)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(1300)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                queryGroupingService.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGroupingService.numberOfTopGroups());
        assertEquals(1200L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1200L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 5. Existing query group update to MIN decrease average - stay in MIN
    public void testExistingGroupUpdateToMinDecreaseAverageStayInMin() {
        queryGroupingService = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(1100, 600, 1000)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(700)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                queryGroupingService.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGroupingService.numberOfTopGroups());
        assertEquals(900L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 6. Existing query group update to MIN decrease average - overflows to MAX
    public void testExistingGroupUpdateToMinDecreaseAverageOverflowsToMax() {
        queryGroupingService = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(1199, 1100, 1000)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(1)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                queryGroupingService.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGroupingService.numberOfTopGroups());
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 7. Existing query group update to MAX increases average - stay in MAX
    public void testExistingGroupUpdateToMaxIncreaseAverageStayInMax() {
        queryGroupingService = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(900, 975, 950)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(920)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                queryGroupingService.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGroupingService.numberOfTopGroups());
        assertEquals(950L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(975L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 8. Existing query group update to MAX increases average - promote to MIN
    public void testExistingGroupUpdateToMaxIncreaseAveragePromoteToMin() {
        queryGroupingService = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(900, 975, 950)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(1100)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                queryGroupingService.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGroupingService.numberOfTopGroups());
        assertEquals(975L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1000L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    // 9. Existing query group update to MAX decrease average
    public void testExistingGroupUpdateToMaxDecreaseAverage() {
        queryGroupingService = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(900, 975, 950)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(800)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                queryGroupingService.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGroupingService.numberOfTopGroups());
        assertEquals(950L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(975L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    public void testSwitchGroupingTypeToNone() {
        queryGroupingService = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(900, 975, 950)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(800)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                queryGroupingService.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGroupingService.numberOfTopGroups());
        assertEquals(950L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(975L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));

        queryGroupingService.setGroupingType(GroupingType.NONE);
        assertEquals(0, queryGroupingService.numberOfTopGroups());

        assertThrows(IllegalArgumentException.class, () -> { queryGroupingService.addQueryToGroup(allRecords1.get(0).get(0)); });
    }

    public void testMultipleQueryGroupsUpdates() {
        queryGroupingService = getQueryGroupingService(AggregationType.AVERAGE, 2);

        List<List<SearchQueryRecord>> allRecords1 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(900, 1000, 1000)
        );

        List<List<SearchQueryRecord>> allRecords2 = QueryInsightsTestUtils.generateMultipleQueryInsightsRecordsWithMeasurement(
            1,
            MetricType.LATENCY,
            List.of(800, 400, 1200)
        );

        allRecords1.addAll(allRecords2);

        for (List<SearchQueryRecord> recordList : allRecords1) {
            for (SearchQueryRecord record : recordList) {
                queryGroupingService.addQueryToGroup(record);
            }
        }

        assertEquals(2, queryGroupingService.numberOfTopGroups());
        assertEquals(850L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
        assertEquals(1100L, topQueriesStore.poll().getMeasurement(MetricType.LATENCY));
    }

    private QueryGroupingService getQueryGroupingService(AggregationType aggregationType, int topNSize) {
        return new QueryGroupingService(MetricType.LATENCY, GroupingType.SIMILARITY, aggregationType, topQueriesStore, topNSize);
    }
}
