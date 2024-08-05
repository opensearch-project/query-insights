/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.Tuple;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Handles grouping of search queries based on the GroupingType for the MetricType
 * Following algorithm :
 */
public class QueryGroupingService {

    /**
     * Logger
     */
    private static final Logger log = LogManager.getLogger(QueryGroupingService.class);
    /**
     * Grouping type for the current grouping service
     */
    private GroupingType groupingType;
    /**
     * Metric type for the current grouping service
     */
    private MetricType metricType;

    /**
     * Aggregation type for the current grouping service
     */
    private AggregationType aggregationType;
    /**
     * Map storing groupingId to Tuple containing Aggregate search query record and boolean.
     * SearchQueryRecord: Aggregate search query record to store the aggregate of a metric type based on the aggregation type..
     * Example: Average latency. This query record will be used to store the average latency for multiple query records
     * in this case.
     * boolean: True if the aggregate record is in the Top N queries priority query (min heap) and False if the aggregate
     * record is in the Max Heap
     */
    private Map<String, Tuple<SearchQueryRecord, Boolean>> groupIdToAggSearchQueryRecord;
    /**
     * Min heap to keep track of the Top N query groups and is passed from TopQueriesService as the topQueriesStore
     */
    private PriorityQueue<SearchQueryRecord> minHeapTopQueriesStore;
    /**
     * The Max heap is an overflow data structure used to manage records that exceed the capacity of the Min heap.
     * It stores all records not included in the Top N query results. When the aggregate measurement for one of these
     * records is updated and it now qualifies as part of the Top N, the record is moved from the Max heap to the Min heap,
     * and the records are rearranged accordingly.
     */
    private PriorityQueue<SearchQueryRecord> maxHeapQueryStore;

    /**
     * Top N size based on the configuration set
     */
    private int topNSize;

    public QueryGroupingService(
        MetricType metricType,
        GroupingType groupingType,
        AggregationType aggregationType,
        PriorityQueue<SearchQueryRecord> topQueriesStore,
        int topNSize
    ) {
        this.groupingType = groupingType;
        this.metricType = metricType;
        this.aggregationType = aggregationType;
        this.groupIdToAggSearchQueryRecord = new HashMap<>();
        this.minHeapTopQueriesStore = topQueriesStore;
        this.maxHeapQueryStore = new PriorityQueue<>((a, b) -> SearchQueryRecord.compare(b, a, metricType));

        this.topNSize = topNSize;
    }

    /**
     * Add query to the group based on the GroupType setting.
     * The grouping of metrics will be stored within the searchQueryRecord.
     * @param searchQueryRecord record
     * @return return the search query record that represents the group
     */
    public SearchQueryRecord addQueryToGroup(SearchQueryRecord searchQueryRecord) {
        if (groupingType == GroupingType.NONE) {
            throw new IllegalArgumentException("Do not use addQueryToGroup when GroupingType is None");
        }
        SearchQueryRecord aggregateSearchQueryRecord;
        String groupId = getGroupingId(searchQueryRecord);

        // New group added to the grouping service
        // Add to min PQ and overflow records to max PQ (if the number of records in the min PQ exceeds the configured size N)
        if (!groupIdToAggSearchQueryRecord.containsKey(groupId)) {
            aggregateSearchQueryRecord = searchQueryRecord;
            aggregateSearchQueryRecord.setGroupingId(groupId);
            aggregateSearchQueryRecord.setMeasurementAggregation(metricType, aggregationType);
            addToMinPQOverflowToMaxPQ(aggregateSearchQueryRecord, groupId);
        }
        // Existing group being updated to the grouping service
        // 1. If present in min PQ
        // - remove the record from the min PQ
        // - update the aggregate record (aggregate measurement could increase or decrease)
        // - If max PQ contains elements, add to max PQ and promote any records to min PQ
        // - If max PQ is empty, add to min PQ and overflow any records to max PQ
        // 2. If present in max PQ
        // - remove the record from the max PQ
        // - update the aggregate record (aggregate measurement could increase or decrease)
        // - If min PQ is full, add to min PQ and overflow any records to max PQ
        // - else, add to max PQ and promote any records to min PQ
        else {
            aggregateSearchQueryRecord = groupIdToAggSearchQueryRecord.get(groupId).v1();
            boolean isPresentInMinPQ = groupIdToAggSearchQueryRecord.get(groupId).v2();
            if (isPresentInMinPQ) {
                updateToMinPQ(searchQueryRecord, aggregateSearchQueryRecord, groupId);
            } else {
                updateToMaxPQ(searchQueryRecord, aggregateSearchQueryRecord, groupId);
            }
        }
        return aggregateSearchQueryRecord;
    }

    private void addToMinPQOverflowToMaxPQ(SearchQueryRecord searchQueryRecord, String groupId) {
        minHeapTopQueriesStore.add(searchQueryRecord);
        groupIdToAggSearchQueryRecord.put(groupId, new Tuple<>(searchQueryRecord, true));

        while (minHeapTopQueriesStore.size() > topNSize) {
            SearchQueryRecord recordMovedFromMinToMax = minHeapTopQueriesStore.poll();
            maxHeapQueryStore.add(recordMovedFromMinToMax);
            groupIdToAggSearchQueryRecord.put(recordMovedFromMinToMax.getGroupingId(), new Tuple<>(recordMovedFromMinToMax, false));
        }
    }

    private void updateToMaxPQ(SearchQueryRecord searchQueryRecord, SearchQueryRecord aggregateSearchQueryRecord, String groupId) {
        maxHeapQueryStore.remove(aggregateSearchQueryRecord);
        Number measurementToAdd = searchQueryRecord.getMeasurement(metricType);
        aggregateSearchQueryRecord.addMeasurement(metricType, measurementToAdd);

        if (minHeapTopQueriesStore.size() >= topNSize) {
            addToMinPQOverflowToMaxPQ(aggregateSearchQueryRecord, groupId);
        } else {
            addToMaxPQPromoteToMinPQ(aggregateSearchQueryRecord, groupId);
        }
    }

    private void updateToMinPQ(SearchQueryRecord searchQueryRecord, SearchQueryRecord aggregateSearchQueryRecord, String groupId) {
        minHeapTopQueriesStore.remove(aggregateSearchQueryRecord);
        Number measurementToAdd = searchQueryRecord.getMeasurement(metricType);
        aggregateSearchQueryRecord.addMeasurement(metricType, measurementToAdd);

        if (maxHeapQueryStore.size() > 0) {
            addToMaxPQPromoteToMinPQ(aggregateSearchQueryRecord, groupId);
        } else {
            addToMinPQOverflowToMaxPQ(aggregateSearchQueryRecord, groupId);
        }
    }

    private void addToMaxPQPromoteToMinPQ(SearchQueryRecord aggregateSearchQueryRecord, String groupId) {
        maxHeapQueryStore.add(aggregateSearchQueryRecord);
        groupIdToAggSearchQueryRecord.put(groupId, new Tuple<>(aggregateSearchQueryRecord, false));

        while (minHeapTopQueriesStore.size() < topNSize && !maxHeapQueryStore.isEmpty()) {
            SearchQueryRecord recordMovedFromMaxToMin = maxHeapQueryStore.poll();
            minHeapTopQueriesStore.add(recordMovedFromMaxToMin);
            groupIdToAggSearchQueryRecord.put(recordMovedFromMaxToMin.getGroupingId(), new Tuple<>(recordMovedFromMaxToMin, true));
        }
    }

    /**
     * Drain the internal grouping. Needs to be performed after every window.
     */
    public void drain() {
        log.debug("Number of groups for the current window is " + numberOfGroups());
        groupIdToAggSearchQueryRecord.clear();
        maxHeapQueryStore.clear();
        minHeapTopQueriesStore.clear();
    }

    /**
     * Gives the number of groups as part of the current grouping.
     * @return number of groups
     */
    int numberOfGroups() {
        return groupIdToAggSearchQueryRecord.size();
    }

    /**
     * Gives the number of groups that are part of the top groups
     * @return number of top groups
     */
    int numberOfTopGroups() {
        return minHeapTopQueriesStore.size();
    }

    /**
     * Set Grouping Type
     * @param newGroupingType grouping type
     */
    public void setGroupingType(GroupingType newGroupingType) {
        if (this.groupingType != newGroupingType) {
            this.groupingType = newGroupingType;
            drain();
        }
    }

    public GroupingType getGroupingType() {
        return groupingType;
    }

    /**
     * Get groupingId. This should be query hashcode for SIMILARITY grouping and user_id for USER_ID grouping.
     * @param searchQueryRecord record
     * @return Grouping Id
     */
    private String getGroupingId(SearchQueryRecord searchQueryRecord) {
        switch (groupingType) {
            case SIMILARITY:
                return searchQueryRecord.getAttributes().get(Attribute.QUERY_HASHCODE).toString();
            case NONE:
                throw new IllegalArgumentException("Should not try to group queries if grouping type is NONE");
            default:
                throw new IllegalArgumentException("The following grouping type is not supported : " + groupingType);
        }
    }

    /**
     * Update Top N size
     * @param newSize new size
     */
    public void updateTopNSize(int newSize) {
        this.topNSize = newSize;
    }
}
