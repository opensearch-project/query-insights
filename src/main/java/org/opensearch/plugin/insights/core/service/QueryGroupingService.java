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
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.DimensionType;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Handles grouping of search queries based on the GroupingType for the MetricType
 */
public class QueryGroupingService {

    private static final Logger log = LogManager.getLogger(QueryGroupingService.class);
    private GroupingType groupingType;
    private MetricType metricType;

    private DimensionType dimensionType;
    private Map<String, Tuple<SearchQueryRecord, Boolean>> groupIdToAggSearchQueryRecord;
    private PriorityQueue<SearchQueryRecord> minHeapTopQueriesStore;
    private PriorityQueue<SearchQueryRecord> maxHeapQueryStore;

    private int topNSize;

    public QueryGroupingService(
        MetricType metricType,
        GroupingType groupingType,
        DimensionType dimensionType,
        PriorityQueue<SearchQueryRecord> topQueriesStore,
        int topNSize
    ) {
        this.groupingType = groupingType;
        this.metricType = metricType;
        this.dimensionType = dimensionType;
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

        // New group
        if (!groupIdToAggSearchQueryRecord.containsKey(groupId)) {
            aggregateSearchQueryRecord = searchQueryRecord;
            aggregateSearchQueryRecord.setGroupingId(groupId);
            aggregateSearchQueryRecord.setMeasurementDimension(metricType, dimensionType);
            addToMinPQ(aggregateSearchQueryRecord, groupId);
        }
        // Old group
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

    private void addToMinPQ(SearchQueryRecord searchQueryRecord, String groupId) {
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

        addToMinPQ(aggregateSearchQueryRecord, groupId);

    }

    private void updateToMinPQ(SearchQueryRecord searchQueryRecord, SearchQueryRecord aggregateSearchQueryRecord, String groupId) {
        minHeapTopQueriesStore.remove(aggregateSearchQueryRecord);
        Number measurementToAdd = searchQueryRecord.getMeasurement(metricType);
        aggregateSearchQueryRecord.addMeasurement(metricType, measurementToAdd);

        if (maxHeapQueryStore.size() > 0) {
            addToMaxPQ(aggregateSearchQueryRecord, groupId);
        } else {
            addToMinPQ(aggregateSearchQueryRecord, groupId);
        }
    }

    private void addToMaxPQ(SearchQueryRecord aggregateSearchQueryRecord, String groupId) {
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
