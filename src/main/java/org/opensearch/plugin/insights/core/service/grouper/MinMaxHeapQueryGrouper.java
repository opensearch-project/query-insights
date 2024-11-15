/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.grouper;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.Tuple;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.Type;
import org.opensearch.plugin.insights.rules.model.healthStats.QueryGrouperHealthStats;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/**
 * Handles grouping of search queries based on the GroupingType for the MetricType
 * Following algorithm : https://github.com/opensearch-project/OpenSearch/issues/13357#issuecomment-2269706425
 */
public class MinMaxHeapQueryGrouper implements QueryGrouper {

    /**
     * Logger
     */
    private static final Logger log = LogManager.getLogger(MinMaxHeapQueryGrouper.class);
    /**
     * Grouping type for the current grouping service
     */
    private volatile GroupingType groupingType;
    /**
     * Metric type for the current grouping service
     */
    private final MetricType metricType;

    /**
     * Aggregation type for the current grouping service
     */
    private final AggregationType aggregationType;
    /**
     * Map storing groupingId to Tuple containing Aggregate search query record and boolean.
     * SearchQueryRecord: Aggregate search query record to store the aggregate of a metric type based on the aggregation type..
     * Example: Average latency. This query record will be used to store the average latency for multiple query records
     * in this case.
     * boolean: True if the aggregate record is in the Top N queries priority query (min heap) and False if the aggregate
     * record is in the Max Heap
     */
    private final ConcurrentHashMap<String, Tuple<SearchQueryRecord, Boolean>> groupIdToAggSearchQueryRecord;
    /**
     * Min heap to keep track of the Top N query groups and is passed from TopQueriesService as the topQueriesStore
     */
    private final PriorityBlockingQueue<SearchQueryRecord> minHeapTopQueriesStore;
    /**
     * The Max heap is an overflow data structure used to manage records that exceed the capacity of the Min heap.
     * It stores all records not included in the Top N query results. When the aggregate measurement for one of these
     * records is updated and it now qualifies as part of the Top N, the record is moved from the Max heap to the Min heap,
     * and the records are rearranged accordingly.
     */
    private final PriorityBlockingQueue<SearchQueryRecord> maxHeapQueryStore;

    /**
     * Top N size based on the configuration set
     */
    private int topNSize;

    /**
     * To keep track of Top N groups we need to store details of all the groups encountered in the window.
     * This value can be arbitrarily large and we need to limit this.
     * Following is the maximum number of groups that should be tracked when calculating Top N groups and we have a
     * cluster setting to configure.
     */
    private int maxGroups;

    public MinMaxHeapQueryGrouper(
        final MetricType metricType,
        final GroupingType groupingType,
        final AggregationType aggregationType,
        final PriorityBlockingQueue<SearchQueryRecord> topQueriesStore,
        final int topNSize
    ) {
        this.groupingType = groupingType;
        this.metricType = metricType;
        this.aggregationType = aggregationType;
        this.groupIdToAggSearchQueryRecord = new ConcurrentHashMap<>();
        this.minHeapTopQueriesStore = topQueriesStore;
        this.topNSize = topNSize;
        this.maxGroups = QueryInsightsSettings.DEFAULT_GROUPS_EXCLUDING_TOPN_LIMIT;
        this.maxHeapQueryStore = new PriorityBlockingQueue<>(maxGroups, (a, b) -> SearchQueryRecord.compare(b, a, metricType));
    }

    /**
     * Add query to the group based on the GroupType setting.
     * The grouping of metrics will be stored within the searchQueryRecord.
     * @param searchQueryRecord record
     * @return return the search query record that represents the group
     */
    @Override
    public SearchQueryRecord add(final SearchQueryRecord searchQueryRecord) {
        if (groupingType == GroupingType.NONE) {
            throw new IllegalArgumentException("Do not use addQueryToGroup when GroupingType is None");
        }
        SearchQueryRecord aggregateSearchQueryRecord;
        String groupId = getGroupingId(searchQueryRecord);

        // 1) New group added to the grouping service
        // Add to min PQ and overflow records to max PQ (if the number of records in the min PQ exceeds the configured size N)
        // 2) Existing group being updated to the grouping service
        // a. If present in min PQ
        // - remove the record from the min PQ
        // b. If present in max PQ
        // - remove the record from the max PQ
        // Add to min PQ and promote to max
        // If max PQ is empty return else try to promote record from max to min
        if (!groupIdToAggSearchQueryRecord.containsKey(groupId)) {
            if (checkMaxGroupsLimitReached(groupId)) {
                return null;
            }
            aggregateSearchQueryRecord = searchQueryRecord;
            aggregateSearchQueryRecord.setGroupingId(groupId);
            aggregateSearchQueryRecord.setMeasurementAggregation(metricType, aggregationType);
            aggregateSearchQueryRecord.addAttribute(Attribute.TYPE, Type.group);
            addToMinPQ(aggregateSearchQueryRecord, groupId);
        } else {
            aggregateSearchQueryRecord = groupIdToAggSearchQueryRecord.get(groupId).v1();
            boolean isPresentInMinPQ = groupIdToAggSearchQueryRecord.get(groupId).v2();
            if (isPresentInMinPQ) {
                minHeapTopQueriesStore.remove(aggregateSearchQueryRecord);
            } else {
                maxHeapQueryStore.remove(aggregateSearchQueryRecord);
            }
            addAndPromote(searchQueryRecord, aggregateSearchQueryRecord, groupId);
        }
        return aggregateSearchQueryRecord;
    }

    /**
     * Drain the internal grouping. Needs to be performed after every window or if a setting is changed.
     */
    @Override
    public void drain() {
        log.debug("Number of groups for the current window is " + numberOfGroups());
        groupIdToAggSearchQueryRecord.clear();
        maxHeapQueryStore.clear();
        minHeapTopQueriesStore.clear();
    }

    /**
     * Set Grouping Type
     * @param newGroupingType grouping type
     * @return grouping type changed
     */
    @Override
    public boolean setGroupingType(final GroupingType newGroupingType) {
        if (this.groupingType != newGroupingType) {
            this.groupingType = newGroupingType;
            drain();
            return true;
        }
        return false;
    }

    /**
     * Get Grouping Type
     * @return grouping type
     */
    @Override
    public GroupingType getGroupingType() {
        return groupingType;
    }

    /**
     * Set the maximum number of groups that should be tracked when calculating Top N groups.
     * If the value changes, reset the state of the query grouper service by draining all internal data.
     * @param maxGroups max number of groups
     * @return max groups changed
     */
    @Override
    public boolean setMaxGroups(final int maxGroups) {
        if (this.maxGroups != maxGroups) {
            this.maxGroups = maxGroups;
            drain();
            return true;
        }
        return false;
    }

    /**
     * Update Top N size
     * @param newSize new size
     */
    @Override
    public void updateTopNSize(final int newSize) {
        this.topNSize = newSize;
    }

    private void addToMinPQ(final SearchQueryRecord searchQueryRecord, final String groupId) {
        minHeapTopQueriesStore.add(searchQueryRecord);
        groupIdToAggSearchQueryRecord.put(groupId, new Tuple<>(searchQueryRecord, true));
        overflow();
    }

    private void addAndPromote(
        final SearchQueryRecord searchQueryRecord,
        final SearchQueryRecord aggregateSearchQueryRecord,
        final String groupId
    ) {
        Number measurementToAdd = searchQueryRecord.getMeasurement(metricType);
        aggregateSearchQueryRecord.addMeasurement(metricType, measurementToAdd);
        addToMinPQ(aggregateSearchQueryRecord, groupId);
        if (maxHeapQueryStore.isEmpty()) {
            return;
        }
        if (SearchQueryRecord.compare(maxHeapQueryStore.peek(), minHeapTopQueriesStore.peek(), metricType) > 0) {
            SearchQueryRecord recordMovedFromMaxToMin = maxHeapQueryStore.poll();
            addToMinPQ(recordMovedFromMaxToMin, recordMovedFromMaxToMin.getGroupingId());
        }
    }

    private void overflow() {
        if (minHeapTopQueriesStore.size() > topNSize) {
            SearchQueryRecord recordMovedFromMinToMax = minHeapTopQueriesStore.poll();
            maxHeapQueryStore.add(recordMovedFromMinToMax);
            groupIdToAggSearchQueryRecord.put(recordMovedFromMinToMax.getGroupingId(), new Tuple<>(recordMovedFromMinToMax, false));
        }
    }

    private boolean checkMaxGroupsLimitReached(final String groupId) {
        if (maxGroups <= maxHeapQueryStore.size() && minHeapTopQueriesStore.size() >= topNSize) {
            log.warn(
                "Exceeded [{}] setting threshold which is set at {}. Discarding new group with id {}.",
                TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N.getKey(),
                maxGroups,
                groupId
            );
            return true;
        }
        return false;
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
     * Get groupingId. This should be the query hashcode for SIMILARITY grouping and user_id for USER_ID grouping.
     * @param searchQueryRecord record
     * @return Grouping Id
     */
    private String getGroupingId(final SearchQueryRecord searchQueryRecord) {
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
     * Get health stats of the MinMaxHeapQueryGrouperService
     *
     * @return QueryGrouperHealthStats
     */
    public QueryGrouperHealthStats getHealthStats() {
        return new QueryGrouperHealthStats(this.groupIdToAggSearchQueryRecord.size(), this.maxHeapQueryStore.size());
    }
}
