/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.grouper;

import org.opensearch.plugin.insights.core.service.store.PriorityQueueTopQueriesStore;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Interface for grouping search queries based on grouping type for the metric type.
 */
public interface QueryGrouper {

    /**
     * Add query to the group based on the GroupType setting.
     * @param searchQueryRecord record to be added
     * @return the aggregate search query record representing the group
     */
    SearchQueryRecord addQueryToGroup(SearchQueryRecord searchQueryRecord);

    /**
     * Drain the internal grouping. Needs to be performed after every window or if a setting is changed.
     */
    void drain();

    /**
     * Get the min heap queue that holds the top N queries.
     *
     * @return the PriorityBlockingQueue containing the top N queries
     */
    PriorityQueueTopQueriesStore<SearchQueryRecord> getMinHeapTopQueriesStore();

    /**
     * Set the grouping type for this grouper.
     *
     * @param groupingType the grouping type to set
     */
    void setGroupingType(GroupingType groupingType);

    /**
     * Get the current grouping type for this grouper.
     *
     * @return the current grouping type
     */
    GroupingType getGroupingType();

    /**
     * Set the maximum number of groups allowed.
     *
     * @param maxGroups the maximum number of groups
     */
    void setMaxGroups(int maxGroups);

    /**
     * Update the top N size for the grouper.
     *
     * @param topNSize the new top N size
     */
    void updateTopNSize(int topNSize);
}
