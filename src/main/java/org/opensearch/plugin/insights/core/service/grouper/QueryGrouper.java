/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.grouper;

import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.healthStats.QueryGrouperHealthStats;

/**
 * Interface for grouping search queries based on grouping type for the metric type.
 */
public interface QueryGrouper {

    /**
     * Add query to the group based on the GroupType setting.
     * @param searchQueryRecord record to be added
     * @return the aggregate search query record representing the group
     */
    SearchQueryRecord add(SearchQueryRecord searchQueryRecord);

    /**
     * Drain the internal grouping. Needs to be performed after every window or if a setting is changed.
     */
    void drain();

    /**
     * Set the grouping type for this grouper.
     *
     * @param groupingType the grouping type to set
     * @return grouping type changed
     */
    boolean setGroupingType(GroupingType groupingType);

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
     * @return max groups changed
     */
    boolean setMaxGroups(int maxGroups);

    /**
     * Update the top N size for the grouper.
     *
     * @param topNSize the new top N size
     */
    void updateTopNSize(int topNSize);

    /**
     * Get health stats of the QueryGrouperService
     *
     * @return QueryGrouperHealthStats
     */
    QueryGrouperHealthStats getHealthStats();
}
