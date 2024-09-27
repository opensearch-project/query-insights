/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.health_stats;

import org.opensearch.action.ActionType;

/**
 * Transport action for cluster/node level query insights plugin health stats.
 */
public class HealthStatsAction extends ActionType<HealthStatsResponse> {

    /**
     * The HealthStatsAction Instance.
     */
    public static final HealthStatsAction INSTANCE = new HealthStatsAction();
    /**
     * The name of this Action
     */
    public static final String NAME = "cluster:admin/opensearch/insights/health_stats";

    private HealthStatsAction() {
        super(NAME, HealthStatsResponse::new);
    }
}
