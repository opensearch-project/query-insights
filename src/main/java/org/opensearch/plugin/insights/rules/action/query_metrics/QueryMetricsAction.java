/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.query_metrics;

import org.opensearch.action.ActionType;

/**
 * Transport action for cluster/node level query type metrics.
 */
public class QueryMetricsAction extends ActionType<QueryMetricsResponse> {

    /**
     * The QueryMetricsAction Instance.
     */
    public static final QueryMetricsAction INSTANCE = new QueryMetricsAction();
    /**
     * The name of this Action
     */
    public static final String NAME = "cluster:admin/opensearch/insights/query_metrics";

    private QueryMetricsAction() {
        super(NAME, QueryMetricsResponse::new);
    }
}
