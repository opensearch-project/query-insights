/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import org.opensearch.action.ActionType;

/**
 * Transport action for cluster/node level ongoing live queries information.
 */
public class LiveQueriesAction extends ActionType<LiveQueriesResponse> {

    /**
     * The LiveQueriesAction Instance.
     */
    public static final LiveQueriesAction INSTANCE = new LiveQueriesAction();
    /**
     * The name of this Action
     */
    public static final String NAME = "cluster:admin/opensearch/insights/live_queries";

    private LiveQueriesAction() {
        super(NAME, LiveQueriesResponse::new);
    }
}
