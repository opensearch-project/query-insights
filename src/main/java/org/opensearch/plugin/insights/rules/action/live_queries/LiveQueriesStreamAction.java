/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import org.opensearch.action.ActionType;

/**
 * Action for streaming live queries
 */
public class LiveQueriesStreamAction extends ActionType<LiveQueriesStreamResponse> {

    public static final LiveQueriesStreamAction INSTANCE = new LiveQueriesStreamAction();
    public static final String NAME = "cluster:monitor/query_insights/live_queries/stream";

    private LiveQueriesStreamAction() {
        super(NAME, LiveQueriesStreamResponse::new);
    }
}