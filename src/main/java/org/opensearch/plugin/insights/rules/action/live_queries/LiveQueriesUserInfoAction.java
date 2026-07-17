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
 * Transport action to resolve live query user info from each node's local map.
 */
public class LiveQueriesUserInfoAction extends ActionType<LiveQueriesUserInfoResponse> {

    public static final LiveQueriesUserInfoAction INSTANCE = new LiveQueriesUserInfoAction();
    public static final String NAME = "cluster:admin/opensearch/insights/live_queries_user_info";

    private LiveQueriesUserInfoAction() {
        super(NAME, LiveQueriesUserInfoResponse::new);
    }
}
