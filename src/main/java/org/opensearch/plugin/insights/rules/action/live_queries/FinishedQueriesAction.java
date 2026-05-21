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
 * Transport action to fetch finished queries from all nodes.
 */
public class FinishedQueriesAction extends ActionType<FinishedQueriesResponse> {

    public static final FinishedQueriesAction INSTANCE = new FinishedQueriesAction();
    public static final String NAME = "cluster:admin/opensearch/insights/finished_queries";

    private FinishedQueriesAction() {
        super(NAME, FinishedQueriesResponse::new);
    }
}
