/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.recommendations;

import org.opensearch.action.ActionType;

/**
 * Action to analyze a query and generate recommendations
 */
public class AnalyzeQueryAction extends ActionType<AnalyzeQueryResponse> {

    /**
     * The AnalyzeQueryAction instance
     */
    public static final AnalyzeQueryAction INSTANCE = new AnalyzeQueryAction();

    /**
     * The name of this action
     */
    public static final String NAME = "cluster:admin/opensearch/insights/recommendations/analyze";

    private AnalyzeQueryAction() {
        super(NAME, AnalyzeQueryResponse::new);
    }
}
