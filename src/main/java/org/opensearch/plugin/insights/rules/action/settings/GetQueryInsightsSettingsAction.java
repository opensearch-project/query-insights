/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.settings;

import org.opensearch.action.ActionType;

/**
 * Transport action to get Query Insights settings.
 */
public class GetQueryInsightsSettingsAction extends ActionType<GetQueryInsightsSettingsResponse> {

    /**
     * The GetQueryInsightsSettingsAction Instance.
     */
    public static final GetQueryInsightsSettingsAction INSTANCE = new GetQueryInsightsSettingsAction();
    /**
     * The name of this Action
     */
    public static final String NAME = "cluster:admin/opensearch/insights/settings/get";

    private GetQueryInsightsSettingsAction() {
        super(NAME, GetQueryInsightsSettingsResponse::new);
    }
}
