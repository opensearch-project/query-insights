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
 * Transport action to update Query Insights settings.
 */
public class UpdateQueryInsightsSettingsAction extends ActionType<UpdateQueryInsightsSettingsResponse> {

    /**
     * The UpdateQueryInsightsSettingsAction Instance.
     */
    public static final UpdateQueryInsightsSettingsAction INSTANCE = new UpdateQueryInsightsSettingsAction();
    /**
     * The name of this Action
     */
    public static final String NAME = "cluster:admin/opensearch/insights/settings/update";

    private UpdateQueryInsightsSettingsAction() {
        super(NAME, UpdateQueryInsightsSettingsResponse::new);
    }
}
