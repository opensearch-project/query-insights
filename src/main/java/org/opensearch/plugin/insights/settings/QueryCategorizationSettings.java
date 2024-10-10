/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.settings;

import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.unit.ByteSizeValue;

/**
 * Settings for Query Categorization
 */
public class QueryCategorizationSettings {
    /**
     * Enabling search query metrics
     */
    public static final Setting<Boolean> SEARCH_QUERY_METRICS_ENABLED_SETTING = Setting.boolSetting(
        "search.query.metrics.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> SEARCH_QUERY_FIELD_TYPE_CACHE_SIZE_KEY = Setting.memorySizeSetting(
        "search.query.fieldtype.cache.size",
        "0.1%",
        Setting.Property.NodeScope
    );

    /**
     * Default constructor
     */
    public QueryCategorizationSettings() {}

}
