/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.recommendations;

/**
 * Type of recommendation action
 */
public enum RecommendationType {
    /**
     * Recommendation to rewrite the query
     */
    QUERY_REWRITE,

    /**
     * Recommendation to change index configuration
     */
    INDEX_CONFIG,

    /**
     * Recommendation to enable a feature
     */
    FEATURE_ENABLE,

    /**
     * Recommendation related to query correctness
     */
    CORRECTNESS
}
