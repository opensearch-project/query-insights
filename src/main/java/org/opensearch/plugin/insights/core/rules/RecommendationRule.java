/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.rules;

import org.opensearch.plugin.insights.core.service.recommendations.QueryContext;
import org.opensearch.plugin.insights.rules.model.recommendations.Recommendation;

/**
 * Interface for recommendation rules that analyze queries and generate recommendations
 */
public interface RecommendationRule {
    /**
     * Get the unique identifier for this rule
     * @return the rule ID
     */
    String getId();

    /**
     * Get the human-readable name of this rule
     * @return the rule name
     */
    String getName();

    /**
     * Get the description of what this rule detects
     * @return the rule description
     */
    String getDescription();

    /**
     * Check if this rule matches the given query context
     * @param context the query context
     * @return true if the rule matches, false otherwise
     */
    boolean matches(QueryContext context);

    /**
     * Generate a recommendation for the given query context
     * This method is only called if matches() returns true
     * @param context the query context
     * @return the generated recommendation
     */
    Recommendation generate(QueryContext context);

    /**
     * Get the priority of this rule (lower values = higher priority)
     * @return the priority value
     */
    default int getPriority() {
        return 100;
    }

    /**
     * Check if this rule is enabled
     * @return true if enabled, false otherwise
     */
    default boolean isEnabled() {
        return true;
    }
}
