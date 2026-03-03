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
 * Abstract base class for recommendation rules that analyze queries and generate recommendations.
 * Subclasses must implement {@link #matches(QueryContext)} and {@link #generate(QueryContext)}.
 */
public abstract class RecommendationRule {
    private final String id;
    private final String name;
    private final String description;
    private final int priority;

    /**
     * Constructor with all fields
     * @param id the unique identifier for this rule
     * @param name the human-readable name of this rule
     * @param description the description of what this rule detects
     * @param priority the priority of this rule (lower values = higher priority)
     */
    protected RecommendationRule(String id, String name, String description, int priority) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.priority = priority;
    }

    /**
     * Constructor with default priority (100)
     * @param id the unique identifier for this rule
     * @param name the human-readable name of this rule
     * @param description the description of what this rule detects
     */
    protected RecommendationRule(String id, String name, String description) {
        this(id, name, description, 100);
    }

    /**
     * Get the unique identifier for this rule
     * @return the rule ID
     */
    public String getId() {
        return id;
    }

    /**
     * Get the human-readable name of this rule
     * @return the rule name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the description of what this rule detects
     * @return the rule description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Get the priority of this rule (lower values = higher priority)
     * @return the priority value
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Check if this rule matches the given query context
     * @param context the query context
     * @return true if the rule matches, false otherwise
     */
    public abstract boolean matches(QueryContext context);

    /**
     * Generate a recommendation for the given query context.
     * This method is only called if matches() returns true.
     * @param context the query context
     * @return the generated recommendation
     */
    public abstract Recommendation generate(QueryContext context);
}
