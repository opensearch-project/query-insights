/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.recommendations;

/**
 * Direction of impact for a recommendation
 */
public enum Direction {
    /**
     * Metric will increase
     */
    INCREASE,

    /**
     * Metric will decrease
     */
    DECREASE,

    /**
     * Metric will not change
     */
    NEUTRAL
}
