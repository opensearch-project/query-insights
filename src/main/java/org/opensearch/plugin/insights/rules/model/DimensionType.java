/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

/**
 * Dimension type for a measurement. Default is NONE and average is used for grouping Top N queries by similarity.
 */
public enum DimensionType {
        NONE,
        AVERAGE,
        SUM;

    public static DimensionType DEFUALT_DIMENSION_TYPE = NONE;
}
