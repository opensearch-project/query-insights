/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.recommendations;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link RecommendationType}
 */
public class RecommendationTypeTests extends OpenSearchTestCase {

    public void testEnumValues() {
        RecommendationType[] values = RecommendationType.values();
        assertEquals(4, values.length);
        assertEquals(RecommendationType.QUERY_REWRITE, RecommendationType.valueOf("QUERY_REWRITE"));
        assertEquals(RecommendationType.INDEX_CONFIG, RecommendationType.valueOf("INDEX_CONFIG"));
        assertEquals(RecommendationType.FEATURE_ENABLE, RecommendationType.valueOf("FEATURE_ENABLE"));
        assertEquals(RecommendationType.CORRECTNESS, RecommendationType.valueOf("CORRECTNESS"));
    }

    public void testOrdinals() {
        assertEquals(0, RecommendationType.QUERY_REWRITE.ordinal());
        assertEquals(1, RecommendationType.INDEX_CONFIG.ordinal());
        assertEquals(2, RecommendationType.FEATURE_ENABLE.ordinal());
        assertEquals(3, RecommendationType.CORRECTNESS.ordinal());
    }
}
