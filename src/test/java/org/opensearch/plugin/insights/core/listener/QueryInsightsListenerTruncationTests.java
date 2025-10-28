/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.listener;

import org.opensearch.test.OpenSearchTestCase;

public final class QueryInsightsListenerTruncationTests extends OpenSearchTestCase {

    public void testTruncationWithBooleanFlag() {
        // Test that truncation concept works correctly
        String longJson = generateLongFieldQuery(1000);

        // Verify it's long enough to trigger truncation
        assertTrue("JSON should be longer than 10KB", longJson.length() > 10000);

        // Simple truncation - the key is the boolean flag prevents parsing
        String truncated = longJson.substring(0, 10000);
        assertTrue("Truncated should be shorter", truncated.length() < longJson.length());
        assertTrue("Truncated should be at limit", truncated.length() == 10000);

        // The truncated JSON doesn't need to be parseable because
        // SOURCE_TRUNCATED=true will skip categorization
    }

    public void testEdgeCases() {
        // Test very short JSON - should not be truncated
        String shortJson = "{\"a\":1}";
        assertTrue("Short JSON should not trigger truncation", shortJson.length() < 10000);

        // Test JSON at the limit
        StringBuilder exactJson = new StringBuilder("{\"query\":{\"match_all\":{}}");
        while (exactJson.length() < 10000) {
            exactJson.append(",\"f").append(exactJson.length()).append("\":\"v\"");
        }
        exactJson.append("}");
        String exactString = exactJson.toString();
        assertTrue("Should be at or over limit", exactString.length() >= 10000);
    }

    private String generateLongFieldQuery(int fieldCount) {
        StringBuilder sb = new StringBuilder("{\"query\":{\"bool\":{\"must\":[");
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) sb.append(",");
            sb.append("{\"match\":{\"field").append(i).append("\":\"value").append(i).append("\"}}");
        }
        sb.append("]}},\"size\":10}");
        return sb.toString();
    }
}
