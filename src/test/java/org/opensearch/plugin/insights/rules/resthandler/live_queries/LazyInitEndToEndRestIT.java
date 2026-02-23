/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.live_queries;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/**
 * Complete end-to-end test for lazy initialization lifecycle
 */
public class LazyInitEndToEndRestIT extends QueryInsightsRestTestCase {

    public void testCompleteLifecycle() throws IOException, InterruptedException {
        // Create test index
        Request createIndex = new Request("PUT", "/test-index");
        client().performRequest(createIndex);

        // Enable finished queries listener
        Request enableRequest = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?use_finished_cache=true");
        client().performRequest(enableRequest);
        Thread.sleep(500);

        // Run search
        Request searchRequest = new Request("GET", "/test-index/_search");
        searchRequest.setJsonEntity("{\"query\":{\"match_all\":{}}}");
        client().performRequest(searchRequest);

        // Wait for capture
        Thread.sleep(1000);

        // Check for finished queries (may need multiple attempts)
        boolean hasFinishedQueries = false;
        for (int i = 0; i < 3; i++) {
            Response response = client().performRequest(
                new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?use_finished_cache=true")
            );
            Map<String, Object> responseMap = entityAsMap(response);
            hasFinishedQueries = !((List<?>) responseMap.get("finished_queries")).isEmpty();
            if (hasFinishedQueries) break;
            Thread.sleep(500);
        }
        assertTrue("Should have finished queries after search", hasFinishedQueries);

        // Wait idle for 5 minutes and 10 seconds (no API calls to keep cache active)
        // Idle check runs every 1 minute, so we need to wait for at least one check cycle after timeout
        Thread.sleep(310000); // 5 minutes + 10 seconds

        // Check for finished queries (should be empty due to cache timeout)
        Response response2 = client().performRequest(
            new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?use_finished_cache=true")
        );
        Map<String, Object> responseMap2 = entityAsMap(response2);
        boolean hasFinishedQueries2 = !((List<?>) responseMap2.get("finished_queries")).isEmpty();
        assertFalse("Should have no finished queries after 5 minutes", hasFinishedQueries2);
    }
}
