/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.live_queries;

import java.io.IOException;
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
        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?cached=true&include_finished=true");

        // 1. First call -> empty (cache not initialized)
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = entityAsMap(response);
        assertTrue("First call should have empty finished_queries", ((java.util.List<?>) responseMap.get("finished_queries")).isEmpty());
        assertTrue("First call should have empty live_queries", ((java.util.List<?>) responseMap.get("live_queries")).isEmpty());

        // 2. Run slow search and call API -> populated (cache initialized)
        Thread searchThread = new Thread(() -> {
            try {
                executeSlowSearch();
            } catch (IOException e) {
                // Expected - script will fail but query runs long enough to be captured
            }
        });
        searchThread.start();
        Thread.sleep(50);

        response = client().performRequest(request);
        responseMap = entityAsMap(response);
        boolean hasLiveQueries = !((java.util.List<?>) responseMap.get("live_queries")).isEmpty();
        assertTrue("Should have live queries during search", hasLiveQueries);

        searchThread.join();

        // 3. Wait for actual idle timeout (5+ minutes)
        Thread.sleep(300001); // Wait longer than 5 minute timeout

        // 4. After idle -> should reinitialize on access
        response = client().performRequest(request);
        responseMap = entityAsMap(response);
        assertTrue(
            "After idle, should have empty arrays (cache reinitialized)",
            ((java.util.List<?>) responseMap.get("finished_queries")).isEmpty()
        );
        assertTrue(
            "After idle, should have empty arrays (cache reinitialized)",
            ((java.util.List<?>) responseMap.get("live_queries")).isEmpty()
        );
    }

    private void executeSlowSearch() throws IOException {
        Request searchRequest = new Request("GET", "/_search");
        searchRequest.setJsonEntity("{\"query\":{\"script\":{\"script\":{\"source\":\"Thread.sleep(200); return true;\"}}}}");
        try {
            client().performRequest(searchRequest);
        } catch (Exception e) {
            // Expected - script will fail but query runs long enough to be captured
        }
    }
}
