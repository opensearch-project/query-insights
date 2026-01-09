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
 * Integration tests for cache idle timeout behavior
 */
public class CacheIdleTimeoutRestIT extends QueryInsightsRestTestCase {

    public void testLiveCacheStartsOnAccess() throws IOException {
        // Access live queries to start cache
        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI);
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertTrue(responseMap.containsKey("live_queries"));
    }

    public void testFinishedCacheStartsOnAccess() throws IOException {
        // Access finished queries to start cache
        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?include_finished=true");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        assertTrue(responseMap.containsKey("live_queries"));
        assertTrue(responseMap.containsKey("finished_queries"));
    }

    public void testCacheIdleTimeout() throws IOException, InterruptedException {
        // Start cache
        Request request = new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI);
        client().performRequest(request);

        // Wait a short time (simulating idle)
        Thread.sleep(1000);

        // Cache should still work after short wait
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
