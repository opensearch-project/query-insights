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
 * End-to-end test for lazy initialization lifecycle.
 * Idle expiry behavior is covered by FinishedQueriesCacheTimeoutTests unit test.
 */
public class LazyInitEndToEndRestIT extends QueryInsightsRestTestCase {

    public void testCacheActivatesAndCapturesFinishedQueries() throws IOException, InterruptedException {
        // Create test index
        client().performRequest(new Request("PUT", "/test-index"));

        // Prime the cache on all nodes — the cache is lazy and only activates on the
        // first getFinishedQueries() API call. The REST client round-robins across nodes,
        // so we send enough priming requests to cover every node in the cluster.
        for (int i = 0; i < 10; i++) {
            client().performRequest(new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?use_finished_cache=true"));
        }

        // Run a search to produce a finished query
        Request searchRequest = new Request("GET", "/test-index/_search");
        searchRequest.setJsonEntity("{\"query\":{\"match_all\":{}}}");
        client().performRequest(searchRequest);

        // Wait for capture
        Thread.sleep(1000);

        // Poll until finished queries appear
        boolean hasFinishedQueries = false;
        for (int i = 0; i < 5; i++) {
            Response response = client().performRequest(
                new Request("GET", QueryInsightsSettings.LIVE_QUERIES_BASE_URI + "?use_finished_cache=true")
            );
            Map<String, Object> responseMap = entityAsMap(response);
            hasFinishedQueries = !((List<?>) responseMap.get("finished_queries")).isEmpty();
            if (hasFinishedQueries) break;
            Thread.sleep(500);
        }
        assertTrue("Cache should activate and capture finished queries", hasFinishedQueries);
    }
}
