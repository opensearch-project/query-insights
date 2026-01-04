/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.resthandler.live_queries;

import java.util.HashMap;
import java.util.Map;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

public class RestLiveQueriesStreamParameterTests extends OpenSearchTestCase {

    public void testStreamParameterParsing() {
        Map<String, String> params = new HashMap<>();
        params.put("cached", "true");
        params.put("verbose", "true");
        params.put("sort", "latency");
        params.put("size", "100");

        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        builder.withPath("/_insights/live_queries");
        builder.withParams(params);
        FakeRestRequest request = builder.build();

        // Test that cached parameter is correctly detected
        boolean useCached = request.paramAsBoolean("cached", false);
        assertTrue(useCached);

        // Test that regular parameters still work
        LiveQueriesRequest liveQueriesRequest = RestLiveQueriesAction.prepareRequest(request);
        assertTrue(liveQueriesRequest.isVerbose());
        assertEquals(100, liveQueriesRequest.getSize());
    }
}
