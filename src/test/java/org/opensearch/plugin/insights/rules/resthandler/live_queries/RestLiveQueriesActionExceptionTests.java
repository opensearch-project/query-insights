/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.resthandler.live_queries;

import java.util.Map;
import org.junit.Before;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

public class RestLiveQueriesActionExceptionTests extends OpenSearchTestCase {

    private RestLiveQueriesAction restLiveQueriesAction;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        restLiveQueriesAction = new RestLiveQueriesAction();
    }

    public void testPrepareRequestWithNullRequest() {
        assertThrows(NullPointerException.class, () -> RestLiveQueriesAction.prepareRequest(null));
    }

    public void testPrepareRequestWithEmptyPath() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("").withMethod(RestRequest.Method.GET).build();

        // Should not throw - empty path is valid
        assertNotNull(RestLiveQueriesAction.prepareRequest(request));
    }

    public void testPrepareRequestWithMalformedParameters() {
        Map<String, String> params = Map.of("size", "not_a_number", "sort", "invalid_sort");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).withMethod(RestRequest.Method.GET).build();

        assertThrows(IllegalArgumentException.class, () -> RestLiveQueriesAction.prepareRequest(request));
    }
}
