/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.live_queries;

import java.util.Map;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

/**
 * Unit tests for cached and include_finished parameters in {@link RestLiveQueriesAction}.
 */
public class RestLiveQueriesActionCachedTests extends OpenSearchTestCase {

    public void testPrepareRequestWithCachedTrue() {
        Map<String, String> params = Map.of("cached", "true");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();

        LiveQueriesRequest liveQueriesRequest = RestLiveQueriesAction.prepareRequest(request);
        assertTrue(liveQueriesRequest.isCached());
        assertFalse(liveQueriesRequest.isIncludeFinished());
    }

    public void testPrepareRequestWithIncludeFinishedTrue() {
        Map<String, String> params = Map.of("include_finished", "true");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();

        LiveQueriesRequest liveQueriesRequest = RestLiveQueriesAction.prepareRequest(request);
        assertFalse(liveQueriesRequest.isCached());
        assertTrue(liveQueriesRequest.isIncludeFinished());
    }

    public void testPrepareRequestWithBothCachedAndIncludeFinished() {
        Map<String, String> params = Map.of("cached", "true", "include_finished", "true");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();

        LiveQueriesRequest liveQueriesRequest = RestLiveQueriesAction.prepareRequest(request);
        assertTrue(liveQueriesRequest.isCached());
        assertTrue(liveQueriesRequest.isIncludeFinished());
    }

    public void testPrepareRequestWithCachedFalse() {
        Map<String, String> params = Map.of("cached", "false");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();

        LiveQueriesRequest liveQueriesRequest = RestLiveQueriesAction.prepareRequest(request);
        assertFalse(liveQueriesRequest.isCached());
    }

    public void testPrepareRequestWithIncludeFinishedFalse() {
        Map<String, String> params = Map.of("include_finished", "false");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();

        LiveQueriesRequest liveQueriesRequest = RestLiveQueriesAction.prepareRequest(request);
        assertFalse(liveQueriesRequest.isIncludeFinished());
    }

    public void testPrepareRequestWithWlmGroupId() {
        Map<String, String> params = Map.of("wlmGroupId", "TEST_WORKLOAD_GROUP");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();

        LiveQueriesRequest liveQueriesRequest = RestLiveQueriesAction.prepareRequest(request);
        assertEquals("TEST_WORKLOAD_GROUP", liveQueriesRequest.getWlmGroupId());
    }

    public void testPrepareRequestWithAllNewParameters() {
        Map<String, String> params = Map.of(
            "cached",
            "true",
            "include_finished",
            "true",
            "wlmGroupId",
            "CUSTOM_GROUP",
            "verbose",
            "false",
            "sort",
            "memory",
            "size",
            "15"
        );
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();

        LiveQueriesRequest liveQueriesRequest = RestLiveQueriesAction.prepareRequest(request);
        assertTrue(liveQueriesRequest.isCached());
        assertTrue(liveQueriesRequest.isIncludeFinished());
        assertEquals("CUSTOM_GROUP", liveQueriesRequest.getWlmGroupId());
        assertFalse(liveQueriesRequest.isVerbose());
        assertEquals(MetricType.MEMORY, liveQueriesRequest.getSortBy());
        assertEquals(15, liveQueriesRequest.getSize());
    }

    public void testPrepareRequestInvalidCachedParameter() {
        Map<String, String> params = Map.of("cached", "invalid");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();

        assertThrows(IllegalArgumentException.class, () -> RestLiveQueriesAction.prepareRequest(request));
    }

    public void testPrepareRequestInvalidIncludeFinishedParameter() {
        Map<String, String> params = Map.of("include_finished", "invalid");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(QueryInsightsSettings.LIVE_QUERIES_BASE_URI)
            .withParams(params)
            .withMethod(RestRequest.Method.GET)
            .build();

        assertThrows(IllegalArgumentException.class, () -> RestLiveQueriesAction.prepareRequest(request));
    }
}
