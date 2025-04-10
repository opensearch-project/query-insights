/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.live_queries;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.LIVE_QUERIES_BASE_URI;
import static org.opensearch.rest.RestRequest.Method.GET;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Rest action to get ongoing live queries
 */
public class RestLiveQueriesAction extends BaseRestHandler {

    /**
     * Constructor for RestLiveQueriesAction
     */
    public RestLiveQueriesAction() {}

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, LIVE_QUERIES_BASE_URI),
            new Route(GET, String.format(Locale.ROOT, "%s/{nodeId}", LIVE_QUERIES_BASE_URI))
        );
    }

    @Override
    public String getName() {
        return "query_insights_live_queries_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final LiveQueriesRequest liveQueriesRequest = prepareRequest(request);

        return channel -> client.execute(LiveQueriesAction.INSTANCE, liveQueriesRequest, liveQueriesResponse(channel));
    }

    static LiveQueriesRequest prepareRequest(final RestRequest request) {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        final boolean verbose = request.paramAsBoolean("verbose", true);

        LiveQueriesRequest liveQueriesRequest = new LiveQueriesRequest(verbose, nodesIds);
        liveQueriesRequest.timeout(request.param("timeout"));
        return liveQueriesRequest;
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    private RestResponseListener<LiveQueriesResponse> liveQueriesResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final LiveQueriesResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
