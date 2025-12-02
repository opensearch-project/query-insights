/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.resthandler.live_queries;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.LIVE_QUERIES_BASE_URI;
import static org.opensearch.rest.RestRequest.Method.GET;

import java.util.List;
import java.util.Locale;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesStreamAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesStreamRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesStreamResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Rest action for streaming live queries
 */
public class RestLiveQueriesStreamAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, LIVE_QUERIES_BASE_URI + "/stream"),
            new Route(GET, String.format(Locale.ROOT, "%s/stream/{nodeId}", LIVE_QUERIES_BASE_URI))
        );
    }

    @Override
    public String getName() {
        return "query_insights_live_queries_stream_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        final LiveQueriesStreamRequest streamRequest = new LiveQueriesStreamRequest(nodesIds);
        
        return channel -> client.execute(LiveQueriesStreamAction.INSTANCE, streamRequest, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(LiveQueriesStreamResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        });
    }

    @Override
    protected java.util.Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}