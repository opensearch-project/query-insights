/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.health_stats;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.QUERY_INSIGHTS_HEALTH_STATS_URI;
import static org.opensearch.rest.RestRequest.Method.GET;

import java.util.List;
import java.util.Set;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.rules.action.health_stats.HealthStatsAction;
import org.opensearch.plugin.insights.rules.action.health_stats.HealthStatsRequest;
import org.opensearch.plugin.insights.rules.action.health_stats.HealthStatsResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Rest action to get operational health stats of the query insights plugin
 */
public class RestHealthStatsAction extends BaseRestHandler {
    /**
     * Constructor for RestHealthStatsAction
     */
    public RestHealthStatsAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, QUERY_INSIGHTS_HEALTH_STATS_URI));
    }

    @Override
    public String getName() {
        return "query_insights_health_stats_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final HealthStatsRequest healthStatsRequest = prepareRequest(request);
        healthStatsRequest.timeout(request.param("timeout"));

        return channel -> client.execute(HealthStatsAction.INSTANCE, healthStatsRequest, healthStatsResponse(channel));
    }

    static HealthStatsRequest prepareRequest(final RestRequest request) {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        return new HealthStatsRequest(nodesIds);
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    RestResponseListener<HealthStatsResponse> healthStatsResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final HealthStatsResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
