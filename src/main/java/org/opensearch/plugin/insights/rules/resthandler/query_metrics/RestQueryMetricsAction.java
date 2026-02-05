/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.query_metrics;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.QUERY_METRICS_BASE_URI;
import static org.opensearch.rest.RestRequest.Method.GET;

import java.util.List;
import java.util.Set;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.rules.action.query_metrics.QueryMetricsAction;
import org.opensearch.plugin.insights.rules.action.query_metrics.QueryMetricsRequest;
import org.opensearch.plugin.insights.rules.action.query_metrics.QueryMetricsResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Rest action to get query type metrics (aggregated stats by aggregation type)
 */
public class RestQueryMetricsAction extends BaseRestHandler {
    /**
     * Constructor for RestQueryMetricsAction
     */
    public RestQueryMetricsAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, QUERY_METRICS_BASE_URI));
    }

    @Override
    public String getName() {
        return "query_insights_query_metrics_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final QueryMetricsRequest queryMetricsRequest = prepareRequest(request);
        queryMetricsRequest.timeout(request.param("timeout"));

        return channel -> client.execute(QueryMetricsAction.INSTANCE, queryMetricsRequest, queryMetricsResponse(channel));
    }

    static QueryMetricsRequest prepareRequest(final RestRequest request) {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        final String aggregationType = request.param("agg_type", null);
        return new QueryMetricsRequest(aggregationType, nodesIds);
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    RestResponseListener<QueryMetricsResponse> queryMetricsResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final QueryMetricsResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
