/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.settings;

import static org.opensearch.rest.RestRequest.Method.GET;

import java.util.List;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.rules.action.settings.GetQueryInsightsSettingsAction;
import org.opensearch.plugin.insights.rules.action.settings.GetQueryInsightsSettingsRequest;
import org.opensearch.plugin.insights.rules.action.settings.GetQueryInsightsSettingsResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST handler to get Query Insights settings.
 */
public class RestGetQueryInsightsSettingsAction extends BaseRestHandler {

    /**
     * Constructor for RestGetQueryInsightsSettingsAction
     */
    public RestGetQueryInsightsSettingsAction() {}

    @Override
    public String getName() {
        return "query_insights_get_settings_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_insights/settings"),
            new Route(GET, "/_insights/settings/{metric_type}") // Optional: filter by metric type
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final String metricType = request.param("metric_type", null);
        final GetQueryInsightsSettingsRequest getRequest = new GetQueryInsightsSettingsRequest(metricType);

        return channel -> client.execute(GetQueryInsightsSettingsAction.INSTANCE, getRequest, getSettingsResponse(channel));
    }

    private RestResponseListener<GetQueryInsightsSettingsResponse> getSettingsResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final GetQueryInsightsSettingsResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
