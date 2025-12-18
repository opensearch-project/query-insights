/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.settings;

import static org.opensearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsAction;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsRequest;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST handler to update Query Insights settings.
 */
public class RestUpdateQueryInsightsSettingsAction extends BaseRestHandler {

    /**
     * Constructor for RestUpdateQueryInsightsSettingsAction
     */
    public RestUpdateQueryInsightsSettingsAction() {}

    @Override
    public String getName() {
        return "query_insights_update_settings_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_insights/settings"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final UpdateQueryInsightsSettingsRequest updateRequest = prepareRequest(request);

        return channel -> client.execute(UpdateQueryInsightsSettingsAction.INSTANCE, updateRequest, updateSettingsResponse(channel));
    }

    /**
     * Prepare the request from the REST request body.
     * Package-private for testing.
     *
     * @param request the REST request
     * @return the UpdateQueryInsightsSettingsRequest
     * @throws IOException if the request body cannot be parsed
     */
    static UpdateQueryInsightsSettingsRequest prepareRequest(final RestRequest request) throws IOException {
        final UpdateQueryInsightsSettingsRequest updateRequest = new UpdateQueryInsightsSettingsRequest();

        // Parse request body to extract settings
        if (request.hasContent()) {
            try (XContentParser parser = request.contentParser()) {
                final Map<String, Object> settings = parser.map();
                updateRequest.getSettings().putAll(settings);
            }
        }

        return updateRequest;
    }

    /**
     * Creates a RestResponseListener for UpdateQueryInsightsSettingsResponse.
     * Package-private for testing.
     *
     * @param channel the REST channel
     * @return the response listener
     */
    RestResponseListener<UpdateQueryInsightsSettingsResponse> updateSettingsResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final UpdateQueryInsightsSettingsResponse response) throws Exception {
                return buildRestResponse(channel, response);
            }
        };
    }

    /**
     * Builds a REST response from the update settings response.
     * Package-private for testing.
     *
     * @param channel the REST channel
     * @param response the update settings response
     * @return the REST response
     * @throws Exception if an error occurs during response building
     */
    static RestResponse buildRestResponse(final RestChannel channel, final UpdateQueryInsightsSettingsResponse response) throws Exception {
        return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
    }
}
