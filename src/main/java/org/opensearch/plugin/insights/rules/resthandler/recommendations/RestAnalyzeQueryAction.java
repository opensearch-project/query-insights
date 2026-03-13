/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.recommendations;

import static org.opensearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.action.recommendations.AnalyzeQueryAction;
import org.opensearch.plugin.insights.rules.action.recommendations.AnalyzeQueryRequest;
import org.opensearch.plugin.insights.rules.action.recommendations.AnalyzeQueryResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Rest action to analyze a query and generate recommendations
 */
public class RestAnalyzeQueryAction extends BaseRestHandler {

    private static final String RECOMMENDATIONS_BASE_URI = "/_insights/recommendations";
    private static final String ANALYZE_URI = RECOMMENDATIONS_BASE_URI + "/analyze";

    /**
     * Constructor for RestAnalyzeQueryAction
     */
    public RestAnalyzeQueryAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, ANALYZE_URI));
    }

    @Override
    public String getName() {
        return "query_insights_analyze_query_action";
    }

    @Override
    @SuppressWarnings("unchecked")
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        // Parse request body
        MediaType mediaType = request.getMediaType();
        if (mediaType == null) {
            mediaType = MediaTypeRegistry.JSON;
        }

        Map<String, Object> body = XContentHelper.convertToMap(request.content(), false, mediaType).v2();

        // Extract query source
        Object queryObj = body.get("query");
        if (queryObj == null) {
            throw new IllegalArgumentException("Request body must contain 'query' field");
        }

        // Convert query object to JSON string
        String querySource;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.value(queryObj);
            BytesReference bytes = BytesReference.bytes(builder);
            querySource = XContentHelper.convertToJson(bytes, false, MediaTypeRegistry.JSON);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid query format: " + e.getMessage());
        }

        // Extract indices (optional)
        List<String> indices = null;
        Object indicesObj = body.get("indices");
        if (indicesObj instanceof List) {
            indices = (List<String>) indicesObj;
        } else if (indicesObj instanceof String) {
            indices = Arrays.asList(((String) indicesObj).split(","));
        }

        // Create request
        final AnalyzeQueryRequest analyzeRequest = new AnalyzeQueryRequest(querySource, indices);

        return channel -> client.execute(AnalyzeQueryAction.INSTANCE, analyzeRequest, analyzeQueryResponse(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    private RestResponseListener<AnalyzeQueryResponse> analyzeQueryResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final AnalyzeQueryResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
