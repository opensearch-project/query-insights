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
import java.util.stream.Collectors;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
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
    static final Set<String> ALLOWED_METRICS = MetricType.allMetricTypes().stream().map(MetricType::toString).collect(Collectors.toSet());

    /**
     * Constructor for RestLiveQueriesAction
     */
    public RestLiveQueriesAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, LIVE_QUERIES_BASE_URI));
    }

    @Override
    public String getName() {
        return "query_insights_live_queries_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final boolean useCached = request.paramAsBoolean("cached", false);
        final boolean includeFinished = request.paramAsBoolean("include_finished", false);

        final LiveQueriesRequest liveQueriesRequest = prepareRequest(request);
        liveQueriesRequest.setCached(useCached);
        liveQueriesRequest.setIncludeFinished(includeFinished);

        if (useCached) {
            return channel -> client.execute(LiveQueriesAction.INSTANCE, liveQueriesRequest, new RestResponseListener<>(channel) {
                @Override
                public RestResponse buildResponse(LiveQueriesResponse response) throws Exception {
                    return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
                }
            });
        } else {
            return channel -> client.execute(LiveQueriesAction.INSTANCE, liveQueriesRequest, liveQueriesResponse(channel));
        }
    }

    static LiveQueriesRequest prepareRequest(final RestRequest request) {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        final boolean verbose = request.paramAsBoolean("verbose", true);
        final String sortParam = request.param("sort", MetricType.LATENCY.toString());
        final String wlmGroupId = request.param("wlmGroupId", null);
        final String taskId = request.param("task_id", null);

        if (!ALLOWED_METRICS.contains(sortParam)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "request [%s] contains invalid sort metric type [%s]", request.path(), sortParam)
            );
        }
        final MetricType sortBy = MetricType.fromString(sortParam);
        final int size = request.paramAsInt("size", QueryInsightsSettings.DEFAULT_LIVE_QUERIES_SIZE);
        if (size <= 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "request [%s] contains invalid size parameter [%d]. size must be positive", request.path(), size)
            );
        }
        LiveQueriesRequest liveQueriesRequest = new LiveQueriesRequest(verbose, sortBy, size, nodesIds, wlmGroupId);
        liveQueriesRequest.setTaskId(taskId);
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
