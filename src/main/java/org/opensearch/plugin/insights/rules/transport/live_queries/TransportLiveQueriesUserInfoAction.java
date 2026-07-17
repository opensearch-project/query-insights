/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesUserInfoAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesUserInfoNodeResponse;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesUserInfoRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesUserInfoResponse;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueryUserInfoDTO;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

/**
 * Fans out to all nodes to resolve live query user info from each node's local map.
 * <p>
 * The listener captures user identity on the node that coordinates a search, keyed by
 * {@code nodeId:taskId}. Since the live queries API can be served by any node, this fan-out
 * lets each node resolve the task keys it owns so identity is populated regardless of which
 * node handles the API request.
 */
public class TransportLiveQueriesUserInfoAction extends TransportNodesAction<
    LiveQueriesUserInfoRequest,
    LiveQueriesUserInfoResponse,
    TransportLiveQueriesUserInfoAction.NodeRequest,
    LiveQueriesUserInfoNodeResponse> {

    private final QueryInsightsService queryInsightsService;

    @Inject
    public TransportLiveQueriesUserInfoAction(
        final ThreadPool threadPool,
        final ClusterService clusterService,
        final TransportService transportService,
        final QueryInsightsService queryInsightsService,
        final ActionFilters actionFilters
    ) {
        super(
            LiveQueriesUserInfoAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            LiveQueriesUserInfoRequest::new,
            NodeRequest::new,
            ThreadPool.Names.GENERIC,
            LiveQueriesUserInfoNodeResponse.class
        );
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    protected LiveQueriesUserInfoResponse newResponse(
        LiveQueriesUserInfoRequest request,
        List<LiveQueriesUserInfoNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new LiveQueriesUserInfoResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(LiveQueriesUserInfoRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected LiveQueriesUserInfoNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new LiveQueriesUserInfoNodeResponse(in);
    }

    @Override
    protected LiveQueriesUserInfoNodeResponse nodeOperation(NodeRequest nodeRequest) {
        Map<String, UserPrincipalContext.UserPrincipalInfo> resolved = queryInsightsService.getLiveQueryUserInfoForKeys(
            nodeRequest.request.getTaskKeys()
        );
        Map<String, LiveQueryUserInfoDTO> dtos = new HashMap<>();
        for (Map.Entry<String, UserPrincipalContext.UserPrincipalInfo> entry : resolved.entrySet()) {
            dtos.put(entry.getKey(), new LiveQueryUserInfoDTO(entry.getValue()));
        }
        return new LiveQueriesUserInfoNodeResponse(clusterService.localNode(), dtos);
    }

    /**
     * Inner per-node request.
     */
    public static class NodeRequest extends TransportRequest {

        final LiveQueriesUserInfoRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new LiveQueriesUserInfoRequest(in);
        }

        public NodeRequest(LiveQueriesUserInfoRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
