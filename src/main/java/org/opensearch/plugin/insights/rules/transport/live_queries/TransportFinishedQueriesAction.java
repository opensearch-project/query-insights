/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import java.io.IOException;
import java.util.List;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.live_queries.FinishedQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.FinishedQueriesNodeResponse;
import org.opensearch.plugin.insights.rules.action.live_queries.FinishedQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.FinishedQueriesResponse;
import org.opensearch.plugin.insights.rules.model.FinishedQueryRecord;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

/**
 * Fans out to all nodes to collect finished queries from each node's local cache.
 */
public class TransportFinishedQueriesAction extends TransportNodesAction<
    FinishedQueriesRequest,
    FinishedQueriesResponse,
    TransportFinishedQueriesAction.NodeRequest,
    FinishedQueriesNodeResponse> {

    private final QueryInsightsService queryInsightsService;

    @Inject
    public TransportFinishedQueriesAction(
        final ThreadPool threadPool,
        final ClusterService clusterService,
        final TransportService transportService,
        final QueryInsightsService queryInsightsService,
        final ActionFilters actionFilters
    ) {
        super(
            FinishedQueriesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            FinishedQueriesRequest::new,
            NodeRequest::new,
            ThreadPool.Names.GENERIC,
            FinishedQueriesNodeResponse.class
        );
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    protected FinishedQueriesResponse newResponse(
        FinishedQueriesRequest request,
        List<FinishedQueriesNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new FinishedQueriesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(FinishedQueriesRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected FinishedQueriesNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new FinishedQueriesNodeResponse(in);
    }

    @Override
    protected FinishedQueriesNodeResponse nodeOperation(NodeRequest nodeRequest) {
        List<FinishedQueryRecord> records = queryInsightsService.getFinishedQueriesCache().getFinishedQueries();
        return new FinishedQueriesNodeResponse(clusterService.localNode(), records);
    }

    /**
     * Inner per-node request.
     */
    public static class NodeRequest extends TransportRequest {

        final FinishedQueriesRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new FinishedQueriesRequest(in);
        }

        public NodeRequest(FinishedQueriesRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
