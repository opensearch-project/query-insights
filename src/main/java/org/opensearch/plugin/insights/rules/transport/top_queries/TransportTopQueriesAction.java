/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.top_queries;

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
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueries;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesAction;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

/**
 * Transport action for cluster/node level top queries information.
 */
public class TransportTopQueriesAction extends TransportNodesAction<
    TopQueriesRequest,
    TopQueriesResponse,
    TransportTopQueriesAction.NodeRequest,
    TopQueries> {

    private final QueryInsightsService queryInsightsService;

    /**
     * Create the TransportTopQueriesAction Object

     * @param threadPool The OpenSearch thread pool to run async tasks
     * @param clusterService The clusterService of this node
     * @param transportService The TransportService of this node
     * @param queryInsightsService The topQueriesByLatencyService associated with this Transport Action
     * @param actionFilters the action filters
     */
    @Inject
    public TransportTopQueriesAction(
        final ThreadPool threadPool,
        final ClusterService clusterService,
        final TransportService transportService,
        final QueryInsightsService queryInsightsService,
        final ActionFilters actionFilters
    ) {
        super(
            TopQueriesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            TopQueriesRequest::new,
            NodeRequest::new,
            ThreadPool.Names.GENERIC,
            TopQueries.class
        );
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    protected TopQueriesResponse newResponse(
        final TopQueriesRequest topQueriesRequest,
        final List<TopQueries> responses,
        final List<FailedNodeException> failures
    ) {
        final String from = topQueriesRequest.getFrom();
        final String to = topQueriesRequest.getTo();
        final String id = topQueriesRequest.getId();
        final Boolean verbose = topQueriesRequest.getVerbose();
        if (from != null && to != null) {
            // If date range is provided, fetch historical data from local index
            responses.add(
                new TopQueries(
                    clusterService.localNode(),
                    queryInsightsService.getTopQueriesService(topQueriesRequest.getMetricType())
                        .getTopQueriesRecordsFromIndex(from, to, id, verbose)
                )
            );
        }
        return new TopQueriesResponse(clusterService.getClusterName(), responses, failures, topQueriesRequest.getMetricType());
    }

    @Override
    protected NodeRequest newNodeRequest(final TopQueriesRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected TopQueries newNodeResponse(final StreamInput in) throws IOException {
        return new TopQueries(in);
    }

    @Override
    protected TopQueries nodeOperation(final NodeRequest nodeRequest) {
        final TopQueriesRequest request = nodeRequest.request;
        final String from = request.getFrom();
        final String to = request.getTo();
        final String id = request.getId();
        final Boolean verbose = request.getVerbose();
        return new TopQueries(
            clusterService.localNode(),
            queryInsightsService.getTopQueriesService(request.getMetricType()).getTopQueriesRecords(true, from, to, id, verbose)
        );
    }

    /**
     * Inner Node Top Queries Request
     */
    public static class NodeRequest extends TransportRequest {

        final TopQueriesRequest request;

        /**
         * Create the NodeResponse object from StreamInput
         *
         * @param in the StreamInput to read the object
         * @throws IOException IOException
         */
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new TopQueriesRequest(in);
        }

        /**
         * Create the NodeResponse object from a TopQueriesRequest
         * @param request the TopQueriesRequest object
         */
        public NodeRequest(final TopQueriesRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
