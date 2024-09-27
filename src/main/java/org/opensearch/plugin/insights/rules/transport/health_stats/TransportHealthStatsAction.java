/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.health_stats;

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
import org.opensearch.plugin.insights.rules.action.health_stats.HealthStatsAction;
import org.opensearch.plugin.insights.rules.action.health_stats.HealthStatsNodeResponse;
import org.opensearch.plugin.insights.rules.action.health_stats.HealthStatsRequest;
import org.opensearch.plugin.insights.rules.action.health_stats.HealthStatsResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

/**
 * Transport action for cluster/node level health stats information.
 */
public class TransportHealthStatsAction extends TransportNodesAction<
    HealthStatsRequest,
    HealthStatsResponse,
    TransportHealthStatsAction.NodeRequest,
    HealthStatsNodeResponse> {

    private final QueryInsightsService queryInsightsService;

    /**
     * Create the TransportHealthStatsAction Object

     * @param threadPool The OpenSearch thread pool to run async tasks
     * @param clusterService The clusterService of this node
     * @param transportService The TransportService of this node
     * @param queryInsightsService The queryInsightsService associated with this Transport Action
     * @param actionFilters the action filters
     */
    @Inject
    public TransportHealthStatsAction(
        final ThreadPool threadPool,
        final ClusterService clusterService,
        final TransportService transportService,
        final QueryInsightsService queryInsightsService,
        final ActionFilters actionFilters
    ) {
        super(
            HealthStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            HealthStatsRequest::new,
            NodeRequest::new,
            ThreadPool.Names.GENERIC,
            HealthStatsNodeResponse.class
        );
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    protected HealthStatsResponse newResponse(
        final HealthStatsRequest healthStatsRequest,
        final List<HealthStatsNodeResponse> responses,
        final List<FailedNodeException> failures
    ) {
        return new HealthStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(final HealthStatsRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected HealthStatsNodeResponse newNodeResponse(final StreamInput in) throws IOException {
        return new HealthStatsNodeResponse(in);
    }

    @Override
    protected HealthStatsNodeResponse nodeOperation(final NodeRequest nodeRequest) {
        final HealthStatsRequest request = nodeRequest.request;
        return new HealthStatsNodeResponse(clusterService.localNode(), queryInsightsService.getHealthStats());
    }

    /**
     * Inner Node Health Check Request
     */
    public static class NodeRequest extends TransportRequest {

        final HealthStatsRequest request;

        /**
         * Create the NodeResponse object from StreamInput
         *
         * @param in the StreamInput to read the object
         * @throws IOException IOException
         */
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new HealthStatsRequest(in);
        }

        /**
         * Create the NodeResponse object from a HealthStatsRequest
         * @param request the HealthStatsRequest object
         */
        public NodeRequest(final HealthStatsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
