/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.query_metrics;

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
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.query_metrics.QueryMetricsAction;
import org.opensearch.plugin.insights.rules.action.query_metrics.QueryMetricsNodeResponse;
import org.opensearch.plugin.insights.rules.action.query_metrics.QueryMetricsRequest;
import org.opensearch.plugin.insights.rules.action.query_metrics.QueryMetricsResponse;
import org.opensearch.plugin.insights.rules.model.QueryTypeMetrics;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

/**
 * Transport action for cluster/node level query type metrics.
 */
public class TransportQueryMetricsAction extends TransportNodesAction<
    QueryMetricsRequest,
    QueryMetricsResponse,
    TransportQueryMetricsAction.NodeRequest,
    QueryMetricsNodeResponse> {

    private final QueryInsightsService queryInsightsService;

    /**
     * Create the TransportQueryMetricsAction Object
     *
     * @param threadPool The OpenSearch thread pool to run async tasks
     * @param clusterService The clusterService of this node
     * @param transportService The TransportService of this node
     * @param queryInsightsService The queryInsightsService associated with this Transport Action
     * @param actionFilters the action filters
     */
    @Inject
    public TransportQueryMetricsAction(
        final ThreadPool threadPool,
        final ClusterService clusterService,
        final TransportService transportService,
        final QueryInsightsService queryInsightsService,
        final ActionFilters actionFilters
    ) {
        super(
            QueryMetricsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            QueryMetricsRequest::new,
            NodeRequest::new,
            ThreadPool.Names.GENERIC,
            QueryMetricsNodeResponse.class
        );
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    protected QueryMetricsResponse newResponse(
        final QueryMetricsRequest queryMetricsRequest,
        final List<QueryMetricsNodeResponse> responses,
        final List<FailedNodeException> failures
    ) {
        return new QueryMetricsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(final QueryMetricsRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected QueryMetricsNodeResponse newNodeResponse(final StreamInput in) throws IOException {
        return new QueryMetricsNodeResponse(in);
    }

    @Override
    protected QueryMetricsNodeResponse nodeOperation(final NodeRequest nodeRequest) {
        final QueryMetricsRequest request = nodeRequest.request;
        Map<String, QueryTypeMetrics> metrics = queryInsightsService.getSearchQueryCategorizer()
            .getSearchQueryCounters()
            .getAggregationTypeMetrics();

        // Filter by aggregation type if specified
        String aggregationType = request.getAggregationType();
        if (aggregationType != null && !aggregationType.isEmpty()) {
            QueryTypeMetrics specificMetrics = metrics.get(aggregationType);
            if (specificMetrics != null) {
                Map<String, QueryTypeMetrics> filtered = new HashMap<>();
                filtered.put(aggregationType, specificMetrics);
                metrics = filtered;
            } else {
                metrics = new HashMap<>();
            }
        }

        return new QueryMetricsNodeResponse(clusterService.localNode(), metrics);
    }

    /**
     * Inner Node Query Metrics Request
     */
    public static class NodeRequest extends TransportRequest {

        final QueryMetricsRequest request;

        /**
         * Create the NodeResponse object from StreamInput
         *
         * @param in the StreamInput to read the object
         * @throws IOException IOException
         */
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new QueryMetricsRequest(in);
        }

        /**
         * Create the NodeResponse object from a QueryMetricsRequest
         * @param request the QueryMetricsRequest object
         */
        public NodeRequest(final QueryMetricsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
