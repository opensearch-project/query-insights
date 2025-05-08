/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.top_queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.action.FailedNodeException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueries;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesAction;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesResponse;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.tasks.Task;
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
     *
     * @param threadPool The OpenSearch thread pool to run async tasks
     * @param clusterService The clusterService of this node
     * @param transportService The TransportService of this node
     * @param queryInsightsService The queryInsightsService associated with this Transport Action
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
    protected void doExecute(Task task, TopQueriesRequest request, ActionListener<TopQueriesResponse> finalListener) {
        final String from = request.getFrom();
        final String to = request.getTo();
        final String id = request.getId();
        final Boolean verbose = request.getVerbose();

        ActionListener<TopQueriesResponse> liveDataCollectionListener = new ActionListener<TopQueriesResponse>() {
            @Override
            public void onResponse(TopQueriesResponse inMemoryQueriesResponse) {
                // inMemoryQueriesResponse contains aggregated in-memory data from nodes and any node failures
                List<TopQueries> liveTopQueries = inMemoryQueriesResponse.getNodes();
                List<FailedNodeException> liveDataFailures = inMemoryQueriesResponse.failures();

                if (from != null && to != null) {
                    // Need to fetch historical data
                    final ThreadContext.StoredContext storedContext = threadPool.getThreadContext().stashContext();
                    try {
                        ActionListener<List<SearchQueryRecord>> historicalDataListener = new ActionListener<List<SearchQueryRecord>>() {
                            @Override
                            public void onResponse(List<SearchQueryRecord> historicalRecords) {
                                storedContext.restore();
                                List<TopQueries> combinedTopQueriesList = new ArrayList<>(liveTopQueries);
                                if (historicalRecords != null && !historicalRecords.isEmpty()) {
                                    combinedTopQueriesList.add(new TopQueries(clusterService.localNode(), historicalRecords));
                                }
                                finalListener.onResponse(
                                    new TopQueriesResponse(
                                        clusterService.getClusterName(),
                                        combinedTopQueriesList,
                                        liveDataFailures,
                                        request.getMetricType()
                                    )
                                );
                            }

                            @Override
                            public void onFailure(Exception e) {
                                storedContext.restore();
                                logger.warn("Failed to fetch historical top queries, proceeding with in-memory data only.", e);
                                finalListener.onResponse(
                                    new TopQueriesResponse(
                                        clusterService.getClusterName(),
                                        liveTopQueries,
                                        liveDataFailures,
                                        request.getMetricType()
                                    )
                                );
                            }
                        };
                        queryInsightsService.getTopQueriesService(request.getMetricType())
                            .getTopQueriesRecordsFromIndex(from, to, id, verbose, historicalDataListener);
                    } catch (Exception e) {
                        storedContext.restore();
                        logger.error("Synchronous failure while initiating historical top queries fetch", e);
                        finalListener.onFailure(e);
                    }
                } else {
                    finalListener.onResponse(inMemoryQueriesResponse);
                }
            }

            @Override
            public void onFailure(Exception e) {
                finalListener.onFailure(e);
            }
        };

        super.doExecute(task, request, liveDataCollectionListener);
    }

    @Override
    protected TopQueriesResponse newResponse(
        final TopQueriesRequest topQueriesRequest,
        final List<TopQueries> collectedNodeResponses,
        final List<FailedNodeException> failures
    ) {
        return new TopQueriesResponse(
            clusterService.getClusterName(),
            collectedNodeResponses,
            failures,
            topQueriesRequest.getMetricType()
        );
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
        return new TopQueries(
            clusterService.localNode(),
            queryInsightsService.getTopQueriesService(request.getMetricType())
                .getTopQueriesRecords(true, null, null, request.getId(), request.getVerbose())
        );
    }

    /**
     * Inner Node Top Queries Request
     */
    public static class NodeRequest extends TransportRequest {

        final TopQueriesRequest request;

        /**
         * Create the NodeRequest object from StreamInput
         *
         * @param in the StreamInput to read the object
         * @throws IOException IOException
         */
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new TopQueriesRequest(in);
        }

        /**
         * Create the NodeRequest object from a TopQueriesRequest
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
