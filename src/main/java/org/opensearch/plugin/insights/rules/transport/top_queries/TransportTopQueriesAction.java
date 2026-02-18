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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.core.auth.TopQueriesRbacFilter;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext.UserPrincipalInfo;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueries;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesAction;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesResponse;
import org.opensearch.plugin.insights.rules.model.FilterByMode;
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

    private static final Logger log = LogManager.getLogger(TransportTopQueriesAction.class);

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

    ActionListener<TopQueriesResponse> createInMemoryDataCollectionListener(
        TopQueriesRequest request,
        ActionListener<TopQueriesResponse> finalListener
    ) {
        return new ActionListener<TopQueriesResponse>() {
            @Override
            public void onResponse(TopQueriesResponse inMemoryQueriesResponse) {
                handleInMemoryDataResponse(request, inMemoryQueriesResponse, finalListener);
            }

            @Override
            public void onFailure(Exception e) {
                finalListener.onFailure(e);
            }
        };
    }

    void handleInMemoryDataResponse(
        TopQueriesRequest request,
        TopQueriesResponse inMemoryQueriesResponse,
        ActionListener<TopQueriesResponse> finalListener
    ) {
        List<TopQueries> inMemoryTopQueries = inMemoryQueriesResponse.getNodes();
        List<FailedNodeException> inMemoryDataFailures = inMemoryQueriesResponse.failures();
        String from = request.getFrom();
        String to = request.getTo();
        if (from != null && to != null) {
            fetchHistoricalData(request, inMemoryTopQueries, inMemoryDataFailures, finalListener);
        } else {
            finalListener.onResponse(inMemoryQueriesResponse);
        }
    }

    void fetchHistoricalData(
        TopQueriesRequest request,
        List<TopQueries> inMemoryTopQueries,
        List<FailedNodeException> inMemoryDataFailures,
        ActionListener<TopQueriesResponse> finalListener
    ) {
        String from = request.getFrom();
        String to = request.getTo();
        String id = request.getId();
        Boolean verbose = request.getVerbose();
        try (final ThreadContext.StoredContext storedContext = threadPool.getThreadContext().stashContext()) {
            queryInsightsService.getTopQueriesService(request.getMetricType())
                .getTopQueriesRecordsFromIndex(from, to, id, verbose, new ActionListener<List<SearchQueryRecord>>() {
                    @Override
                    public void onResponse(List<SearchQueryRecord> historicalRecords) {
                        onHistoricalDataResponse(request, inMemoryTopQueries, inMemoryDataFailures, historicalRecords, finalListener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onHistoricalDataFailure(request, inMemoryTopQueries, inMemoryDataFailures, e, finalListener);
                    }
                });
        } catch (Exception e) {
            logger.error("Synchronous failure while initiating historical top queries fetch", e);
            finalListener.onFailure(e);
        }
    }

    void onHistoricalDataResponse(
        TopQueriesRequest request,
        List<TopQueries> inMemoryTopQueries,
        List<FailedNodeException> inMemoryDataFailures,
        List<SearchQueryRecord> historicalRecords,
        ActionListener<TopQueriesResponse> finalListener
    ) {
        List<TopQueries> combinedTopQueriesList = new ArrayList<>(inMemoryTopQueries);
        if (historicalRecords != null && !historicalRecords.isEmpty()) {
            // Remove duplicates between in-memory and historical records
            List<SearchQueryRecord> deduplicatedHistoricalRecords = removeDuplicates(inMemoryTopQueries, historicalRecords);
            if (!deduplicatedHistoricalRecords.isEmpty()) {
                combinedTopQueriesList.add(new TopQueries(clusterService.localNode(), deduplicatedHistoricalRecords));
            }
        }
        finalListener.onResponse(
            new TopQueriesResponse(clusterService.getClusterName(), combinedTopQueriesList, inMemoryDataFailures, request.getMetricType())
        );
    }

    void onHistoricalDataFailure(
        TopQueriesRequest request,
        List<TopQueries> inMemoryTopQueries,
        List<FailedNodeException> inMemoryDataFailures,
        Exception e,
        ActionListener<TopQueriesResponse> finalListener
    ) {
        logger.warn("Failed to fetch historical top queries, proceeding with in-memory data only.", e);
        finalListener.onResponse(
            new TopQueriesResponse(clusterService.getClusterName(), inMemoryTopQueries, inMemoryDataFailures, request.getMetricType())
        );
    }

    /**
     * Remove duplicate records from historical data that already exist in in-memory data.
     * Uses a Set to maintain unique record IDs for efficient lookup and comparison.
     *
     * @param inMemoryTopQueries List of TopQueries containing in-memory records
     * @param historicalRecords List of historical SearchQueryRecord objects
     * @return List of deduplicated historical records
     */
    private List<SearchQueryRecord> removeDuplicates(List<TopQueries> inMemoryTopQueries, List<SearchQueryRecord> historicalRecords) {
        // Collect all in-memory record IDs into a set for efficient lookup
        Set<String> inMemoryRecordIds = new LinkedHashSet<>();
        for (TopQueries topQueries : inMemoryTopQueries) {
            if (topQueries.getTopQueriesRecord() != null) {
                for (SearchQueryRecord record : topQueries.getTopQueriesRecord()) {
                    inMemoryRecordIds.add(record.getId());
                }
            }
        }

        // Filter out historical records that have IDs already present in in-memory data
        List<SearchQueryRecord> deduplicatedRecords = new ArrayList<>();
        for (SearchQueryRecord historicalRecord : historicalRecords) {
            if (!inMemoryRecordIds.contains(historicalRecord.getId())) {
                deduplicatedRecords.add(historicalRecord);
            }
        }

        return deduplicatedRecords;
    }

    @Override
    protected void doExecute(Task task, TopQueriesRequest request, ActionListener<TopQueriesResponse> finalListener) {
        // Capture filter mode and user info before super.doExecute() which may stash context
        FilterByMode filterByMode = queryInsightsService.getFilterByMode();
        UserPrincipalInfo userInfo = null;

        if (filterByMode != FilterByMode.NONE) {
            try {
                userInfo = new UserPrincipalContext(threadPool).extractUserInfo();
            } catch (Exception e) {
                log.warn("Failed to extract user info for RBAC filtering", e);
            }
            if (userInfo == null) {
                log.warn("User info unavailable with filter_by_mode [{}], falling back to no filtering", filterByMode);
                filterByMode = FilterByMode.NONE;
            }
        }

        ActionListener<TopQueriesResponse> rbacListener = wrapWithRbacFilter(finalListener, filterByMode, userInfo);
        super.doExecute(task, request, createInMemoryDataCollectionListener(request, rbacListener));
    }

    /**
     * Wraps the final listener with an RBAC filtering step that filters records in each node's response.
     */
    ActionListener<TopQueriesResponse> wrapWithRbacFilter(
        ActionListener<TopQueriesResponse> delegate,
        FilterByMode filterByMode,
        UserPrincipalInfo userInfo
    ) {
        if (filterByMode == FilterByMode.NONE) {
            return delegate;
        }
        final FilterByMode mode = filterByMode;
        final UserPrincipalInfo info = userInfo;
        return new ActionListener<TopQueriesResponse>() {
            @Override
            public void onResponse(TopQueriesResponse response) {
                try {
                    List<TopQueries> filteredNodes = response.getNodes().stream().map(topQueries -> {
                        List<SearchQueryRecord> filtered = TopQueriesRbacFilter.filterRecords(topQueries.getTopQueriesRecord(), mode, info);
                        return new TopQueries(topQueries.getNode(), filtered);
                    }).collect(Collectors.toList());
                    delegate.onResponse(
                        new TopQueriesResponse(response.getClusterName(), filteredNodes, response.failures(), response.getMetricType())
                    );
                } catch (Exception e) {
                    delegate.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
    }

    @Override
    protected TopQueriesResponse newResponse(
        final TopQueriesRequest topQueriesRequest,
        final List<TopQueries> collectedNodeResponses,
        final List<FailedNodeException> failures
    ) {
        return new TopQueriesResponse(clusterService.getClusterName(), collectedNodeResponses, failures, topQueriesRequest.getMetricType());
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
                .getTopQueriesRecords(true, request.getFrom(), request.getTo(), request.getId(), request.getVerbose())
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
