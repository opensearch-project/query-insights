/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueries;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Transport action for fetching ongoing live queries.
 */
public class TransportLiveQueriesAction extends TransportNodesAction<
    LiveQueriesRequest,
    LiveQueriesResponse,
    TransportLiveQueriesAction.NodeRequest,
    LiveQueries> {

    private static final String TOTAL = "total";
    private static final Logger logger = LogManager.getLogger(TransportLiveQueriesAction.class);
    private final Client client;

    /**
     * Create the TransportLiveQueriesAction Object
     *
     * @param threadPool The OpenSearch thread pool to run async tasks
     * @param clusterService The clusterService of this node
     * @param transportService The TransportService of this node
     * @param client The client to make requests
     * @param actionFilters the action filters
     */
    @Inject
    public TransportLiveQueriesAction(
        final ThreadPool threadPool,
        final ClusterService clusterService,
        final TransportService transportService,
        final Client client,
        final ActionFilters actionFilters
    ) {
        super(
            LiveQueriesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            LiveQueriesRequest::new,
            NodeRequest::new,
            ThreadPool.Names.GENERIC,
            LiveQueries.class
        );
        this.client = client;
    }

    @Override
    protected LiveQueriesResponse newResponse(
        final LiveQueriesRequest liveQueriesRequest,
        final List<LiveQueries> responses,
        final List<FailedNodeException> failures
    ) {
        return new LiveQueriesResponse(clusterService.getClusterName(), responses, failures, liveQueriesRequest.isVerbose());
    }

    @Override
    protected NodeRequest newNodeRequest(final LiveQueriesRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected LiveQueries newNodeResponse(final StreamInput in) throws IOException {
        return new LiveQueries(in);
    }

    @Override
    protected LiveQueries nodeOperation(final NodeRequest nodeRequest) {
        final LiveQueriesRequest request = nodeRequest.request;
        final boolean verbose = request.isVerbose();

        // Use the ListTasksRequest to get all tasks from this node
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setDetailed(verbose);
        listTasksRequest.setActions("indices:data/read/search*"); // Filter for search actions

        List<SearchQueryRecord> liveQueries = new ArrayList<>();

        try {
            ListTasksResponse taskResponse = client.admin().cluster().listTasks(listTasksRequest).actionGet();

            // Filter for running search tasks and convert to SearchQueryRecords
            for (TaskInfo taskInfo : taskResponse.getTasks()) {
                if (taskInfo.getAction().startsWith("indices:data/read/search")) {
                    long timestamp = taskInfo.getStartTime();
                    String nodeId = clusterService.localNode().getId();
                    long runningTimeNanos = taskInfo.getRunningTimeNanos();

                    Map<MetricType, Measurement> measurements = new HashMap<>();
                    measurements.put(MetricType.LATENCY, new Measurement(runningTimeNanos));
                    measurements.put(
                        MetricType.CPU,
                        new Measurement(taskInfo.getResourceStats().getResourceUsageInfo().get(TOTAL).getCpuTimeInNanos())
                    );
                    measurements.put(
                        MetricType.MEMORY,
                        new Measurement(taskInfo.getResourceStats().getResourceUsageInfo().get(TOTAL).getMemoryInBytes())
                    );

                    Map<Attribute, Object> attributes = new HashMap<>();
                    attributes.put(Attribute.NODE_ID, nodeId);

                    if (verbose) {
                        attributes.put(Attribute.DESCRIPTION, taskInfo.getDescription());
                    }

                    liveQueries.add(new SearchQueryRecord(timestamp, measurements, attributes, taskInfo.getTaskId().toString()));
                }
            }
        } catch (Exception e) {
            logger.error("Failed to retrieve live queries", e);
        }

        return new LiveQueries(clusterService.localNode(), liveQueries);
    }

    /**
     * Inner Node Live Queries Request
     */
    public static class NodeRequest extends TransportRequest {

        final LiveQueriesRequest request;

        /**
         * Create the NodeRequest object from StreamInput
         *
         * @param in the StreamInput to read the object
         * @throws IOException IOException
         */
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new LiveQueriesRequest(in);
        }

        /**
         * Create the NodeRequest object from a LiveQueriesRequest
         * @param request the LiveQueriesRequest object
         */
        public NodeRequest(final LiveQueriesRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
