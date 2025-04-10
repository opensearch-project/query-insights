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

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueries;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.LiveQueryInfo;
import org.opensearch.tasks.Task;
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
        return new LiveQueriesResponse(clusterService.getClusterName(), responses, failures, liveQueriesRequest.isDetailed());
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
        final boolean detailed = request.isDetailed();
        
        // Use the ListTasksRequest to get all tasks from this node
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setDetailed(detailed);
        listTasksRequest.setActions("indices:data/read/search*"); // Filter for search actions
        
        List<LiveQueryInfo> liveQueries = new ArrayList<>();
        
        // Execute the task list request synchronously
        try {
            ListTasksResponse taskResponse = client.admin().cluster().listTasks(listTasksRequest).actionGet();
            
            // Filter for running search tasks and convert to our model
            for (TaskInfo taskInfo : taskResponse.getTasks()) {
                if (taskInfo.getAction().startsWith("indices:data/read/search")) {
                    Map<String, Object> queryDetails = detailed ? convertTaskDetailsToMap(taskInfo) : new HashMap<>();
                    Map<String, Object> headers = new HashMap<>(taskInfo.getHeaders());
                    
                    liveQueries.add(
                        new LiveQueryInfo(
                            taskInfo.getTaskId().toString(),
                            taskInfo.getAction(),
                            taskInfo.getDescription(),
                            taskInfo.getStartTime(),
                            taskInfo.getRunningTimeNanos(),
                            headers,
                            queryDetails
                        )
                    );
                }
            }
        } catch (Exception e) {
            // Log the error but return an empty list instead of failing
            logger.error("Failed to retrieve live queries", e);
        }
        
        return new LiveQueries(clusterService.localNode(), liveQueries);
    }

    /**
     * Convert task details to a map structure for serialization.
     * 
     * @param taskInfo The task info
     * @return A map with task details
     */
    private Map<String, Object> convertTaskDetailsToMap(TaskInfo taskInfo) {
        Map<String, Object> detailsMap = new HashMap<>();
        
        if (taskInfo.getStatus() != null) {
            detailsMap.put("status", taskInfo.getStatus());
        }
        
        detailsMap.put("type", taskInfo.getType());
        detailsMap.put("start_time_millis", taskInfo.getStartTime());
        detailsMap.put("running_time_nanos", taskInfo.getRunningTimeNanos());
        detailsMap.put("parent_task_id", taskInfo.getParentTaskId().toString());
        detailsMap.put("cancellable", taskInfo.isCancellable());
        
        return detailsMap;
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