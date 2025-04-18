/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueries;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Transport action for fetching ongoing live queries
 */
public class TransportLiveQueriesAction extends HandledTransportAction<LiveQueriesRequest, LiveQueriesResponse> {

    private static final Logger logger = LogManager.getLogger(TransportLiveQueriesAction.class);
    private static final String TOTAL = "total";

    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportLiveQueriesAction(
        final ThreadPool threadPool,
        final ClusterService clusterService,
        final TransportService transportService,
        final Client client,
        final ActionFilters actionFilters
    ) {
        super(LiveQueriesAction.NAME, transportService, actionFilters, LiveQueriesRequest::new, ThreadPool.Names.GENERIC);
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(final Task task, final LiveQueriesRequest request, final ActionListener<LiveQueriesResponse> listener) {
        // Prepare list-tasks request
        ListTasksRequest listTasksRequest = new ListTasksRequest().setDetailed(request.isVerbose()).setActions("indices:data/read/search*");
        if (request.timeout() != null) {
            listTasksRequest.setTimeout(request.timeout());
        }
        String[] nodesIds = request.nodesIds();
        if (nodesIds != null && nodesIds.length > 0) {
            listTasksRequest.setNodes(nodesIds);
        }

        // Execute tasks request asynchronously to avoid blocking transport threads
        client.admin().cluster().listTasks(listTasksRequest, new ActionListener<ListTasksResponse>() {
            @Override
            public void onResponse(ListTasksResponse taskResponse) {
                try {
                    // Group tasks by node and build records
                    Map<String, List<SearchQueryRecord>> perNode = new HashMap<>();
                    for (TaskInfo taskInfo : taskResponse.getTasks()) {
                        if (!taskInfo.getAction().startsWith("indices:data/read/search")) {
                            continue;
                        }
                        long timestamp = taskInfo.getStartTime();
                        String nodeId = taskInfo.getTaskId().getNodeId();
                        long runningNanos = taskInfo.getRunningTimeNanos();

                        Map<MetricType, Measurement> measurements = new HashMap<>();
                        measurements.put(MetricType.LATENCY, new Measurement(runningNanos));
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
                        if (request.isVerbose()) {
                            attributes.put(Attribute.DESCRIPTION, taskInfo.getDescription());
                        }

                        SearchQueryRecord record = new SearchQueryRecord(
                            timestamp,
                            measurements,
                            attributes,
                            taskInfo.getTaskId().toString()
                        );
                        perNode.computeIfAbsent(nodeId, id -> new ArrayList<>()).add(record);
                    }

                    // Build node-level responses
                    List<LiveQueries> nodeResponses = new ArrayList<>();
                    for (Map.Entry<String, List<SearchQueryRecord>> entry : perNode.entrySet()) {
                        DiscoveryNode node = clusterService.state().nodes().get(entry.getKey());
                        nodeResponses.add(new LiveQueries(node, entry.getValue()));
                    }

                    LiveQueriesResponse response = new LiveQueriesResponse(
                        clusterService.getClusterName(),
                        nodeResponses,
                        Collections.emptyList(),
                        request.isVerbose()
                    );
                    listener.onResponse(response);
                } catch (Exception ex) {
                    logger.error("Failed to process live queries response", ex);
                    listener.onFailure(ex);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to retrieve live queries", e);
                listener.onFailure(e);
            }
        });
    }
}
