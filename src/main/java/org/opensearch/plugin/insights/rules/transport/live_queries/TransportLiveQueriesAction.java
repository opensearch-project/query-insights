/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.core.service.LiveQueriesCache;
import org.opensearch.plugin.insights.core.service.LiveQueriesHelper;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Transport action for fetching ongoing live queries
 */
public class TransportLiveQueriesAction extends HandledTransportAction<LiveQueriesRequest, LiveQueriesResponse> {

    private static final Logger logger = LogManager.getLogger(TransportLiveQueriesAction.class);
    private static final String TOTAL = "total";

    private final Client client;
    private final TransportService transportService;
    private final QueryInsightsService queryInsightsService;

    @Inject
    public TransportLiveQueriesAction(
        final TransportService transportService,
        final Client client,
        final ActionFilters actionFilters,
        final QueryInsightsService queryInsightsService
    ) {
        super(LiveQueriesAction.NAME, transportService, actionFilters, LiveQueriesRequest::new, ThreadPool.Names.GENERIC);
        this.transportService = transportService;
        this.client = client;
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    protected void doExecute(final Task task, final LiveQueriesRequest request, final ActionListener<LiveQueriesResponse> listener) {
        // If use_live_cache=true, read directly from cache without ListTasks
        if (request.isUseLiveCache()) {
            try {
                LiveQueriesCache liveCache = queryInsightsService.getLiveQueriesCache();
                List<SearchQueryRecord> liveRecords = liveCache.getCurrentQueries();

                // Filter by wlmGroupId and taskId
                List<SearchQueryRecord> filteredRecords = liveRecords.stream().filter(record -> {
                    if (request.getWlmGroupId() != null
                        && !request.getWlmGroupId().equals(record.getAttributes().get(Attribute.WLM_GROUP_ID))) {
                        return false;
                    }
                    if (request.getTaskId() != null && !request.getTaskId().equals(record.getId())) {
                        return false;
                    }
                    return true;
                })
                    .sorted((a, b) -> SearchQueryRecord.compare(b, a, request.getSortBy()))
                    .limit(request.getSize() < 0 ? Long.MAX_VALUE : request.getSize())
                    .toList();

                listener.onResponse(new LiveQueriesResponse(filteredRecords));
            } catch (Exception ex) {
                logger.error("Failed to retrieve live cache queries", ex);
                listener.onFailure(ex);
            }
            return;
        }

        // use_live_cache=false: Fan out to all nodes to get fresh data
        ListTasksRequest listTasksRequest = new ListTasksRequest().setDetailed(request.isVerbose()).setActions("indices:data/read/search*");

        // Set nodes filter if provided in the request
        String[] requestedNodeIds = request.nodesIds();
        if (requestedNodeIds != null && requestedNodeIds.length > 0) {
            listTasksRequest.setNodes(requestedNodeIds);
        } else {
            // Fan out to all nodes when no specific nodes are requested
            listTasksRequest.setNodes(new String[] { "_all" });
        }

        // Execute tasks request asynchronously to avoid blocking
        client.admin().cluster().listTasks(listTasksRequest, new ActionListener<ListTasksResponse>() {
            @Override
            public void onResponse(ListTasksResponse taskResponse) {
                try {
                    Map<String, SearchQueryRecord> recordMap = LiveQueriesHelper.processTaskGroups(
                        taskResponse.getTaskGroups(),
                        transportService,
                        request.isVerbose(),
                        null,
                        request.getWlmGroupId(),
                        request.getTaskId()
                    );

                    List<SearchQueryRecord> allFilteredRecords = new ArrayList<>(recordMap.values());

                    // Sort descending by the requested metric and apply size limit in one pass
                    List<SearchQueryRecord> finalRecords = allFilteredRecords.stream()
                        .sorted((a, b) -> SearchQueryRecord.compare(b, a, request.getSortBy()))
                        .limit(request.getSize() < 0 ? Long.MAX_VALUE : request.getSize())
                        .toList();
                    listener.onResponse(new LiveQueriesResponse(finalRecords));
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
