/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.listener;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Listener specifically for finished queries cache
 */
public class FinishedQueriesListener extends SearchRequestOperationsListener {

    private final QueryInsightsService queryInsightsService;
    private final ClusterService clusterService;

    public FinishedQueriesListener(ClusterService clusterService, QueryInsightsService queryInsightsService) {
        this.clusterService = clusterService;
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    public void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        addFinishedQuery(context, searchRequestContext);
    }

    @Override
    public void onRequestFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        addFinishedQuery(context, searchRequestContext);
    }

    private void addFinishedQuery(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        SearchTask searchTask = context.getTask();
        String taskId = clusterService.localNode().getId() + ":" + searchTask.getId();

        // Create minimal record for finished queries
        SearchQueryRecord record = createMinimalRecord(context, searchRequestContext, taskId);
        if (record != null) {
            queryInsightsService.getFinishedQueriesCache().addFinishedQuery(record);
        }
    }

    private SearchQueryRecord createMinimalRecord(SearchPhaseContext context, SearchRequestContext searchRequestContext, String taskId) {
        // Minimal implementation - you can expand this as needed
        return null; // TODO: implement minimal record creation
    }
}
