/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import java.util.List;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.core.service.LiveQueriesCache;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesStreamAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesStreamRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesStreamResponse;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action for streaming live queries
 */
public class TransportLiveQueriesStreamAction extends HandledTransportAction<LiveQueriesStreamRequest, LiveQueriesStreamResponse> {

    private final LiveQueriesCache liveQueriesCache;

    @Inject
    public TransportLiveQueriesStreamAction(
        TransportService transportService,
        QueryInsightsService queryInsightsService,
        ActionFilters actionFilters
    ) {
        super(LiveQueriesStreamAction.NAME, transportService, actionFilters, LiveQueriesStreamRequest::new, ThreadPool.Names.GENERIC);
        this.liveQueriesCache = queryInsightsService.getLiveQueriesCache();
    }

    @Override
    protected void doExecute(Task task, LiveQueriesStreamRequest request, ActionListener<LiveQueriesStreamResponse> listener) {
        try {
            List<SearchQueryRecord> queries = liveQueriesCache.getCurrentQueries();
            LiveQueriesStreamResponse response = new LiveQueriesStreamResponse(queries);
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
