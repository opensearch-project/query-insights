/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.listener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Listener for capturing finished queries for live queries functionality
 */
public class FinishedQueriesListener extends SearchRequestOperationsListener {

    private final QueryInsightsService queryInsightsService;

    public FinishedQueriesListener(QueryInsightsService queryInsightsService) {
        super(false); // Initially disabled
        this.queryInsightsService = queryInsightsService;
    }

    public void enableListener(boolean enabled) {
        super.setEnabled(enabled);
    }

    @Override
    public void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        captureFinishedQuery(context, searchRequestContext);
    }

    @Override
    public void onRequestFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        captureFinishedQuery(context, searchRequestContext);
    }

    private void captureFinishedQuery(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        List<TaskResourceInfo> tasksResourceUsages = searchRequestContext.getPhaseResourceUsage();

        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(
            MetricType.LATENCY,
            new Measurement(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchRequestContext.getAbsoluteStartNanos()))
        );

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.TASK_RESOURCE_USAGES, tasksResourceUsages);

        SearchQueryRecord record = new SearchQueryRecord(context.getRequest().getOrCreateAbsoluteStartMillis(), measurements, attributes);

        queryInsightsService.getFinishedQueriesCache().addFinishedQuery(record);
    }
}
