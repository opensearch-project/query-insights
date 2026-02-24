/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.listener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;
import org.opensearch.plugin.insights.core.service.FinishedQueriesCache;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

/**
 * Listener that captures every finished query into the FinishedQueriesCache.
 * Always enabled — captures individual queries regardless of Top N grouping.
 */
public class FinishedQueriesListener extends SearchRequestOperationsListener {

    private static final Logger log = LogManager.getLogger(FinishedQueriesListener.class);

    private final QueryInsightsService queryInsightsService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public FinishedQueriesListener(
        final ClusterService clusterService,
        final QueryInsightsService queryInsightsService,
        final ThreadPool threadPool
    ) {
        super(true);
        this.clusterService = clusterService;
        this.queryInsightsService = queryInsightsService;
        this.threadPool = threadPool;
    }

    @Override
    public void onPhaseStart(SearchPhaseContext context) {}

    @Override
    public void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}

    @Override
    public void onPhaseFailure(SearchPhaseContext context, Throwable cause) {}

    @Override
    public void onRequestStart(SearchRequestContext searchRequestContext) {}

    @Override
    public void onRequestEnd(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        captureFinishedQuery(context, searchRequestContext, false);
    }

    @Override
    public void onRequestFailure(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        captureFinishedQuery(context, searchRequestContext, true);
    }

    private void captureFinishedQuery(
        final SearchPhaseContext context,
        final SearchRequestContext searchRequestContext,
        final boolean failed
    ) {
        FinishedQueriesCache cache = queryInsightsService.getFinishedQueriesCache();
        if (cache == null) {
            return;
        }

        // Skip top_queries index reads
        SearchRequest request = context.getRequest();
        String[] indices = request.indices();
        if (indices != null && indices.length > 0) {
            boolean allTopN = true;
            for (String idx : indices) {
                if (!idx.contains(QueryInsightsSettings.TOP_QUERIES_INDEX_PREFIX)) {
                    allTopN = false;
                    break;
                }
            }
            if (allTopN) return;
        }

        try {
            List<TaskResourceInfo> tasksResourceUsages = searchRequestContext.getPhaseResourceUsage();
            long cpuNanos = tasksResourceUsages.stream()
                .mapToLong(t -> t.getTaskResourceUsage().getCpuTimeInNanos()).sum();
            long memBytes = tasksResourceUsages.stream()
                .mapToLong(t -> t.getTaskResourceUsage().getMemoryInBytes()).sum();
            long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchRequestContext.getAbsoluteStartNanos());

            Map<MetricType, Measurement> measurements = new HashMap<>();
            measurements.put(MetricType.LATENCY, new Measurement(latencyMs));
            measurements.put(MetricType.CPU, new Measurement(cpuNanos));
            measurements.put(MetricType.MEMORY, new Measurement(memBytes));

            Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.SEARCH_TYPE, request.searchType().toString().toLowerCase(java.util.Locale.ROOT));
            attributes.put(Attribute.TOTAL_SHARDS, context.getNumShards());
            attributes.put(Attribute.INDICES, request.indices());
            attributes.put(Attribute.PHASE_LATENCY_MAP, searchRequestContext.phaseTookMap());
            attributes.put(Attribute.TASK_RESOURCE_USAGES, tasksResourceUsages);
            attributes.put(Attribute.NODE_ID, clusterService.localNode().getId());
            attributes.put(Attribute.FAILED, failed);
            attributes.put(Attribute.IS_CANCELLED, context.getTask().isCancelled());
            if (context.getTask().getWorkloadGroupId() != null) {
                attributes.put(Attribute.WLM_GROUP_ID, context.getTask().getWorkloadGroupId());
            }
            String userProvidedLabel = context.getTask().getHeader(Task.X_OPAQUE_ID);
            if (userProvidedLabel != null) {
                Map<String, Object> labels = new HashMap<>();
                labels.put(Task.X_OPAQUE_ID, userProvidedLabel);
                attributes.put(Attribute.LABELS, labels);
            }

            // liveQueryId = nodeId:taskId (coordinator task)
            String liveQueryId = clusterService.localNode().getId() + ":" + context.getTask().getId();

            UserPrincipalContext userPrincipalContext = threadPool != null ? new UserPrincipalContext(threadPool) : null;

            SearchQueryRecord record = new SearchQueryRecord(
                request.getOrCreateAbsoluteStartMillis(),
                measurements,
                attributes,
                request.source(),
                userPrincipalContext,
                liveQueryId
            );

            // Link to Top N record via thread-local recordId set by QueryInsightsListener
            String topNId = QueryInsightsListener.getRecordId();
            record.setTopNId(topNId);
            record.setQueryStatus(context.getTask().isCancelled() ? "cancelled" : (failed ? "failed" : "completed"));

            cache.addFinishedQuery(record);
        } catch (Exception e) {
            log.error("Failed to capture finished query", e);
        }
    }
}
