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
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.plugin.insights.core.auth.PrincipalExtractor;
import org.opensearch.plugin.insights.core.service.FinishedQueriesCache;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.threadpool.ThreadPool;

public final class FinishedQueriesListener extends SearchRequestOperationsListener {

    private static final Logger log = LogManager.getLogger(FinishedQueriesListener.class);

    private final QueryInsightsService queryInsightsService;
    private final ClusterService clusterService;
    private final PrincipalExtractor principalExtractor;

    public FinishedQueriesListener(
        final ClusterService clusterService,
        final QueryInsightsService queryInsightsService,
        final ThreadPool threadPool
    ) {
        super(false);
        this.clusterService = clusterService;
        this.queryInsightsService = queryInsightsService;
        this.principalExtractor = threadPool != null ? new PrincipalExtractor(threadPool) : null;
    }

    public void enable() {
        super.setEnabled(true);
    }

    public void disable() {
        super.setEnabled(false);
    }

    @Override
    public void onRequestEnd(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        captureFinishedQuery(context, searchRequestContext);
    }

    @Override
    public void onRequestFailure(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        captureFinishedQuery(context, searchRequestContext);
    }

    private void captureFinishedQuery(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        FinishedQueriesCache cache = queryInsightsService.isFinishedQueriesCacheStarted()
            ? queryInsightsService.getFinishedQueriesCache()
            : null;

        if (cache == null) {
            return;
        }

        String recordId = QueryInsightsListener.getRecordId();
        if (recordId == null) {
            recordId = java.util.UUID.randomUUID().toString();
        }

        List<TaskResourceInfo> tasksResourceUsages = searchRequestContext.getPhaseResourceUsage();

        long cpuNanos = 0L;
        long memBytes = 0L;
        long taskId = context.getTask().getId();
        String nodeId = clusterService.localNode().getId();

        for (TaskResourceInfo taskInfo : tasksResourceUsages) {
            cpuNanos += taskInfo.getTaskResourceUsage().getCpuTimeInNanos();
            memBytes += taskInfo.getTaskResourceUsage().getMemoryInBytes();
            if (taskInfo.getParentTaskId() == -1) {
                taskId = taskInfo.getTaskId();
                nodeId = taskInfo.getNodeId();
            }
        }

        long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchRequestContext.getAbsoluteStartNanos());
        String taskIdStr = nodeId + ":" + taskId;
        String description = context.getRequest().source() != null ? context.getRequest().source().toString() : "";
        boolean isCancelled = context.getTask().isCancelled();
        String wlmGroupId = null;
        String username = null;
        String[] userRoles = null;

        if (context.getTask() instanceof org.opensearch.wlm.WorkloadGroupTask workloadTask) {
            wlmGroupId = workloadTask.getWorkloadGroupId();
        }

        if (principalExtractor != null) {
            try {
                PrincipalExtractor.UserPrincipalInfo userInfo = principalExtractor.extractUserInfo();
                if (userInfo != null) {
                    username = userInfo.getUserName();
                    if (userInfo.getRoles() != null && !userInfo.getRoles().isEmpty()) {
                        userRoles = userInfo.getRoles().toArray(new String[0]);
                    }
                }
            } catch (Exception e) {
                // Ignore
            }
        }

        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(latencyMs));
        measurements.put(MetricType.CPU, new Measurement(cpuNanos));
        measurements.put(MetricType.MEMORY, new Measurement(memBytes));

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.TASK_ID, taskIdStr);
        attributes.put(Attribute.NODE_ID, clusterService.localNode().getId());
        attributes.put(Attribute.DESCRIPTION, description);
        attributes.put(Attribute.IS_CANCELLED, isCancelled);
        if (wlmGroupId != null) attributes.put(Attribute.WLM_GROUP_ID, wlmGroupId);
        if (username != null) attributes.put(Attribute.USERNAME, username);
        if (userRoles != null) attributes.put(Attribute.USER_ROLES, userRoles);

        SearchQueryRecord record = new SearchQueryRecord(
            context.getRequest().getOrCreateAbsoluteStartMillis(),
            measurements,
            attributes,
            recordId
        );

        cache.addFinishedQuery(record);
        QueryInsightsListener.removeRecordId();
    }
}
