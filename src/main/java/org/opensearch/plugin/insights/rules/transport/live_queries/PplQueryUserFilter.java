/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

/**
 * Captures a PPL query's user when its action starts, under its Live Queries id ({@code nodeId:taskId}),
 * and releases it when the action completes. The search listener only sees searches, so without this
 * a PPL query shows no user between its searches. Other actions pass straight through.
 */
public final class PplQueryUserFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(PplQueryUserFilter.class);

    private volatile ClusterService clusterService;
    private volatile ThreadPool threadPool;
    private volatile QueryInsightsService queryInsightsService;

    /**
     * Called once the plugin's components exist. Until then the filter captures nothing.
     *
     * @param clusterService       for the local node id
     * @param threadPool           for the thread context that holds the user
     * @param queryInsightsService where captured users are kept
     */
    public void initialize(
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final QueryInsightsService queryInsightsService
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.queryInsightsService = queryInsightsService;
    }

    @Override
    public int order() {
        // Run last, so the security plugin has already set the user.
        return Integer.MAX_VALUE;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        final Task task,
        final String action,
        final Request request,
        final ActionRequestMetadata<Request, Response> actionRequestMetadata,
        final ActionListener<Response> listener,
        final ActionFilterChain<Request, Response> chain
    ) {
        final String queryId = TransportLiveQueriesAction.PPL_ACTION.equals(action) ? capture(task) : null;
        if (queryId == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        final QueryInsightsService service = queryInsightsService;
        chain.proceed(task, action, request, ActionListener.runAfter(listener, () -> service.removeLiveQueryUserInfo(queryId)));
    }

    /** Captures the user for {@code task}; returns its live id, or {@code null} if no user was captured. */
    private String capture(final Task task) {
        final QueryInsightsService service = queryInsightsService;
        if (task == null || service == null || clusterService == null || threadPool == null || !service.isAnyFeatureEnabled()) {
            return null;
        }
        try {
            final UserPrincipalContext.UserPrincipalInfo user = new UserPrincipalContext(threadPool).extractUserInfo();
            if (user == null) {
                return null;
            }
            final String queryId = QueryInsightsService.buildLiveQueryTaskKey(clusterService.localNode().getId(), task.getId());
            service.putLiveQueryUserInfo(queryId, user);
            return queryId;
        } catch (Exception e) {
            logger.debug("Failed to capture the user of a PPL query", e);
            return null;
        }
    }
}
