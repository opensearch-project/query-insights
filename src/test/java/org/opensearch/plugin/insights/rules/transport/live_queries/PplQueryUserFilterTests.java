/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * A PPL query's user is captured for Live Queries from the moment its action starts until it ends.
 */
public class PplQueryUserFilterTests extends OpenSearchTestCase {

    private final ThreadPool threadPool = new TestThreadPool("PplQueryUserFilterTests");
    private final QueryInsightsService queryInsightsService = mock(QueryInsightsService.class);
    private final ClusterService clusterService = mock(ClusterService.class);
    private final PplQueryUserFilter filter = new PplQueryUserFilter();
    private final Task task = new Task(42, "transport", TransportLiveQueriesAction.PPL_ACTION, "PPL query", null, Map.of());
    private final ActionRequest request = mock(ActionRequest.class);
    private final AtomicReference<ActionListener<ActionResponse>> sentOn = new AtomicReference<>();

    @SuppressWarnings("unchecked")
    private final ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);

    @SuppressWarnings("unchecked")
    private final ActionListener<ActionResponse> listener = mock(ActionListener.class);

    @Before
    @SuppressWarnings("unchecked")
    public void setUpFilter() {
        when(clusterService.localNode()).thenReturn(
            new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT)
        );
        when(queryInsightsService.isAnyFeatureEnabled()).thenReturn(true);
        doAnswer(inv -> {
            sentOn.set(inv.getArgument(3));
            return null;
        }).when(chain).proceed(any(), anyString(), any(), any());
        filter.initialize(clusterService, threadPool, queryInsightsService);
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testRunsAfterEveryOtherFilter() {
        // The security plugin's filter puts the user in the thread context first.
        assertEquals(Integer.MAX_VALUE, filter.order());
    }

    public void testAPplQueryKeepsItsUserUntilItsActionEnds() {
        threadPool.getThreadContext().putTransient("_opendistro_security_user_info", "alice|backend1|role1|tenant1");

        filter.apply(task, TransportLiveQueriesAction.PPL_ACTION, request, ActionRequestMetadata.empty(), listener, chain);

        ArgumentCaptor<UserPrincipalContext.UserPrincipalInfo> user = ArgumentCaptor.forClass(UserPrincipalContext.UserPrincipalInfo.class);
        verify(queryInsightsService).putLiveQueryUserInfo(eq("node1:42"), user.capture());
        assertEquals("alice", user.getValue().getUserName());
        verify(queryInsightsService, never()).removeLiveQueryUserInfo(anyString());

        // Once the query answers, the user goes, after the caller has the answer.
        ActionResponse response = mock(ActionResponse.class);
        sentOn.get().onResponse(response);
        InOrder order = inOrder(listener, queryInsightsService);
        order.verify(listener).onResponse(response);
        order.verify(queryInsightsService).removeLiveQueryUserInfo("node1:42");
    }

    public void testAFailedPplQueryLetsItsUserGo() {
        threadPool.getThreadContext().putTransient("_opendistro_security_user_info", "alice|backend1|role1|tenant1");

        filter.apply(task, TransportLiveQueriesAction.PPL_ACTION, request, ActionRequestMetadata.empty(), listener, chain);
        Exception failure = new RuntimeException("query failed");
        sentOn.get().onFailure(failure);

        verify(listener).onFailure(failure);
        verify(queryInsightsService).removeLiveQueryUserInfo("node1:42");
    }

    public void testOtherActionsPassThrough() {
        threadPool.getThreadContext().putTransient("_opendistro_security_user_info", "alice|backend1|role1|tenant1");

        filter.apply(task, "indices:data/read/search", request, ActionRequestMetadata.empty(), listener, chain);

        verify(queryInsightsService, never()).putLiveQueryUserInfo(anyString(), any());
        assertSame(listener, sentOn.get());
    }

    public void testNothingIsCapturedWithoutAUserOrWhileQueryInsightsIsOff() {
        // Without the security plugin there is no user.
        filter.apply(task, TransportLiveQueriesAction.PPL_ACTION, request, ActionRequestMetadata.empty(), listener, chain);
        assertSame(listener, sentOn.get());

        threadPool.getThreadContext().putTransient("_opendistro_security_user_info", "alice|backend1|role1|tenant1");
        when(queryInsightsService.isAnyFeatureEnabled()).thenReturn(false);
        filter.apply(task, TransportLiveQueriesAction.PPL_ACTION, request, ActionRequestMetadata.empty(), listener, chain);
        assertSame(listener, sentOn.get());

        verify(queryInsightsService, never()).putLiveQueryUserInfo(anyString(), any());
    }

    public void testAFilterNotYetGivenItsComponentsPassesThrough() {
        threadPool.getThreadContext().putTransient("_opendistro_security_user_info", "alice|backend1|role1|tenant1");

        new PplQueryUserFilter().apply(
            task,
            TransportLiveQueriesAction.PPL_ACTION,
            request,
            ActionRequestMetadata.empty(),
            listener,
            chain
        );

        assertSame(listener, sentOn.get());
        verify(queryInsightsService, never()).putLiveQueryUserInfo(anyString(), any());
    }
}
