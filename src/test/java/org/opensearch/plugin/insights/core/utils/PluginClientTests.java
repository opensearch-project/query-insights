/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.utils;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.identity.Subject;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * Granular tests for the {@link PluginClient} class.
 */
public class PluginClientTests extends OpenSearchTestCase {

    private static final ActionType<SearchResponse> TEST_ACTION = new ActionType<>("indices:data/read/test", SearchResponse::new);
    private static final String CALLER_HEADER = "caller";

    private final ThreadPool threadPool = new TestThreadPool("PluginClientThreadPool");
    private Client delegate;
    private PluginClient pluginClient;

    @Before
    public void setup() {
        delegate = mock(Client.class);
        when(delegate.settings()).thenReturn(Settings.EMPTY);
        when(delegate.threadPool()).thenReturn(threadPool);
        pluginClient = new PluginClient(delegate);
    }

    @After
    public void cleanup() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testExecuteWithoutSubjectFails() {
        expectThrows(
            IllegalStateException.class,
            () -> pluginClient.execute(TEST_ACTION, new TestRequest(), ActionListener.wrap(r -> {}, e -> {}))
        );
    }

    public void testActionRunsAsSubjectWithCallerContextIntact() {
        final PassThroughSubject subject = new PassThroughSubject();
        pluginClient.setSubject(subject);

        final AtomicReference<String> headerSeenByDelegate = new AtomicReference<>();
        doAnswer(invocation -> {
            headerSeenByDelegate.set(threadPool.getThreadContext().getHeader(CALLER_HEADER));
            return null;
        }).when(delegate).execute(any(), any(), any());

        threadPool.getThreadContext().putHeader(CALLER_HEADER, "caller_value");
        pluginClient.execute(TEST_ACTION, new TestRequest(), ActionListener.wrap(r -> {}, e -> {}));

        assertTrue("action should have been dispatched through the subject", subject.invoked.get());
        // The caller's context is saved for later restoration rather than cleared, so the only change
        // the delegate sees is whatever the subject itself layers on top.
        assertEquals("caller_value", headerSeenByDelegate.get());
    }

    public void testSynchronousFailureIsReportedToListener() {
        final RuntimeException failure = new RuntimeException("subject switch refused");
        pluginClient.setSubject(new FailingSubject(failure));

        final AtomicReference<Exception> reported = new AtomicReference<>();
        pluginClient.execute(TEST_ACTION, new TestRequest(), ActionListener.wrap(r -> fail("should not succeed"), reported::set));

        assertSame(failure, reported.get());
    }

    private static class TestRequest extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /**
     * Subject that runs the action without altering the thread context, so a test can observe what
     * the client itself did to the context.
     */
    private static class PassThroughSubject implements Subject {

        private final AtomicBoolean invoked = new AtomicBoolean(false);

        @Override
        public Principal getPrincipal() {
            return () -> "plugin:test";
        }

        @Override
        public <E extends Exception> void runAs(final CheckedRunnable<E> r) throws E {
            invoked.set(true);
            r.run();
        }
    }

    /**
     * Subject that refuses to switch, as the security plugin does when the plugin is not authorized.
     */
    private static class FailingSubject implements Subject {

        private final RuntimeException failure;

        FailingSubject(final RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public Principal getPrincipal() {
            return () -> "plugin:test";
        }

        @Override
        public <E extends Exception> void runAs(final CheckedRunnable<E> r) {
            throw failure;
        }
    }
}
