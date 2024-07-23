/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.listener;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchTask;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.core.service.TopQueriesService;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit Tests for {@link QueryInsightsListener}.
 */
public class QueryInsightsListenerTests extends OpenSearchTestCase {
    private final SearchRequestContext searchRequestContext = mock(SearchRequestContext.class);
    private final SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
    private final SearchRequest searchRequest = mock(SearchRequest.class);
    private final QueryInsightsService queryInsightsService = mock(QueryInsightsService.class);
    private final TopQueriesService topQueriesService = mock(TopQueriesService.class);
    private final ThreadPool threadPool = new TestThreadPool("QueryInsightsThreadPool");
    private ClusterService clusterService;

    @Before
    public void setup() {
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        ClusterState state = ClusterStateCreationUtils.stateWithActivePrimary("test", true, 1 + randomInt(3), randomInt(2));
        clusterService = ClusterServiceUtils.createClusterService(threadPool, state.getNodes().getLocalNode(), clusterSettings);
        ClusterServiceUtils.setState(clusterService, state);
        when(queryInsightsService.isCollectionEnabled(MetricType.LATENCY)).thenReturn(true);
        when(queryInsightsService.getTopQueriesService(MetricType.LATENCY)).thenReturn(topQueriesService);

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadPool.getThreadContext().setHeaders(new Tuple<>(Collections.singletonMap(Task.X_OPAQUE_ID, "userLabel"), new HashMap<>()));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(clusterService);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testOnRequestEnd() throws InterruptedException {
        Long timestamp = System.currentTimeMillis() - 100L;
        SearchType searchType = SearchType.QUERY_THEN_FETCH;

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(new TermsAggregationBuilder("agg1").userValueTypeHint(ValueType.STRING).field("type.keyword"));
        searchSourceBuilder.size(0);
        SearchTask task = new SearchTask(
            0,
            "n/a",
            "n/a",
            () -> "test",
            TaskId.EMPTY_TASK_ID,
            Collections.singletonMap(Task.X_OPAQUE_ID, "userLabel")
        );

        String[] indices = new String[] { "index-1", "index-2" };

        Map<String, Long> phaseLatencyMap = new HashMap<>();
        phaseLatencyMap.put("expand", 0L);
        phaseLatencyMap.put("query", 20L);
        phaseLatencyMap.put("fetch", 1L);

        int numberOfShards = 10;

        QueryInsightsListener queryInsightsListener = new QueryInsightsListener(clusterService, queryInsightsService);

        when(searchRequest.getOrCreateAbsoluteStartMillis()).thenReturn(timestamp);
        when(searchRequest.searchType()).thenReturn(searchType);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        when(searchRequest.indices()).thenReturn(indices);
        when(searchRequestContext.phaseTookMap()).thenReturn(phaseLatencyMap);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchPhaseContext.getNumShards()).thenReturn(numberOfShards);
        when(searchPhaseContext.getTask()).thenReturn(task);
        ArgumentCaptor<SearchQueryRecord> captor = ArgumentCaptor.forClass(SearchQueryRecord.class);

        queryInsightsListener.onRequestEnd(searchPhaseContext, searchRequestContext);

        verify(queryInsightsService, times(1)).addRecord(captor.capture());
        SearchQueryRecord generatedRecord = captor.getValue();
        assertEquals(timestamp.longValue(), generatedRecord.getTimestamp());
        assertEquals(numberOfShards, generatedRecord.getAttributes().get(Attribute.TOTAL_SHARDS));
        assertEquals(searchType.toString().toLowerCase(Locale.ROOT), generatedRecord.getAttributes().get(Attribute.SEARCH_TYPE));
        assertEquals(searchSourceBuilder, generatedRecord.getAttributes().get(Attribute.SOURCE));
        Map<String, String> labels = (Map<String, String>) generatedRecord.getAttributes().get(Attribute.LABELS);
        assertEquals("userLabel", labels.get(Task.X_OPAQUE_ID));
    }

    public void testConcurrentOnRequestEnd() throws InterruptedException {
        Long timestamp = System.currentTimeMillis() - 100L;
        SearchType searchType = SearchType.QUERY_THEN_FETCH;

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(new TermsAggregationBuilder("agg1").userValueTypeHint(ValueType.STRING).field("type.keyword"));
        searchSourceBuilder.size(0);
        SearchTask task = new SearchTask(
            0,
            "n/a",
            "n/a",
            () -> "test",
            TaskId.EMPTY_TASK_ID,
            Collections.singletonMap(Task.X_OPAQUE_ID, "userLabel")
        );

        String[] indices = new String[] { "index-1", "index-2" };

        Map<String, Long> phaseLatencyMap = new HashMap<>();
        phaseLatencyMap.put("expand", 0L);
        phaseLatencyMap.put("query", 20L);
        phaseLatencyMap.put("fetch", 1L);

        int numberOfShards = 10;

        final List<QueryInsightsListener> searchListenersList = new ArrayList<>();

        when(searchRequest.getOrCreateAbsoluteStartMillis()).thenReturn(timestamp);
        when(searchRequest.searchType()).thenReturn(searchType);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        when(searchRequest.indices()).thenReturn(indices);
        when(searchRequestContext.phaseTookMap()).thenReturn(phaseLatencyMap);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchPhaseContext.getNumShards()).thenReturn(numberOfShards);
        when(searchPhaseContext.getTask()).thenReturn(task);

        int numRequests = 50;
        Thread[] threads = new Thread[numRequests];
        Phaser phaser = new Phaser(numRequests + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numRequests);

        for (int i = 0; i < numRequests; i++) {
            searchListenersList.add(new QueryInsightsListener(clusterService, queryInsightsService));
        }

        for (int i = 0; i < numRequests; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                QueryInsightsListener thisListener = searchListenersList.get(finalI);
                thisListener.onRequestEnd(searchPhaseContext, searchRequestContext);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        verify(queryInsightsService, times(numRequests)).addRecord(any());
    }

    public void testTopNFeatureEnabledDisabled() {
        // Test case 1: Only latency enabled initially, disable latency and verify expected behavior
        QueryInsightsService queryInsightsService1 = mock(QueryInsightsService.class);
        QueryInsightsListener queryInsightsListener1 = new QueryInsightsListener(clusterService, queryInsightsService1);

        when(queryInsightsService1.isCollectionEnabled(MetricType.LATENCY)).thenReturn(true);
        when(queryInsightsService1.isCollectionEnabled(MetricType.CPU)).thenReturn(false);
        when(queryInsightsService1.isCollectionEnabled(MetricType.MEMORY)).thenReturn(false);
        when(queryInsightsService1.isTopNFeatureEnabled()).thenReturn(true).thenReturn(false);

        queryInsightsListener1.setEnableTopQueries(MetricType.LATENCY, false);
        assertFalse(queryInsightsListener1.isEnabled());
        verify(queryInsightsService1).checkAndStopQueryInsights();
        verify(queryInsightsService1, never()).checkAndRestartQueryInsights();

        // Test case 2: All disabled initially, enable latency and verify expected behavior
        QueryInsightsService queryInsightsService2 = mock(QueryInsightsService.class);
        QueryInsightsListener queryInsightsListener2 = new QueryInsightsListener(clusterService, queryInsightsService2);

        when(queryInsightsService2.isCollectionEnabled(MetricType.LATENCY)).thenReturn(false);
        when(queryInsightsService2.isCollectionEnabled(MetricType.CPU)).thenReturn(false);
        when(queryInsightsService2.isCollectionEnabled(MetricType.MEMORY)).thenReturn(false);
        when(queryInsightsService2.isTopNFeatureEnabled()).thenReturn(false).thenReturn(true);

        queryInsightsListener2.setEnableTopQueries(MetricType.LATENCY, true);
        assertTrue(queryInsightsListener2.isEnabled());
        verify(queryInsightsService2).checkAndRestartQueryInsights();
        verify(queryInsightsService2, never()).checkAndStopQueryInsights();

        // Test case 3: latency and CPU enabled initially, disable latency and verify expected behavior
        QueryInsightsService queryInsightsService3 = mock(QueryInsightsService.class);
        QueryInsightsListener queryInsightsListener3 = new QueryInsightsListener(clusterService, queryInsightsService3);

        when(queryInsightsService3.isCollectionEnabled(MetricType.LATENCY)).thenReturn(true);
        when(queryInsightsService3.isCollectionEnabled(MetricType.CPU)).thenReturn(true);
        when(queryInsightsService3.isCollectionEnabled(MetricType.MEMORY)).thenReturn(false);
        when(queryInsightsService3.isTopNFeatureEnabled()).thenReturn(true).thenReturn(true);

        queryInsightsListener3.setEnableTopQueries(MetricType.LATENCY, false);
        assertTrue(queryInsightsListener3.isEnabled());
        verify(queryInsightsService3, never()).checkAndStopQueryInsights();
        verify(queryInsightsService3, never()).checkAndRestartQueryInsights();

        // Test case 4: Only CPU enabled initially, enable latency and verify expected behavior
        QueryInsightsService queryInsightsService4 = mock(QueryInsightsService.class);
        QueryInsightsListener queryInsightsListener4 = new QueryInsightsListener(clusterService, queryInsightsService4);

        when(queryInsightsService4.isCollectionEnabled(MetricType.LATENCY)).thenReturn(false);
        when(queryInsightsService4.isCollectionEnabled(MetricType.CPU)).thenReturn(true);
        when(queryInsightsService4.isCollectionEnabled(MetricType.MEMORY)).thenReturn(false);
        when(queryInsightsService4.isTopNFeatureEnabled()).thenReturn(true).thenReturn(true);

        queryInsightsListener4.setEnableTopQueries(MetricType.LATENCY, true);
        assertTrue(queryInsightsListener4.isEnabled());
        verify(queryInsightsService4, never()).checkAndRestartQueryInsights();
        verify(queryInsightsService4, never()).checkAndStopQueryInsights();
    }
}
