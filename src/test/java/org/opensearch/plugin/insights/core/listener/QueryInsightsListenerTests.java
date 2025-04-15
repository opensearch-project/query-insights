/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
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
        when(searchRequestContext.getRequest()).thenReturn(searchRequest);
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

    public void testAllFeatureEnabledDisabled() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        QueryInsightsListener queryInsightsListener;

        // Test case 1
        queryInsightsListener = testFeatureEnableDisableSetup(
            Arrays.asList(true, false),  // Latency enabled, then disabled
            Arrays.asList(false),        // CPU disabled
            Arrays.asList(false),        // Memory disabled
            Arrays.asList(false)         // Metrics disabled
        );
        queryInsightsListener.setEnableTopQueries(MetricType.LATENCY, false);
        assertFalse(queryInsightsListener.isEnabled());  // All metrics are disabled, so QI should be disabled

        // Test case 2
        queryInsightsListener = testFeatureEnableDisableSetup(
            Arrays.asList(false, true),  // Latency disabled, then enabled
            Arrays.asList(false),        // CPU disabled
            Arrays.asList(false),        // Memory disabled
            Arrays.asList(false)         // Metrics disabled
        );
        queryInsightsListener.setEnableTopQueries(MetricType.LATENCY, true);
        assertTrue(queryInsightsListener.isEnabled());  // Latency is enabled, so QI should be enabled

        // Test case 3
        queryInsightsListener = testFeatureEnableDisableSetup(
            Arrays.asList(true, false),  // Latency enabled, then disabled
            Arrays.asList(true),         // CPU enabled
            Arrays.asList(false),        // Memory disabled
            Arrays.asList(false)         // Metrics disabled
        );
        queryInsightsListener.setEnableTopQueries(MetricType.LATENCY, false);
        assertTrue(queryInsightsListener.isEnabled());  // CPU is still enabled, so QI should still be enabled

        // Test case 4
        queryInsightsListener = testFeatureEnableDisableSetup(
            Arrays.asList(false, true),  // Latency disabled, then enabled
            Arrays.asList(true),         // CPU enabled
            Arrays.asList(false),        // Memory disabled
            Arrays.asList(false)         // Metrics disabled
        );
        queryInsightsListener.setEnableTopQueries(MetricType.LATENCY, true);
        assertTrue(queryInsightsListener.isEnabled());  // Latency and CPU is enabled, so QI should be enabled

        // Test case 5
        queryInsightsListener = testFeatureEnableDisableSetup(
            Arrays.asList(true, false),  // Latency enabled, then disabled
            Arrays.asList(true, false),  // CPU enabled, then disabled
            Arrays.asList(false),         // Memory disabled
            Arrays.asList(true)          // Metrics enabled
        );
        queryInsightsListener.setEnableTopQueries(MetricType.LATENCY, false);
        queryInsightsListener.setEnableTopQueries(MetricType.CPU, false);
        assertTrue(queryInsightsListener.isEnabled());  // Metrics is still enabled, so QI should still be enabled

        // Test case 6
        queryInsightsListener = testFeatureEnableDisableSetup(
            Arrays.asList(false),        // Latency disabled
            Arrays.asList(false),        // CPU disabled
            Arrays.asList(true, false),  // Memory enabled, then disabled
            Arrays.asList(true)          // Metrics enabled
        );
        queryInsightsListener.setEnableTopQueries(MetricType.MEMORY, false);
        assertTrue(queryInsightsListener.isEnabled());  // Metrics is still enabled, so QI should still be enabled

        // Test case 7
        queryInsightsListener = testFeatureEnableDisableSetup(
            Arrays.asList(true, false),  // Latency enabled, then disabled
            Arrays.asList(true, false),  // CPU enabled, then disabled
            Arrays.asList(true, false),  // Memory enabled, then disabled
            Arrays.asList(true)          // Metrics enabled
        );
        queryInsightsListener.setEnableTopQueries(MetricType.LATENCY, false);
        queryInsightsListener.setEnableTopQueries(MetricType.CPU, false);
        queryInsightsListener.setEnableTopQueries(MetricType.MEMORY, false);
        assertTrue(queryInsightsListener.isEnabled());  // Metrics is still enabled, so QI should still be enabled

        // Test case 8
        queryInsightsListener = testFeatureEnableDisableSetup(
            Arrays.asList(true),  // Latency enabled
            Arrays.asList(false),        // CPU disabled
            Arrays.asList(false),        // Memory disabled
            Arrays.asList(true)         // Metrics enabled then disabled
        );
        queryInsightsListener.setSearchQueryMetricsEnabled(false);
        assertTrue(queryInsightsListener.isEnabled());  // Latency is still enabled, so QI should be enabled

        // Test case 9
        queryInsightsListener = testFeatureEnableDisableSetup(
            Arrays.asList(false),  // Latency disabled
            Arrays.asList(false),        // CPU disabled
            Arrays.asList(false),        // Memory disabled
            Arrays.asList(true, false)   // Metrics enabled then disabled
        );
        queryInsightsListener.setSearchQueryMetricsEnabled(false);
        assertFalse(queryInsightsListener.isEnabled());  // All disabled, so QI should be disabled
    }

    private QueryInsightsListener testFeatureEnableDisableSetup(
        List<Boolean> latencyValues,
        List<Boolean> cpuValues,
        List<Boolean> memoryValues,
        List<Boolean> metricsEnabledValues
    ) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        // Determine initial state: If any of the first values in the lists is true, enable it
        boolean shouldEnable = latencyValues.get(0) || cpuValues.get(0) || memoryValues.get(0) || metricsEnabledValues.get(0);

        QueryInsightsService queryInsightsService = mock(QueryInsightsService.class);
        QueryInsightsListener queryInsightsListener = new QueryInsightsListener(clusterService, queryInsightsService, shouldEnable);

        // Configure the mock to return multiple values in sequence for the various metrics
        when(queryInsightsService.isCollectionEnabled(MetricType.LATENCY)).thenReturn(
            latencyValues.get(0),
            latencyValues.subList(1, latencyValues.size()).toArray(new Boolean[0])
        );

        when(queryInsightsService.isCollectionEnabled(MetricType.CPU)).thenReturn(
            cpuValues.get(0),
            cpuValues.subList(1, cpuValues.size()).toArray(new Boolean[0])
        );

        when(queryInsightsService.isCollectionEnabled(MetricType.MEMORY)).thenReturn(
            memoryValues.get(0),
            memoryValues.subList(1, memoryValues.size()).toArray(new Boolean[0])
        );

        when(queryInsightsService.isSearchQueryMetricsFeatureEnabled()).thenReturn(
            metricsEnabledValues.get(0),
            metricsEnabledValues.subList(1, metricsEnabledValues.size()).toArray(new Boolean[0])
        );

        when(queryInsightsService.isTopNFeatureEnabled()).thenCallRealMethod(); // Call real method if needed

        // Logic for isAnyFeatureEnabled() based on the last values in the lists
        boolean isAnyFeatureEnabled = latencyValues.get(latencyValues.size() - 1)
            || cpuValues.get(cpuValues.size() - 1)
            || memoryValues.get(memoryValues.size() - 1)
            || metricsEnabledValues.get(metricsEnabledValues.size() - 1);

        when(queryInsightsService.isAnyFeatureEnabled()).thenReturn(isAnyFeatureEnabled);

        return queryInsightsListener;
    }

    public void testSkipProfileQuery() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().profile(true);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        QueryInsightsListener queryInsightsListener = new QueryInsightsListener(clusterService, queryInsightsService);
        queryInsightsListener.onRequestEnd(searchPhaseContext, searchRequestContext);
        verify(queryInsightsService, times(0)).addRecord(any());
    }
}
