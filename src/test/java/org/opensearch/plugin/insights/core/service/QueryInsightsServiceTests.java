/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import org.junit.Before;
import org.opensearch.client.Client;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Unit Tests for {@link QueryInsightsService}.
 */
public class QueryInsightsServiceTests extends OpenSearchTestCase {
    private final ThreadPool threadPool = mock(ThreadPool.class);
    private final Client client = mock(Client.class);
    private QueryInsightsService queryInsightsService;
    private QueryInsightsService queryInsightsServiceSpy;

    @Before
    public void setup() {
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        queryInsightsService = new QueryInsightsService(clusterSettings, threadPool, client, NoopMetricsRegistry.INSTANCE);
        queryInsightsService.enableCollection(MetricType.LATENCY, true);
        queryInsightsService.enableCollection(MetricType.CPU, true);
        queryInsightsService.enableCollection(MetricType.MEMORY, true);
        queryInsightsServiceSpy = spy(queryInsightsService);
    }

    public void testAddRecordToLimitAndDrain() {
        SearchQueryRecord record = QueryInsightsTestUtils.generateQueryInsightRecords(1, 1, System.currentTimeMillis(), 0).get(0);
        for (int i = 0; i < QueryInsightsSettings.QUERY_RECORD_QUEUE_CAPACITY; i++) {
            assertTrue(queryInsightsService.addRecord(record));
        }
        // exceed capacity
        assertFalse(queryInsightsService.addRecord(record));
        queryInsightsService.drainRecords();
        assertEquals(
            QueryInsightsSettings.DEFAULT_TOP_N_SIZE,
            queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false).size()
        );
    }

    public void testClose() {
        try {
            queryInsightsService.doClose();
        } catch (Exception e) {
            fail("No exception expected when closing query insights service");
        }
    }

    public void testSearchQueryMetricsEnabled() {
        // Initially, searchQueryMetricsEnabled should be false and searchQueryCategorizer should be null
        assertFalse(queryInsightsService.isSearchQueryMetricsFeatureEnabled());
        assertNotNull(queryInsightsService.getSearchQueryCategorizer());

        // Enable search query metrics
        queryInsightsService.setSearchQueryMetricsEnabled(true);

        // Assert that searchQueryMetricsEnabled is true and searchQueryCategorizer is initialized
        assertTrue(queryInsightsService.isSearchQueryMetricsFeatureEnabled());
        assertNotNull(queryInsightsService.getSearchQueryCategorizer());

        // Disable search query metrics
        queryInsightsService.setSearchQueryMetricsEnabled(false);

        // Assert that searchQueryMetricsEnabled is false and searchQueryCategorizer is not null
        assertFalse(queryInsightsService.isSearchQueryMetricsFeatureEnabled());
        assertNotNull(queryInsightsService.getSearchQueryCategorizer());

    }

    public void testFeaturesEnableDisable() {
        // Test case 1: All metric type collection disabled and search query metrics disabled, enable search query metrics
        queryInsightsServiceSpy.enableCollection(MetricType.LATENCY, false);
        queryInsightsServiceSpy.enableCollection(MetricType.CPU, false);
        queryInsightsServiceSpy.enableCollection(MetricType.MEMORY, false);
        queryInsightsServiceSpy.setSearchQueryMetricsEnabled(false);

        queryInsightsServiceSpy.setSearchQueryMetricsEnabled(true);
        verify(queryInsightsServiceSpy).checkAndRestartQueryInsights();

        // Test case 2: All metric type collection disabled and search query metrics enabled, disable search query metrics
        queryInsightsServiceSpy.enableCollection(MetricType.LATENCY, false);
        queryInsightsServiceSpy.enableCollection(MetricType.CPU, false);
        queryInsightsServiceSpy.enableCollection(MetricType.MEMORY, false);
        queryInsightsServiceSpy.setSearchQueryMetricsEnabled(true);

        queryInsightsServiceSpy.setSearchQueryMetricsEnabled(false);
        verify(queryInsightsServiceSpy).checkAndStopQueryInsights();
    }
}
