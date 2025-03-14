/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.format.DateTimeFormatter;
import java.util.Locale;
import org.junit.Before;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

/**
 * Granular tests for the {@link QueryInsightsExporterFactoryTests} class.
 */
public class QueryInsightsExporterFactoryTests extends OpenSearchTestCase {
    private final String format = "YYYY.MM.dd";

    private final Client client = mock(Client.class);
    private QueryInsightsExporterFactory queryInsightsExporterFactory;
    private MetricsRegistry metricsRegistry;
    private ClusterService clusterService;
    private final ThreadPool threadPool = mock(ThreadPool.class);

    @Before
    public void setup() {
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterService = ClusterServiceUtils.createClusterService(settings, clusterSettings, threadPool);
        queryInsightsExporterFactory = new QueryInsightsExporterFactory(client, clusterService);
        metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );
        OperationalMetricsCounter.initialize("cluster", metricsRegistry);
    }

    public void testValidateConfigWhenResetExporter() {
        try {
            // empty settings
            queryInsightsExporterFactory.validateExporterType(null);
        } catch (Exception e) {
            fail("No exception should be thrown when setting is null");
        }
    }

    public void testInvalidExporterTypeConfig() {
        assertThrows(IllegalArgumentException.class, () -> { queryInsightsExporterFactory.validateExporterType("some_invalid_type"); });
    }

    public void testCreateAndCloseExporter() {
        QueryInsightsExporter exporter1 = queryInsightsExporterFactory.createLocalIndexExporter("id-index", format, "");
        assertTrue(exporter1 instanceof LocalIndexExporter);
        QueryInsightsExporter exporter2 = queryInsightsExporterFactory.createDebugExporter("id-debug");
        assertTrue(exporter2 instanceof DebugExporter);
        QueryInsightsExporter exporter3 = queryInsightsExporterFactory.createDebugExporter("id-debug2");
        assertTrue(exporter3 instanceof DebugExporter);
        try {
            queryInsightsExporterFactory.closeExporter(exporter1);
            queryInsightsExporterFactory.closeExporter(exporter2);
            queryInsightsExporterFactory.closeAllExporters();
        } catch (Exception e) {
            fail("No exception should be thrown when closing exporter");
        }
    }

    public void testUpdateExporter() {
        LocalIndexExporter exporter = new LocalIndexExporter(
            client,
            clusterService,
            DateTimeFormatter.ofPattern(format, Locale.ROOT),
            "",
            "id"
        );
        queryInsightsExporterFactory.updateExporter(exporter, "yyyy-MM-dd-HH", 0L);
        assertEquals(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH", Locale.ROOT).toString(), exporter.getIndexPattern().toString());
    }

    public void testCreateLocalIndexExporterWithPriority() {
        QueryInsightsExporterFactory factory = new QueryInsightsExporterFactory(client, clusterService);
        LocalIndexExporter exporter = factory.createLocalIndexExporter("test-id", "YYYY.MM.dd", "mapping.json", 3000L);

        assertEquals(3000L, exporter.getTemplatePriority());
        assertEquals("test-id", exporter.getId());
        assertNotNull(exporter.getIndexPattern());
    }

    public void testUpdateExporterWithPriority() {
        // Create a new factory
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        QueryInsightsExporterFactory factory = new QueryInsightsExporterFactory(mockClient, mockClusterService);

        // Create a new exporter with default priority
        LocalIndexExporter exporter = factory.createLocalIndexExporter("test-id", "YYYY.MM.dd", "mapping.json");

        // Verify default priority
        assertEquals(QueryInsightsSettings.DEFAULT_TEMPLATE_PRIORITY, exporter.getTemplatePriority());

        // Update with new priority
        factory.updateExporter(exporter, "YYYY.MM.dd", 5000L);

        // Verify updated priority
        assertEquals(5000L, exporter.getTemplatePriority());
    }

}
