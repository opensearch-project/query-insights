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
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Granular tests for the {@link QueryInsightsExporterFactoryTests} class.
 */
public class QueryInsightsExporterFactoryTests extends OpenSearchTestCase {
    private final String format = "YYYY.MM.dd";

    private final Client client = mock(Client.class);
    private QueryInsightsExporterFactory queryInsightsExporterFactory;
    private MetricsRegistry metricsRegistry;

    @Before
    public void setup() {
        queryInsightsExporterFactory = new QueryInsightsExporterFactory(client);
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
        QueryInsightsExporter exporter1 = queryInsightsExporterFactory.createExporter("id", SinkType.LOCAL_INDEX, format);
        assertTrue(exporter1 instanceof LocalIndexExporter);
        QueryInsightsExporter exporter2 = queryInsightsExporterFactory.createExporter("id", SinkType.DEBUG, format);
        assertTrue(exporter2 instanceof DebugExporter);
        QueryInsightsExporter exporter3 = queryInsightsExporterFactory.createExporter("id", SinkType.DEBUG, format);
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
        LocalIndexExporter exporter = new LocalIndexExporter(client, DateTimeFormatter.ofPattern(format, Locale.ROOT), "id");
        queryInsightsExporterFactory.updateExporter(exporter, "yyyy-MM-dd-HH");
        assertEquals(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH", Locale.ROOT).toString(), exporter.getIndexPattern().toString());
    }

}
