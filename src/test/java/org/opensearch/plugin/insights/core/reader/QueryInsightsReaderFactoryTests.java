/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.reader;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_TOP_N_QUERIES_INDEX_PATTERN;

import org.joda.time.format.DateTimeFormat;
import org.junit.Before;
import org.opensearch.client.Client;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Granular tests for the {@link QueryInsightsReaderFactoryTests} class.
 */
public class QueryInsightsReaderFactoryTests extends OpenSearchTestCase {
    private final String format = DEFAULT_TOP_N_QUERIES_INDEX_PATTERN;

    private final Client client = mock(Client.class);
    private final NamedXContentRegistry namedXContentRegistry = mock(NamedXContentRegistry.class);
    private QueryInsightsReaderFactory queryInsightsReaderFactory;
    private MetricsRegistry metricsRegistry;

    @Before
    public void setup() {
        queryInsightsReaderFactory = new QueryInsightsReaderFactory(client);
        metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );
        OperationalMetricsCounter.initialize("cluster", metricsRegistry);
    }

    public void testCreateAndCloseReader() {
        QueryInsightsReader reader1 = queryInsightsReaderFactory.createReader(format, namedXContentRegistry);
        assertTrue(reader1 instanceof LocalIndexReader);
        try {
            queryInsightsReaderFactory.closeReader(reader1);
            queryInsightsReaderFactory.closeAllReaders();
        } catch (Exception e) {
            fail("No exception should be thrown when closing reader");
        }
    }

    public void testUpdateReader() {
        LocalIndexReader reader = new LocalIndexReader(client, DateTimeFormat.forPattern(format), namedXContentRegistry);
        queryInsightsReaderFactory.updateReader(reader, "yyyy-MM-dd-HH");
        assertEquals(DateTimeFormat.forPattern("yyyy-MM-dd-HH"), reader.getIndexPattern());
    }

}
