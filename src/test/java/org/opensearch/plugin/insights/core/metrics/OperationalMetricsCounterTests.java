/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.metrics;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.ArgumentCaptor;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the {@link OperationalMetricsCounter} class.
 */
public class OperationalMetricsCounterTests extends OpenSearchTestCase {
    private static final String CLUSTER_NAME = "test-cluster";

    public void testSingletonInitializationAndIncrement() {
        Counter mockCounter = mock(Counter.class);
        MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        // Stub the createCounter method to return the mockCounter
        when(metricsRegistry.createCounter(any(), any(), any())).thenReturn(mockCounter);
        OperationalMetricsCounter.initialize(CLUSTER_NAME, metricsRegistry);
        OperationalMetricsCounter instance = OperationalMetricsCounter.getInstance();
        ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
        verify(metricsRegistry, times(8)).createCounter(nameCaptor.capture(), any(), eq("1"));
        assertNotNull(instance);
        instance.incrementCounter(OperationalMetric.LOCAL_INDEX_READER_PARSING_EXCEPTIONS);
        instance.incrementCounter(OperationalMetric.LOCAL_INDEX_READER_PARSING_EXCEPTIONS);
        instance.incrementCounter(OperationalMetric.LOCAL_INDEX_READER_PARSING_EXCEPTIONS);
        verify(mockCounter, times(3)).add(eq(1.0), any(Tags.class));
    }
}
