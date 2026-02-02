/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class QueryTypeMetricsTests extends OpenSearchTestCase {

    public void testQueryTypeMetricsInitialization() {
        QueryTypeMetrics metrics = new QueryTypeMetrics("streaming_terms");
        assertEquals("streaming_terms", metrics.getQueryType());
        assertEquals(0, metrics.getCount());
        assertEquals(0, metrics.getLatencyTotal());
        assertEquals(0, metrics.getLatencyMin());
        assertEquals(0, metrics.getLatencyMax());
        assertEquals(0.0, metrics.getLatencyAvg(), 0.001);
    }

    public void testRecordMeasurement() {
        QueryTypeMetrics metrics = new QueryTypeMetrics("streaming_terms");

        metrics.recordMeasurement(100, 1000, 2000);
        assertEquals(1, metrics.getCount());
        assertEquals(100, metrics.getLatencyTotal());
        assertEquals(100, metrics.getLatencyMin());
        assertEquals(100, metrics.getLatencyMax());
        assertEquals(100.0, metrics.getLatencyAvg(), 0.001);
        assertEquals(1000, metrics.getCpuTotal());
        assertEquals(2000, metrics.getMemoryTotal());

        metrics.recordMeasurement(200, 2000, 3000);
        assertEquals(2, metrics.getCount());
        assertEquals(300, metrics.getLatencyTotal());
        assertEquals(100, metrics.getLatencyMin());
        assertEquals(200, metrics.getLatencyMax());
        assertEquals(150.0, metrics.getLatencyAvg(), 0.001);
        assertEquals(3000, metrics.getCpuTotal());
        assertEquals(5000, metrics.getMemoryTotal());
    }

    public void testSerialization() throws IOException {
        QueryTypeMetrics original = new QueryTypeMetrics("streaming_terms");
        original.recordMeasurement(100, 1000, 2000);
        original.recordMeasurement(200, 2000, 3000);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        QueryTypeMetrics deserialized = new QueryTypeMetrics(in);

        assertEquals(original.getQueryType(), deserialized.getQueryType());
        assertEquals(original.getCount(), deserialized.getCount());
        assertEquals(original.getLatencyTotal(), deserialized.getLatencyTotal());
        assertEquals(original.getLatencyMin(), deserialized.getLatencyMin());
        assertEquals(original.getLatencyMax(), deserialized.getLatencyMax());
        assertEquals(original.getCpuTotal(), deserialized.getCpuTotal());
        assertEquals(original.getMemoryTotal(), deserialized.getMemoryTotal());
    }

    public void testEqualsAndHashCode() {
        QueryTypeMetrics metrics1 = new QueryTypeMetrics("streaming_terms");
        QueryTypeMetrics metrics2 = new QueryTypeMetrics("streaming_terms");
        QueryTypeMetrics metrics3 = new QueryTypeMetrics("terms");

        assertEquals(metrics1, metrics2);
        assertEquals(metrics1.hashCode(), metrics2.hashCode());
        assertNotEquals(metrics1, metrics3);
    }
}
