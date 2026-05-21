/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.List;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class LiveQueryRecordTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        LiveQueryRecord record = new LiveQueryRecord("query1", "running", 123L, "wlm1", 456L, 789L, 1024L, null, List.of());

        BytesStreamOutput out = new BytesStreamOutput();
        record.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        LiveQueryRecord deserialized = new LiveQueryRecord(in);

        assertEquals(record.getQueryId(), deserialized.getQueryId());
        assertEquals(record.getStatus(), deserialized.getStatus());
        assertEquals(record.getStartTime(), deserialized.getStartTime());
        assertEquals(record.getWlmGroupId(), deserialized.getWlmGroupId());
        assertEquals(record.getTotalLatency(), deserialized.getTotalLatency());
        assertEquals(record.getTotalCpu(), deserialized.getTotalCpu());
        assertEquals(record.getTotalMemory(), deserialized.getTotalMemory());
    }

    public void testToXContent() throws IOException {
        LiveQueryRecord record = new LiveQueryRecord("query1", "running", 123L, null, 456L, 789L, 1024L, null, List.of());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"id\":\"query1\""));
        assertTrue(json.contains("\"status\":\"running\""));
        assertTrue(json.contains("\"start_time\":123"));
        assertTrue(json.contains("\"total_latency_millis\":456"));
        assertTrue(json.contains("\"total_cpu_nanos\":789"));
        assertTrue(json.contains("\"total_memory_bytes\":1024"));
        assertFalse(json.contains("wlm_group_id"));
    }
}
