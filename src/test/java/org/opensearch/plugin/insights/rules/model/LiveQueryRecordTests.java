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
        LiveQueryRecord record = new LiveQueryRecord(
            "query1",
            "running",
            123L,
            "wlm1",
            456L,
            789L,
            1024L,
            null,
            List.of(),
            null,
            List.of(),
            List.of()
        );

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
        LiveQueryRecord record = new LiveQueryRecord(
            "query1",
            "running",
            123L,
            null,
            456L,
            789L,
            1024L,
            null,
            List.of(),
            null,
            List.of(),
            List.of()
        );

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

    public void testSerializationWithUserInfo() throws IOException {
        LiveQueryRecord record = new LiveQueryRecord(
            "query2",
            "running",
            1000L,
            "wlm-group-1",
            500L,
            300L,
            2048L,
            null,
            List.of(),
            "testuser",
            List.of("all_access", "readall"),
            List.of("admin", "devteam")
        );

        BytesStreamOutput out = new BytesStreamOutput();
        record.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        LiveQueryRecord deserialized = new LiveQueryRecord(in);

        assertEquals("testuser", deserialized.getUsername());
        assertEquals(List.of("all_access", "readall"), deserialized.getUserRoles());
        assertEquals(List.of("admin", "devteam"), deserialized.getBackendRoles());
    }

    public void testSerializationWithNullUserInfo() throws IOException {
        LiveQueryRecord record = new LiveQueryRecord(
            "query3",
            "completed",
            2000L,
            null,
            100L,
            50L,
            512L,
            null,
            List.of(),
            null,
            List.of(),
            List.of()
        );

        BytesStreamOutput out = new BytesStreamOutput();
        record.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        LiveQueryRecord deserialized = new LiveQueryRecord(in);

        assertNull(deserialized.getUsername());
        assertEquals(List.of(), deserialized.getUserRoles());
        assertEquals(List.of(), deserialized.getBackendRoles());
    }

    public void testToXContentWithUserInfo() throws IOException {
        LiveQueryRecord record = new LiveQueryRecord(
            "query4",
            "running",
            3000L,
            "wlm1",
            700L,
            400L,
            4096L,
            null,
            List.of(),
            "analyst_user",
            List.of("all_access", "readall"),
            List.of("backend_role1")
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"username\":\"analyst_user\""));
        assertTrue(json.contains("\"user_roles\":[\"all_access\",\"readall\"]"));
        assertTrue(json.contains("\"backend_roles\":[\"backend_role1\"]"));
    }

    public void testToXContentWithoutUserInfo() throws IOException {
        LiveQueryRecord record = new LiveQueryRecord(
            "query5",
            "running",
            4000L,
            null,
            200L,
            100L,
            1024L,
            null,
            List.of(),
            null,
            List.of(),
            List.of()
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertFalse(json.contains("username"));
        assertFalse(json.contains("user_roles"));
        assertFalse(json.contains("backend_roles"));
    }

    public void testSerializationWithNullRolesLists() throws IOException {
        LiveQueryRecord record = new LiveQueryRecord(
            "query6",
            "running",
            5000L,
            null,
            100L,
            50L,
            256L,
            null,
            List.of(),
            "user_with_null_roles",
            null,
            null
        );

        BytesStreamOutput out = new BytesStreamOutput();
        record.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        LiveQueryRecord deserialized = new LiveQueryRecord(in);

        assertEquals("user_with_null_roles", deserialized.getUsername());
        assertEquals(List.of(), deserialized.getUserRoles());
        assertEquals(List.of(), deserialized.getBackendRoles());
    }
}
