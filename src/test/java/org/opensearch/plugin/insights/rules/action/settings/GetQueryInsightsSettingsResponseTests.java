/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.settings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link GetQueryInsightsSettingsResponse}.
 */
public class GetQueryInsightsSettingsResponseTests extends OpenSearchTestCase {

    /**
     * Test serialization and deserialization with empty settings
     */
    public void testSerializationWithEmptySettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                GetQueryInsightsSettingsResponse deserializedResponse = new GetQueryInsightsSettingsResponse(input);
                assertEquals(settings, deserializedResponse.getSettings());
                assertTrue(deserializedResponse.getSettings().isEmpty());
            }
        }
    }

    /**
     * Test serialization and deserialization with latency settings
     */
    public void testSerializationWithLatencySettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true);
        settings.put("latency.top_n_size", 10);
        settings.put("latency.window_size", "5m");

        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                GetQueryInsightsSettingsResponse deserializedResponse = new GetQueryInsightsSettingsResponse(input);
                assertEquals(settings, deserializedResponse.getSettings());
                assertEquals(3, deserializedResponse.getSettings().size());
            }
        }
    }

    /**
     * Test serialization with all metric types
     */
    public void testSerializationWithAllMetrics() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true);
        settings.put("latency.top_n_size", 10);
        settings.put("cpu.enabled", false);
        settings.put("cpu.top_n_size", 20);
        settings.put("memory.enabled", true);
        settings.put("memory.window_size", "10m");

        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                GetQueryInsightsSettingsResponse deserializedResponse = new GetQueryInsightsSettingsResponse(input);
                assertEquals(settings.size(), deserializedResponse.getSettings().size());
                assertEquals(true, deserializedResponse.getSettings().get("latency.enabled"));
                assertEquals(false, deserializedResponse.getSettings().get("cpu.enabled"));
            }
        }
    }

    /**
     * Test toXContent with latency settings only
     */
    public void testToXContentWithLatencySettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true);
        settings.put("latency.top_n_size", 10);
        settings.put("latency.window_size", "5m");

        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"persistent\""));
        assertTrue(json.contains("\"latency\""));
        assertTrue(json.contains("\"enabled\":true"));
        assertTrue(json.contains("\"top_n_size\":10"));
        assertTrue(json.contains("\"window_size\":\"5m\""));
    }

    /**
     * Test toXContent with CPU settings
     */
    public void testToXContentWithCpuSettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("cpu.enabled", false);
        settings.put("cpu.top_n_size", 15);
        settings.put("cpu.window_size", "1m");

        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"cpu\""));
        assertTrue(json.contains("\"enabled\":false"));
        assertTrue(json.contains("\"top_n_size\":15"));
    }

    /**
     * Test toXContent with memory settings
     */
    public void testToXContentWithMemorySettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("memory.enabled", true);
        settings.put("memory.top_n_size", 25);
        settings.put("memory.window_size", "30m");

        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"memory\""));
        assertTrue(json.contains("\"enabled\":true"));
        assertTrue(json.contains("\"top_n_size\":25"));
        assertTrue(json.contains("\"window_size\":\"30m\""));
    }

    /**
     * Test toXContent with grouping settings
     */
    public void testToXContentWithGroupingSettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("grouping.group_by", "SIMILARITY");

        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"grouping\""));
        assertTrue(json.contains("\"group_by\":\"SIMILARITY\""));
    }

    /**
     * Test toXContent with exporter settings
     */
    public void testToXContentWithExporterSettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("exporter.type", "local_index");
        settings.put("exporter.delete_after_days", 7);

        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"exporter\""));
        assertTrue(json.contains("\"type\":\"local_index\""));
        assertTrue(json.contains("\"delete_after_days\":7"));
    }

    /**
     * Test toXContent with all settings combined
     */
    public void testToXContentWithAllSettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true);
        settings.put("latency.top_n_size", 10);
        settings.put("cpu.enabled", false);
        settings.put("cpu.top_n_size", 20);
        settings.put("memory.enabled", true);
        settings.put("memory.window_size", "10m");
        settings.put("grouping.group_by", "NONE");
        settings.put("exporter.type", "debug");
        settings.put("exporter.delete_after_days", 3);

        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"latency\""));
        assertTrue(json.contains("\"cpu\""));
        assertTrue(json.contains("\"memory\""));
        assertTrue(json.contains("\"grouping\""));
        assertTrue(json.contains("\"exporter\""));
    }

    /**
     * Test toXContent excludes metrics without any settings
     */
    public void testToXContentExcludesEmptyMetrics() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true);
        // No CPU or memory settings

        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"latency\""));
        assertFalse(json.contains("\"cpu\""));
        assertFalse(json.contains("\"memory\""));
    }

    /**
     * Test getSettings returns correct map
     */
    public void testGetSettings() {
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true);
        settings.put("cpu.top_n_size", 50);

        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);
        Map<String, Object> retrievedSettings = response.getSettings();

        assertEquals(settings, retrievedSettings);
        assertEquals(2, retrievedSettings.size());
        assertEquals(true, retrievedSettings.get("latency.enabled"));
        assertEquals(50, retrievedSettings.get("cpu.top_n_size"));
    }

    /**
     * Test with partial metric settings (only one field per metric)
     */
    public void testToXContentWithPartialMetricSettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true); // Only enabled, no size or window
        settings.put("cpu.top_n_size", 15); // Only size, no enabled or window

        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"latency\""));
        assertTrue(json.contains("\"cpu\""));
        assertTrue(json.contains("\"enabled\":true"));
        assertTrue(json.contains("\"top_n_size\":15"));
    }

    /**
     * Test with string values for numeric settings
     */
    public void testWithStringValuesForNumericSettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.top_n_size", "10"); // String instead of int
        settings.put("exporter.delete_after_days", "7"); // String instead of int

        GetQueryInsightsSettingsResponse response = new GetQueryInsightsSettingsResponse(settings);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"top_n_size\":\"10\""));
        assertTrue(json.contains("\"delete_after_days\":\"7\""));
    }
}
