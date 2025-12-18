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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link UpdateQueryInsightsSettingsRequest}.
 */
public class UpdateQueryInsightsSettingsRequestTests extends OpenSearchTestCase {

    /**
     * Test default constructor creates empty settings map
     */
    public void testDefaultConstructor() {
        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest();
        assertNotNull(request.getSettings());
        assertTrue(request.getSettings().isEmpty());
    }

    /**
     * Test constructor with null settings
     */
    public void testConstructorWithNullSettings() {
        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest((Map<String, Object>) null);
        assertNotNull(request.getSettings());
        assertTrue(request.getSettings().isEmpty());
    }

    /**
     * Test constructor with empty settings map
     */
    public void testConstructorWithEmptySettings() {
        Map<String, Object> settings = new HashMap<>();
        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest(settings);
        assertNotNull(request.getSettings());
        assertTrue(request.getSettings().isEmpty());
    }

    /**
     * Test constructor with settings map
     */
    public void testConstructorWithSettings() {
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true);
        settings.put("cpu.top_n_size", 10);

        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest(settings);
        assertEquals(2, request.getSettings().size());
        assertEquals(true, request.getSettings().get("latency.enabled"));
        assertEquals(10, request.getSettings().get("cpu.top_n_size"));
    }

    /**
     * Test serialization and deserialization with empty settings
     */
    public void testSerializationWithEmptySettings() throws IOException {
        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                UpdateQueryInsightsSettingsRequest deserializedRequest = new UpdateQueryInsightsSettingsRequest(input);
                assertTrue(deserializedRequest.getSettings().isEmpty());
            }
        }
    }

    /**
     * Test serialization and deserialization with settings
     */
    public void testSerializationWithSettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true);
        settings.put("cpu.top_n_size", 15);
        settings.put("memory.window_size", "10m");

        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest(settings);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                UpdateQueryInsightsSettingsRequest deserializedRequest = new UpdateQueryInsightsSettingsRequest(input);
                assertEquals(3, deserializedRequest.getSettings().size());
                assertEquals(true, deserializedRequest.getSettings().get("latency.enabled"));
                assertEquals(15, deserializedRequest.getSettings().get("cpu.top_n_size"));
                assertEquals("10m", deserializedRequest.getSettings().get("memory.window_size"));
            }
        }
    }

    /**
     * Test setSetting method
     */
    public void testSetSetting() {
        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest();
        assertTrue(request.getSettings().isEmpty());

        request.setSetting("latency.enabled", true);
        assertEquals(1, request.getSettings().size());
        assertEquals(true, request.getSettings().get("latency.enabled"));

        request.setSetting("cpu.top_n_size", 20);
        assertEquals(2, request.getSettings().size());
        assertEquals(20, request.getSettings().get("cpu.top_n_size"));
    }

    /**
     * Test setSetting overwrites existing values
     */
    public void testSetSettingOverwrite() {
        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest();

        request.setSetting("latency.enabled", true);
        assertEquals(true, request.getSettings().get("latency.enabled"));

        request.setSetting("latency.enabled", false);
        assertEquals(false, request.getSettings().get("latency.enabled"));
    }

    /**
     * Test setSetting with null key
     */
    public void testSetSettingWithNullKey() {
        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest();
        request.setSetting(null, "value");
        assertEquals(1, request.getSettings().size());
        assertEquals("value", request.getSettings().get(null));
        assertTrue(request.getSettings().containsKey(null));
    }

    /**
     * Test setSetting with null value
     */
    public void testSetSettingWithNullValue() {
        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest();
        request.setSetting("key", null);
        assertEquals(1, request.getSettings().size());
        assertNull(request.getSettings().get("key"));
    }

    /**
     * Test getSettings returns correct map
     */
    public void testGetSettings() {
        Map<String, Object> settings = new HashMap<>();
        settings.put("latency.enabled", true);
        settings.put("cpu.top_n_size", 25);

        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest(settings);
        Map<String, Object> retrievedSettings = request.getSettings();

        assertEquals(settings, retrievedSettings);
        assertEquals(2, retrievedSettings.size());
    }

    /**
     * Test validate method returns null (no validation errors)
     */
    public void testValidate() {
        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest();
        assertNull(request.validate());

        request.setSetting("latency.enabled", true);
        assertNull(request.validate());
    }

    /**
     * Test serialization with various data types
     */
    public void testSerializationWithVariousTypes() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("boolean_value", true);
        settings.put("integer_value", 42);
        settings.put("string_value", "test");
        settings.put("long_value", 9999L);

        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest(settings);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                UpdateQueryInsightsSettingsRequest deserializedRequest = new UpdateQueryInsightsSettingsRequest(input);
                assertEquals(4, deserializedRequest.getSettings().size());
                assertEquals(true, deserializedRequest.getSettings().get("boolean_value"));
                assertEquals(42, deserializedRequest.getSettings().get("integer_value"));
                assertEquals("test", deserializedRequest.getSettings().get("string_value"));
                assertEquals(9999L, deserializedRequest.getSettings().get("long_value"));
            }
        }
    }

    /**
     * Test settings map is mutable after construction
     */
    public void testSettingsMapIsMutable() {
        Map<String, Object> settings = new HashMap<>();
        settings.put("initial_key", "initial_value");

        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest(settings);
        assertEquals(1, request.getSettings().size());

        // Modify through setSetting
        request.setSetting("new_key", "new_value");
        assertEquals(2, request.getSettings().size());
        assertTrue(request.getSettings().containsKey("new_key"));
    }

    /**
     * Test serialization with complex nested settings
     */
    public void testSerializationWithComplexSettings() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("search.insights.top_queries.latency.enabled", true);
        settings.put("search.insights.top_queries.cpu.top_n_size", 100);
        settings.put("search.insights.top_queries.memory.window_size", "30m");
        settings.put("search.insights.top_queries.grouping.group_by", "SIMILARITY");

        UpdateQueryInsightsSettingsRequest request = new UpdateQueryInsightsSettingsRequest(settings);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                UpdateQueryInsightsSettingsRequest deserializedRequest = new UpdateQueryInsightsSettingsRequest(input);
                assertEquals(4, deserializedRequest.getSettings().size());
                assertEquals(true, deserializedRequest.getSettings().get("search.insights.top_queries.latency.enabled"));
                assertEquals("SIMILARITY", deserializedRequest.getSettings().get("search.insights.top_queries.grouping.group_by"));
            }
        }
    }
}
