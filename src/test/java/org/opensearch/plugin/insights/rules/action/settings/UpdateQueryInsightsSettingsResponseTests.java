/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.settings;

import java.io.IOException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link UpdateQueryInsightsSettingsResponse}.
 */
public class UpdateQueryInsightsSettingsResponseTests extends OpenSearchTestCase {

    /**
     * Test serialization and deserialization with acknowledged true
     */
    public void testSerializationWithAcknowledgedTrue() throws IOException {
        UpdateQueryInsightsSettingsResponse response = new UpdateQueryInsightsSettingsResponse(true);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                UpdateQueryInsightsSettingsResponse deserializedResponse = new UpdateQueryInsightsSettingsResponse(input);
                assertTrue(deserializedResponse.isAcknowledged());
            }
        }
    }

    /**
     * Test serialization and deserialization with acknowledged false
     */
    public void testSerializationWithAcknowledgedFalse() throws IOException {
        UpdateQueryInsightsSettingsResponse response = new UpdateQueryInsightsSettingsResponse(false);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                UpdateQueryInsightsSettingsResponse deserializedResponse = new UpdateQueryInsightsSettingsResponse(input);
                assertFalse(deserializedResponse.isAcknowledged());
            }
        }
    }

    /**
     * Test isAcknowledged returns correct value
     */
    public void testIsAcknowledged() {
        UpdateQueryInsightsSettingsResponse responseTrue = new UpdateQueryInsightsSettingsResponse(true);
        assertTrue(responseTrue.isAcknowledged());

        UpdateQueryInsightsSettingsResponse responseFalse = new UpdateQueryInsightsSettingsResponse(false);
        assertFalse(responseFalse.isAcknowledged());
    }

    /**
     * Test toXContent with acknowledged true
     */
    public void testToXContentWithAcknowledgedTrue() throws IOException {
        UpdateQueryInsightsSettingsResponse response = new UpdateQueryInsightsSettingsResponse(true);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"acknowledged\":true"));
    }

    /**
     * Test toXContent with acknowledged false
     */
    public void testToXContentWithAcknowledgedFalse() throws IOException {
        UpdateQueryInsightsSettingsResponse response = new UpdateQueryInsightsSettingsResponse(false);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"acknowledged\":false"));
    }

    /**
     * Test toXContent contains proper JSON structure
     */
    public void testToXContentStructure() throws IOException {
        UpdateQueryInsightsSettingsResponse response = new UpdateQueryInsightsSettingsResponse(true);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        // Should start and end with braces
        assertTrue(json.startsWith("{"));
        assertTrue(json.endsWith("}"));
        // Should contain acknowledged field
        assertTrue(json.contains("acknowledged"));
    }

    /**
     * Test multiple serialization/deserialization cycles
     */
    public void testMultipleSerializationCycles() throws IOException {
        UpdateQueryInsightsSettingsResponse original = new UpdateQueryInsightsSettingsResponse(true);

        // First cycle
        try (BytesStreamOutput output1 = new BytesStreamOutput()) {
            original.writeTo(output1);

            try (StreamInput input1 = output1.bytes().streamInput()) {
                UpdateQueryInsightsSettingsResponse deserialized1 = new UpdateQueryInsightsSettingsResponse(input1);
                assertTrue(deserialized1.isAcknowledged());

                // Second cycle
                try (BytesStreamOutput output2 = new BytesStreamOutput()) {
                    deserialized1.writeTo(output2);

                    try (StreamInput input2 = output2.bytes().streamInput()) {
                        UpdateQueryInsightsSettingsResponse deserialized2 = new UpdateQueryInsightsSettingsResponse(input2);
                        assertTrue(deserialized2.isAcknowledged());
                        assertEquals(original.isAcknowledged(), deserialized2.isAcknowledged());
                    }
                }
            }
        }
    }

    /**
     * Test constructor with StreamInput
     */
    public void testConstructorWithStreamInput() throws IOException {
        boolean acknowledged = true;

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeBoolean(acknowledged);

            try (StreamInput input = output.bytes().streamInput()) {
                UpdateQueryInsightsSettingsResponse response = new UpdateQueryInsightsSettingsResponse(input);
                assertEquals(acknowledged, response.isAcknowledged());
            }
        }
    }

    /**
     * Test toXContent output format consistency
     */
    public void testToXContentOutputFormatConsistency() throws IOException {
        UpdateQueryInsightsSettingsResponse responseTrue = new UpdateQueryInsightsSettingsResponse(true);
        UpdateQueryInsightsSettingsResponse responseFalse = new UpdateQueryInsightsSettingsResponse(false);

        XContentBuilder builderTrue = XContentFactory.jsonBuilder();
        responseTrue.toXContent(builderTrue, ToXContent.EMPTY_PARAMS);
        String jsonTrue = builderTrue.toString();

        XContentBuilder builderFalse = XContentFactory.jsonBuilder();
        responseFalse.toXContent(builderFalse, ToXContent.EMPTY_PARAMS);
        String jsonFalse = builderFalse.toString();

        // Both should have same structure, just different boolean values
        assertTrue(jsonTrue.contains("acknowledged"));
        assertTrue(jsonFalse.contains("acknowledged"));
        assertNotEquals(jsonTrue, jsonFalse);
    }
}
