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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link GetQueryInsightsSettingsRequest}.
 */
public class GetQueryInsightsSettingsRequestTests extends OpenSearchTestCase {

    /**
     * Test serialization and deserialization with null metric type
     */
    public void testSerializationWithNullMetricType() throws IOException {
        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest((String) null);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                GetQueryInsightsSettingsRequest deserializedRequest = new GetQueryInsightsSettingsRequest(input);
                assertNull(deserializedRequest.getMetricType());
            }
        }
    }

    /**
     * Test serialization and deserialization with specific metric type
     */
    public void testSerializationWithMetricType() throws IOException {
        String metricType = "latency";
        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest(metricType);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                GetQueryInsightsSettingsRequest deserializedRequest = new GetQueryInsightsSettingsRequest(input);
                assertEquals(metricType, deserializedRequest.getMetricType());
            }
        }
    }

    /**
     * Test serialization with different metric types
     */
    public void testSerializationWithDifferentMetricTypes() throws IOException {
        String[] metricTypes = { "latency", "cpu", "memory", "custom_metric" };

        for (String metricType : metricTypes) {
            GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest(metricType);

            try (BytesStreamOutput output = new BytesStreamOutput()) {
                request.writeTo(output);

                try (StreamInput input = output.bytes().streamInput()) {
                    GetQueryInsightsSettingsRequest deserializedRequest = new GetQueryInsightsSettingsRequest(input);
                    assertEquals(metricType, deserializedRequest.getMetricType());
                }
            }
        }
    }

    /**
     * Test getMetricType returns correct value
     */
    public void testGetMetricType() {
        String metricType = "cpu";
        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest(metricType);
        assertEquals(metricType, request.getMetricType());

        GetQueryInsightsSettingsRequest requestWithNull = new GetQueryInsightsSettingsRequest((String) null);
        assertNull(requestWithNull.getMetricType());
    }

    /**
     * Test validate method returns null (no validation errors)
     */
    public void testValidate() {
        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest("latency");
        assertNull(request.validate());

        GetQueryInsightsSettingsRequest requestWithNull = new GetQueryInsightsSettingsRequest((String) null);
        assertNull(requestWithNull.validate());
    }

    /**
     * Test constructor with empty string
     */
    public void testConstructorWithEmptyString() throws IOException {
        GetQueryInsightsSettingsRequest request = new GetQueryInsightsSettingsRequest("");
        assertEquals("", request.getMetricType());

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                GetQueryInsightsSettingsRequest deserializedRequest = new GetQueryInsightsSettingsRequest(input);
                assertEquals("", deserializedRequest.getMetricType());
            }
        }
    }
}
