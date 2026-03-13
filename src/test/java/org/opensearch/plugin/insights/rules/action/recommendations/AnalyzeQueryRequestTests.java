/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.recommendations;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link AnalyzeQueryRequest}
 */
public class AnalyzeQueryRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws IOException {
        AnalyzeQueryRequest original = new AnalyzeQueryRequest("{\"term\":{\"status\":\"active\"}}", Arrays.asList("index1", "index2"));
        AnalyzeQueryRequest deserialized = roundTrip(original);

        assertEquals(original.getQuerySource(), deserialized.getQuerySource());
        assertEquals(original.getIndices(), deserialized.getIndices());
    }

    public void testSerializationWithEmptyIndices() throws IOException {
        AnalyzeQueryRequest original = new AnalyzeQueryRequest("{\"match_all\":{}}", Collections.emptyList());
        AnalyzeQueryRequest deserialized = roundTrip(original);

        assertEquals(original.getQuerySource(), deserialized.getQuerySource());
        assertTrue(deserialized.getIndices().isEmpty());
    }

    public void testSerializationWithNullIndicesDefaultsToEmpty() throws IOException {
        AnalyzeQueryRequest original = new AnalyzeQueryRequest("{\"match_all\":{}}", null);
        AnalyzeQueryRequest deserialized = roundTrip(original);

        assertEquals(original.getQuerySource(), deserialized.getQuerySource());
        assertTrue(deserialized.getIndices().isEmpty());
    }

    public void testValidateReturnsNullForValidRequest() {
        AnalyzeQueryRequest request = new AnalyzeQueryRequest("{\"term\":{\"status\":\"active\"}}", List.of("index1"));
        assertNull(request.validate());
    }

    public void testValidateRejectsNullQuerySource() {
        AnalyzeQueryRequest request = new AnalyzeQueryRequest(null, List.of("index1"));
        ActionRequestValidationException exception = request.validate();
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("query source must not be null or empty"));
    }

    public void testValidateRejectsEmptyQuerySource() {
        AnalyzeQueryRequest request = new AnalyzeQueryRequest("", List.of("index1"));
        ActionRequestValidationException exception = request.validate();
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("query source must not be null or empty"));
    }

    public void testGetters() {
        List<String> indices = Arrays.asList("a", "b");
        AnalyzeQueryRequest request = new AnalyzeQueryRequest("{\"match_all\":{}}", indices);

        assertEquals("{\"match_all\":{}}", request.getQuerySource());
        assertEquals(indices, request.getIndices());
    }

    private AnalyzeQueryRequest roundTrip(AnalyzeQueryRequest request) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new AnalyzeQueryRequest(in);
            }
        }
    }
}
