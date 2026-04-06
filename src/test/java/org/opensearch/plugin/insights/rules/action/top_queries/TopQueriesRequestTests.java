/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.top_queries;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Granular tests for the {@link TopQueriesRequest} class.
 */
public class TopQueriesRequestTests extends OpenSearchTestCase {

    /**
     * Check that we can set the metric type
     */
    public void testSetMetricType() throws Exception {
        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY, null, null, randomAlphaOfLength(5), null, null);
        TopQueriesRequest deserializedRequest = roundTripRequest(request);
        assertEquals(request.getMetricType(), deserializedRequest.getMetricType());
    }

    public void testRecommendationsTrueRoundTrip() throws Exception {
        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY, null, null, null, null, true);
        TopQueriesRequest deserialized = roundTripRequest(request);
        assertEquals(true, deserialized.getRecommendations());
    }

    public void testRecommendationsFalseRoundTrip() throws Exception {
        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY, null, null, null, null, false);
        TopQueriesRequest deserialized = roundTripRequest(request);
        assertEquals(false, deserialized.getRecommendations());
    }

    public void testRecommendationsNullRoundTrip() throws Exception {
        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY, null, null, null, null, null);
        TopQueriesRequest deserialized = roundTripRequest(request);
        assertNull(deserialized.getRecommendations());
    }

    /**
     * Serialize and deserialize a request.
     * @param request A request to serialize.
     * @return The deserialized, "round-tripped" request.
     */
    private static TopQueriesRequest roundTripRequest(TopQueriesRequest request) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new TopQueriesRequest(in);
            }
        }
    }
}
