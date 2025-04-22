/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the {@link LiveQueriesRequest} class.
 */
public class LiveQueriesRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        boolean verbose = true;
        MetricType sortBy = MetricType.CPU;
        int size = 5;
        String[] nodeIds = new String[] { "nodeA", "nodeB", "nodeC" };

        LiveQueriesRequest originalRequest = new LiveQueriesRequest(verbose, sortBy, size, nodeIds);

        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesRequest deserializedRequest = new LiveQueriesRequest(in);

        assertEquals(originalRequest.isVerbose(), deserializedRequest.isVerbose());
        assertEquals(originalRequest.getSortBy(), deserializedRequest.getSortBy());
        assertEquals(originalRequest.getSize(), deserializedRequest.getSize());
        assertArrayEquals(originalRequest.nodesIds(), deserializedRequest.nodesIds());
    }

    public void testSerializationWithEmptyNodes() throws IOException {
        boolean verbose = false;
        MetricType sortBy = MetricType.MEMORY;
        int size = 3;
        String[] nodeIds = new String[0];

        LiveQueriesRequest originalRequest = new LiveQueriesRequest(verbose, sortBy, size, nodeIds);

        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesRequest deserializedRequest = new LiveQueriesRequest(in);

        assertEquals(originalRequest.isVerbose(), deserializedRequest.isVerbose());
        assertEquals(originalRequest.getSortBy(), deserializedRequest.getSortBy());
        assertEquals(originalRequest.getSize(), deserializedRequest.getSize());
        assertArrayEquals(originalRequest.nodesIds(), deserializedRequest.nodesIds());
        assertEquals(0, deserializedRequest.nodesIds().length);
    }

    public void testMainConstructor() {
        boolean verbose = true;
        MetricType sortBy = MetricType.CPU;
        int size = 50;
        String[] nodeIds = { "node1", "node2" };

        LiveQueriesRequest request = new LiveQueriesRequest(verbose, sortBy, size, nodeIds);

        assertTrue(request.isVerbose());
        assertEquals(MetricType.CPU, request.getSortBy());
        assertEquals(50, request.getSize());
        assertArrayEquals(new String[] { "node1", "node2" }, request.nodesIds());
    }

    public void testConvenienceConstructor() {
        boolean verbose = false;
        String[] nodeIds = { "node3" };

        LiveQueriesRequest request = new LiveQueriesRequest(verbose, nodeIds);

        assertFalse(request.isVerbose());
        assertEquals(MetricType.LATENCY, request.getSortBy());
        assertEquals(QueryInsightsSettings.DEFAULT_LIVE_QUERIES_SIZE, request.getSize());
        assertArrayEquals(new String[] { "node3" }, request.nodesIds());
    }

    public void testConvenienceConstructorNoNodes() {
        boolean verbose = true;
        // No node IDs specified
        LiveQueriesRequest request = new LiveQueriesRequest(verbose);

        assertTrue(request.isVerbose());
        assertEquals(MetricType.LATENCY, request.getSortBy());
        assertEquals(QueryInsightsSettings.DEFAULT_LIVE_QUERIES_SIZE, request.getSize());
        assertEquals(0, request.nodesIds().length);
    }

    public void testGetters() {
        LiveQueriesRequest request = new LiveQueriesRequest(false, MetricType.MEMORY, 10, "nodeA");
        assertFalse(request.isVerbose());
        assertEquals(MetricType.MEMORY, request.getSortBy());
        assertEquals(10, request.getSize());
        assertArrayEquals(new String[] { "nodeA" }, request.nodesIds());
    }
}
