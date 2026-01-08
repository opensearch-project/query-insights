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
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the cached and include_finished parameters in {@link LiveQueriesRequest}.
 */
public class LiveQueriesRequestCachedTests extends OpenSearchTestCase {

    public void testSerializationWithCachedAndIncludeFinished() throws IOException {
        boolean verbose = true;
        MetricType sortBy = MetricType.CPU;
        int size = 5;
        String[] nodeIds = new String[] { "nodeA", "nodeB" };
        String wlmGroupId = "DEFAULT_WORKLOAD_GROUP";
        boolean cached = true;
        boolean includeFinished = true;

        LiveQueriesRequest originalRequest = new LiveQueriesRequest(
            verbose,
            sortBy,
            size,
            nodeIds,
            wlmGroupId,
            null,
            cached,
            includeFinished
        );

        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesRequest deserializedRequest = new LiveQueriesRequest(in);

        assertEquals(originalRequest.isVerbose(), deserializedRequest.isVerbose());
        assertEquals(originalRequest.getSortBy(), deserializedRequest.getSortBy());
        assertEquals(originalRequest.getSize(), deserializedRequest.getSize());
        assertArrayEquals(originalRequest.nodesIds(), deserializedRequest.nodesIds());
        assertEquals(originalRequest.getWlmGroupId(), deserializedRequest.getWlmGroupId());
        assertEquals(originalRequest.isCached(), deserializedRequest.isCached());
        assertEquals(originalRequest.isIncludeFinished(), deserializedRequest.isIncludeFinished());
    }

    public void testCachedParameterDefaults() {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        assertFalse(request.isCached());
        assertFalse(request.isIncludeFinished());
    }

    public void testCachedParameterTrue() {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.LATENCY, 10, new String[0], null, null, true, false);
        assertTrue(request.isCached());
        assertFalse(request.isIncludeFinished());
    }

    public void testIncludeFinishedParameterTrue() {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.LATENCY, 10, new String[0], null, null, false, true);
        assertFalse(request.isCached());
        assertTrue(request.isIncludeFinished());
    }

    public void testBothCachedAndIncludeFinishedTrue() {
        LiveQueriesRequest request = new LiveQueriesRequest(
            false,
            MetricType.MEMORY,
            20,
            new String[] { "node1" },
            "TEST_GROUP",
            null,
            true,
            true
        );
        assertTrue(request.isCached());
        assertTrue(request.isIncludeFinished());
        assertEquals("TEST_GROUP", request.getWlmGroupId());
    }
}
