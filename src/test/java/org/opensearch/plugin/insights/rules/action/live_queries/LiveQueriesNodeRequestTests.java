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
 * Unit tests for the {@link LiveQueriesNodeRequest} class.
 */
public class LiveQueriesNodeRequestTests extends OpenSearchTestCase {

    public void testConstructorWithRequest() {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.CPU, 10, new String[] { "node1" }, "group1", null);
        LiveQueriesNodeRequest nodeRequest = new LiveQueriesNodeRequest(request);

        assertEquals(request, nodeRequest.getRequest());
    }

    public void testSerialization() throws IOException {
        LiveQueriesRequest request = new LiveQueriesRequest(
            false,
            MetricType.MEMORY,
            5,
            new String[] { "nodeA", "nodeB" },
            "DEFAULT_WORKLOAD_GROUP",
            null
        );
        LiveQueriesNodeRequest originalRequest = new LiveQueriesNodeRequest(request);

        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesNodeRequest deserializedRequest = new LiveQueriesNodeRequest(in);

        assertEquals(originalRequest.getRequest().isVerbose(), deserializedRequest.getRequest().isVerbose());
        assertEquals(originalRequest.getRequest().getSortBy(), deserializedRequest.getRequest().getSortBy());
        assertEquals(originalRequest.getRequest().getSize(), deserializedRequest.getRequest().getSize());
        assertArrayEquals(originalRequest.getRequest().nodesIds(), deserializedRequest.getRequest().nodesIds());
        assertEquals(originalRequest.getRequest().getWlmGroupId(), deserializedRequest.getRequest().getWlmGroupId());
    }

    public void testGetRequest() {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        LiveQueriesNodeRequest nodeRequest = new LiveQueriesNodeRequest(request);

        assertSame(request, nodeRequest.getRequest());
    }

    public void should_SerializeCorrectly_When_EmptyNodeIds() throws IOException {
        LiveQueriesRequest request = new LiveQueriesRequest(true, MetricType.LATENCY, 3, new String[] {}, "group", null);
        LiveQueriesNodeRequest original = new LiveQueriesNodeRequest(request);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesNodeRequest deserialized = new LiveQueriesNodeRequest(in);

        assertEquals(0, deserialized.getRequest().nodesIds().length);
        assertEquals("group", deserialized.getRequest().getWlmGroupId());
    }

    public void should_PreserveAllFields_When_SerializedWithNullGroup() throws IOException {
        LiveQueriesRequest request = new LiveQueriesRequest(false, MetricType.CPU, 1, new String[] { "node1" }, null, null);
        LiveQueriesNodeRequest original = new LiveQueriesNodeRequest(request);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesNodeRequest deserialized = new LiveQueriesNodeRequest(in);

        assertNull(deserialized.getRequest().getWlmGroupId());
        assertFalse(deserialized.getRequest().isVerbose());
        assertEquals(MetricType.CPU, deserialized.getRequest().getSortBy());
    }

    public void should_ThrowException_When_NullRequestProvided() {
        LiveQueriesRequest nullRequest = null;
        assertThrows(NullPointerException.class, () -> { new LiveQueriesNodeRequest(nullRequest); });
    }

    public void should_ThrowException_When_SerializingWithNullStream() {
        LiveQueriesRequest request = new LiveQueriesRequest(true);
        LiveQueriesNodeRequest nodeRequest = new LiveQueriesNodeRequest(request);

        assertThrows(NullPointerException.class, () -> { nodeRequest.writeTo(null); });
    }
}
