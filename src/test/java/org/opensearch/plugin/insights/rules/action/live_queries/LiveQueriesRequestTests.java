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
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the {@link LiveQueriesRequest} class.
 */
public class LiveQueriesRequestTests extends OpenSearchTestCase {

    public void testConstructorWithNodeIdsAndVerbose() {
        String[] nodeIds = { "node1", "node2" };
        boolean verbose = true;
        LiveQueriesRequest request = new LiveQueriesRequest(verbose, nodeIds);
        assertNotNull(request);
        assertArrayEquals(nodeIds, request.nodesIds());
        assertTrue(request.isVerbose());

        verbose = false;
        request = new LiveQueriesRequest(verbose, nodeIds);
        assertNotNull(request);
        assertArrayEquals(nodeIds, request.nodesIds());
        assertFalse(request.isVerbose());
    }

    public void testConstructorWithStreamInput() throws IOException {
        String[] nodeIds = { "node1", "node2" };
        boolean verbose = randomBoolean();
        LiveQueriesRequest originalRequest = new LiveQueriesRequest(verbose, nodeIds);
        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesRequest requestFromStream = new LiveQueriesRequest(in);
        assertNotNull(requestFromStream);
        assertArrayEquals(nodeIds, requestFromStream.nodesIds());
        assertEquals(verbose, requestFromStream.isVerbose());
    }

    public void testWriteTo() throws IOException {
        String[] nodeIds = { "node1", "node2" };
        boolean verbose = randomBoolean();
        LiveQueriesRequest request = new LiveQueriesRequest(verbose, nodeIds);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesRequest deserializedRequest = new LiveQueriesRequest(in);
        assertNotNull(deserializedRequest);
        assertArrayEquals(nodeIds, deserializedRequest.nodesIds());
        assertEquals(verbose, deserializedRequest.isVerbose());
    }

    public void testIsVerbose() {
        LiveQueriesRequest requestTrue = new LiveQueriesRequest(true, "node1");
        assertTrue(requestTrue.isVerbose());

        LiveQueriesRequest requestFalse = new LiveQueriesRequest(false, "node1");
        assertFalse(requestFalse.isVerbose());
    }
}
