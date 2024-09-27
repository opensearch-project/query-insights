/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.health_stats;

import java.io.IOException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the {@link HealthStatsRequest} class.
 */
public class HealthStatsRequestTests extends OpenSearchTestCase {

    public void testConstructorWithNodeIds() {
        String[] nodeIds = { "node1", "node2" };
        HealthStatsRequest request = new HealthStatsRequest(nodeIds);
        assertNotNull(request);
        assertArrayEquals(nodeIds, request.nodesIds());
    }

    public void testConstructorWithStreamInput() throws IOException {
        String[] nodeIds = { "node1", "node2" };
        HealthStatsRequest originalRequest = new HealthStatsRequest(nodeIds);
        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        HealthStatsRequest requestFromStream = new HealthStatsRequest(in);
        assertNotNull(requestFromStream);
        assertArrayEquals(nodeIds, requestFromStream.nodesIds());
    }

    public void testWriteTo() throws IOException {
        String[] nodeIds = { "node1", "node2" };
        HealthStatsRequest request = new HealthStatsRequest(nodeIds);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        HealthStatsRequest deserializedRequest = new HealthStatsRequest(in);
        assertNotNull(deserializedRequest);
        assertArrayEquals(nodeIds, deserializedRequest.nodesIds());
    }
}
