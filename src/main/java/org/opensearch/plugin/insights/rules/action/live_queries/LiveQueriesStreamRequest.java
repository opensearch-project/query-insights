/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request for streaming live queries
 */
public class LiveQueriesStreamRequest extends BaseNodesRequest<LiveQueriesStreamRequest> {

    public LiveQueriesStreamRequest(StreamInput in) throws IOException {
        super(in);
    }

    public LiveQueriesStreamRequest(String... nodesIds) {
        super(nodesIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}