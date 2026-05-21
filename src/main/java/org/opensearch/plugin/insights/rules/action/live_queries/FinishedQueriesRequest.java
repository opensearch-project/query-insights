/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request to fetch finished queries from all (or specific) nodes.
 */
public class FinishedQueriesRequest extends BaseNodesRequest<FinishedQueriesRequest> {

    public FinishedQueriesRequest(StreamInput in) throws IOException {
        super(in);
    }

    public FinishedQueriesRequest(String... nodeIds) {
        super(nodeIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
