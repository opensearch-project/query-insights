/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.List;
import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.rules.model.FinishedQueryRecord;

/**
 * Per-node response carrying finished queries from one node.
 */
public class FinishedQueriesNodeResponse extends BaseNodeResponse {

    private final List<FinishedQueryRecord> finishedQueries;

    public FinishedQueriesNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.finishedQueries = in.getVersion().onOrAfter(Version.V_3_6_0) ? in.readList(FinishedQueryRecord::new) : List.of();
    }

    public FinishedQueriesNodeResponse(DiscoveryNode node, List<FinishedQueryRecord> finishedQueries) {
        super(node);
        this.finishedQueries = finishedQueries;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_3_6_0)) {
            out.writeList(finishedQueries);
        }
    }

    public List<FinishedQueryRecord> getFinishedQueries() {
        return finishedQueries;
    }
}
