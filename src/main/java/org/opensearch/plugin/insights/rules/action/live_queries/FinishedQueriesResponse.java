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
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.rules.model.FinishedQueryRecord;

/**
 * Aggregated response carrying finished queries from all nodes.
 */
public class FinishedQueriesResponse extends BaseNodesResponse<FinishedQueriesNodeResponse> {

    public FinishedQueriesResponse(StreamInput in) throws IOException {
        super(in);
    }

    public FinishedQueriesResponse(ClusterName clusterName, List<FinishedQueriesNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<FinishedQueriesNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(FinishedQueriesNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<FinishedQueriesNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    public List<FinishedQueryRecord> getAllFinishedQueries() {
        return getNodes().stream().flatMap(n -> n.getFinishedQueries().stream()).toList();
    }
}
