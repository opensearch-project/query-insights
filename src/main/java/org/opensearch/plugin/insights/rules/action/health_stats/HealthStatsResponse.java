/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.health_stats;

import java.io.IOException;
import java.util.List;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Transport response for cluster/node level health stats
 */
public class HealthStatsResponse extends BaseNodesResponse<HealthStatsNodeResponse> implements ToXContentFragment {
    /**
     * Constructor for HealthStatsNodeResponseResponse.
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public HealthStatsResponse(final StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructor for HealthStatsResponse
     *
     * @param clusterName The current cluster name
     * @param nodes A list that contains health stats from all nodes
     * @param failures A list that contains FailedNodeException
     */
    public HealthStatsResponse(
        final ClusterName clusterName,
        final List<HealthStatsNodeResponse> nodes,
        final List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<HealthStatsNodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(HealthStatsNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(final StreamOutput out, final List<HealthStatsNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        final List<HealthStatsNodeResponse> results = getNodes();
        builder.startObject();
        for (HealthStatsNodeResponse nodeResponse : results) {
            nodeResponse.toXContent(builder, params);
        }
        return builder.endObject();
    }

    @Override
    public String toString() {
        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            this.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.toString();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
