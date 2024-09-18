/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.health_stats;

import java.io.IOException;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.healthStats.QueryInsightsHealthStats;

/**
 * Holds the health stats retrieved from a node
 */
public class HealthStatsNodeResponse extends BaseNodeResponse implements ToXContentObject {
    /** The health stats retrieved from one node */
    private final QueryInsightsHealthStats healthStats;

    /**
     * Create the HealthStatsNodeResponse Object from StreamInput
     * @param in A {@link StreamInput} object.
     * @throws IOException IOException
     */
    public HealthStatsNodeResponse(final StreamInput in) throws IOException {
        super(in);
        healthStats = new QueryInsightsHealthStats(in);
    }

    /**
     * Create the HealthStatsNodeResponse Object
     * @param node A node that is part of the cluster.
     * @param healthStats A list of HealthStats from nodes.
     */
    public HealthStatsNodeResponse(final DiscoveryNode node, final QueryInsightsHealthStats healthStats) {
        super(node);
        this.healthStats = healthStats;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(this.getNode().getId());
        healthStats.toXContent(builder, params);
        return builder.endObject();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        healthStats.writeTo(out);

    }

    /**
     * Get health stats
     *
     * @return the health stats records in this node response
     */
    public QueryInsightsHealthStats getHealthStats() {
        return healthStats;
    }
}
