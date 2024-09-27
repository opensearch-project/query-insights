/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.health_stats;

import java.io.IOException;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * A request to get cluster/node level health stats information.
 */
public class HealthStatsRequest extends BaseNodesRequest<HealthStatsRequest> {
    /**
     * Constructor for HealthStatsRequest
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public HealthStatsRequest(final StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Get health stats from nodes based on the nodes ids specified.
     * If none are passed, cluster level health stats will be returned.
     *
     * @param nodesIds the nodeIds specified in the request
     */
    public HealthStatsRequest(final String... nodesIds) {
        super(nodesIds);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
