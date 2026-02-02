/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.query_metrics;

import java.io.IOException;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * A request to get cluster/node level query type metrics.
 */
public class QueryMetricsRequest extends BaseNodesRequest<QueryMetricsRequest> {

    private final String aggregationType;

    /**
     * Constructor for QueryMetricsRequest
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public QueryMetricsRequest(final StreamInput in) throws IOException {
        super(in);
        this.aggregationType = in.readOptionalString();
    }

    /**
     * Get query metrics from nodes based on the nodes ids specified.
     * If none are passed, cluster level metrics will be returned.
     *
     * @param aggregationType optional filter for specific aggregation type (e.g., "streaming_terms")
     * @param nodesIds the nodeIds specified in the request
     */
    public QueryMetricsRequest(final String aggregationType, final String... nodesIds) {
        super(nodesIds);
        this.aggregationType = aggregationType;
    }

    /**
     * Get the aggregation type filter
     *
     * @return the aggregation type or null for all types
     */
    public String getAggregationType() {
        return aggregationType;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(aggregationType);
    }
}
