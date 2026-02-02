/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.query_metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.QueryTypeMetrics;

/**
 * Holds the query type metrics retrieved from a node
 */
public class QueryMetricsNodeResponse extends BaseNodeResponse implements ToXContentObject {
    /** The metrics retrieved from one node */
    private final Map<String, QueryTypeMetrics> queryTypeMetrics;

    /**
     * Create the QueryMetricsNodeResponse Object from StreamInput
     * @param in A {@link StreamInput} object.
     * @throws IOException IOException
     */
    public QueryMetricsNodeResponse(final StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        queryTypeMetrics = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            QueryTypeMetrics metrics = new QueryTypeMetrics(in);
            queryTypeMetrics.put(key, metrics);
        }
    }

    /**
     * Create the QueryMetricsNodeResponse Object
     * @param node A node that is part of the cluster.
     * @param queryTypeMetrics Map of aggregation type to QueryTypeMetrics from this node.
     */
    public QueryMetricsNodeResponse(final DiscoveryNode node, final Map<String, QueryTypeMetrics> queryTypeMetrics) {
        super(node);
        this.queryTypeMetrics = queryTypeMetrics != null ? queryTypeMetrics : new HashMap<>();
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(this.getNode().getId());
        builder.startArray("query_type_metrics");
        for (QueryTypeMetrics metrics : queryTypeMetrics.values()) {
            metrics.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(queryTypeMetrics.size());
        for (Map.Entry<String, QueryTypeMetrics> entry : queryTypeMetrics.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    /**
     * Get query type metrics
     *
     * @return the query type metrics in this node response
     */
    public Map<String, QueryTypeMetrics> getQueryTypeMetrics() {
        return queryTypeMetrics;
    }
}
