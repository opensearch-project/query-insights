/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.top_queries;

import java.io.IOException;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.rules.model.MetricType;

/**
 * A request to get cluster/node level top queries information.
 */
public class TopQueriesRequest extends BaseNodesRequest<TopQueriesRequest> {

    final MetricType metricType;
    final String from;
    final String to;
    final String id;
    final Boolean verbose;

    /**
     * Constructor for TopQueriesRequest
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public TopQueriesRequest(final StreamInput in) throws IOException {
        super(in);
        this.metricType = MetricType.readFromStream(in);
        this.from = null;
        this.to = null;
        this.verbose = null;
        this.id = null;
    }

    /**
     * Get top queries from nodes based on the nodes ids specified.
     * If none are passed, cluster level top queries will be returned.
     *
     * @param metricType {@link MetricType}
     * @param from start timestamp
     * @param to end timestamp
     * @param id query/group id
     * @param verbose whether to return full output
     * @param nodesIds the nodeIds specified in the request
     */
    public TopQueriesRequest(
        final MetricType metricType,
        final String from,
        final String to,
        final String id,
        final Boolean verbose,
        final String... nodesIds
    ) {
        super(nodesIds);
        this.metricType = metricType;
        this.from = from;
        this.to = to;
        this.verbose = verbose;
        this.id = id;
    }

    /**
     * Get the type of requested metrics
     * @return MetricType for current top query service
     */
    public MetricType getMetricType() {
        return metricType;
    }

    /**
     * Get from for timestamp request
     * @return String of from timestamp
     */
    public String getFrom() {
        return from;
    }

    /**
     * Get to for timestamp request
     * @return String of to timestamp
     */
    public String getTo() {
        return to;
    }

    /**
     * Get id which is the query_id and query_group_id
     * @return String of id
     */
    public String getId() {
        return id;
    }

    /**
     * Get verbose value for request
     * @return Boolean verbose value
     */
    public Boolean getVerbose() {
        return verbose;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(metricType.toString());
    }
}
