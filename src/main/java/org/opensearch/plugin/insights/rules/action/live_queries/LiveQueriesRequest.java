/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/**
 * A request to get cluster/node level ongoing live queries information.
 */
public class LiveQueriesRequest extends ActionRequest {

    private final boolean verbose;
    private final MetricType sortBy;
    // Maximum number of results to return
    private final int size;
    // Node IDs to filter queries by
    private final String[] nodeIds;

    /**
     * Constructor for LiveQueriesRequest
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public LiveQueriesRequest(final StreamInput in) throws IOException {
        super(in);
        this.verbose = in.readBoolean();
        this.sortBy = MetricType.readFromStream(in);
        this.size = in.readInt();
        this.nodeIds = in.readStringArray();
    }

    /**
     * Get live queries from nodes based on the nodes ids specified.
     * If none are passed, cluster level live queries will be returned.
     *
     * @param verbose Whether to include verbose information about live queries (defaults to true)
     * @param sortBy the metric to sort by (latency, cpu, memory)
     * @param size maximum number of results
     * @param nodeIds The node IDs specified in the request
     */
    public LiveQueriesRequest(final boolean verbose, final MetricType sortBy, final int size, final String... nodeIds) {
        this.verbose = verbose;
        this.sortBy = sortBy;
        this.size = size;
        this.nodeIds = nodeIds;
    }

    /**
     * Convenience constructor using default sortBy=LATENCY and no size limit.
     * @param verbose whether to include verbose information about live queries
     * @param nodeIds the node IDs specified in the request
     */
    public LiveQueriesRequest(final boolean verbose, final String... nodeIds) {
        this(verbose, MetricType.LATENCY, QueryInsightsSettings.DEFAULT_LIVE_QUERIES_SIZE, nodeIds);
    }

    /**
     * Get whether verbose information is requested
     * @return boolean indicating whether verbose information is requested
     */
    public boolean isVerbose() {
        return verbose;
    }

    /**
     * Get metric type to sort by
     */
    public MetricType getSortBy() {
        return sortBy;
    }

    /**
     * Get maximum result size
     */
    public int getSize() {
        return size;
    }

    /**
     * Get node IDs to filter by
     * @return array of node IDs
     */
    public String[] nodesIds() {
        return nodeIds;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(verbose);
        MetricType.writeTo(out, sortBy);
        out.writeInt(size);
        out.writeStringArray(nodeIds);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
