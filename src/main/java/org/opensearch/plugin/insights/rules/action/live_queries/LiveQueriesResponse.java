/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Transport response for cluster/node level live queries information.
 */
public class LiveQueriesResponse extends BaseNodesResponse<LiveQueries> implements ToXContentFragment {

    private static final String CLUSTER_LEVEL_RESULTS_KEY = "live_queries";

    /**
     * Constructor for LiveQueriesResponse.
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public LiveQueriesResponse(final StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructor for LiveQueriesResponse
     *
     * @param clusterName The current cluster name
     * @param nodes A list that contains live queries results from all nodes
     * @param failures A list that contains FailedNodeException
     * @param verbose Whether verbose query information was requested (used by constructor, but not stored)
     */
    public LiveQueriesResponse(
        final ClusterName clusterName,
        final List<LiveQueries> nodes,
        final List<FailedNodeException> failures,
        final boolean verbose
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<LiveQueries> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(LiveQueries::new);
    }

    @Override
    protected void writeNodesTo(final StreamOutput out, final List<LiveQueries> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        final List<LiveQueries> results = getNodes();
        builder.startObject();
        toClusterLevelResult(builder, params, results);
        return builder.endObject();
    }

    /**
     * Merge live queries results from nodes into cluster level results in XContent format.
     *
     * @param builder XContent builder
     * @param params serialization parameters
     * @param results live queries results from all nodes
     * @throws IOException if an error occurs
     */
    private void toClusterLevelResult(final XContentBuilder builder, final Params params, final List<LiveQueries> results)
        throws IOException {
        final List<SearchQueryRecord> allQueries = results.stream()
            .map(LiveQueries::getLiveQueries)
            .flatMap(Collection::stream)
            .sorted((a, b) -> SearchQueryRecord.compare(a, b, MetricType.LATENCY) * -1)
            .toList();

        builder.startArray(CLUSTER_LEVEL_RESULTS_KEY);
        for (SearchQueryRecord query : allQueries) {
            query.toXContent(builder, params);
        }
        builder.endArray();
    }
}
