/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Transport response for cluster/node level live queries information.
 */
public class LiveQueriesResponse extends BaseNodesResponse<LiveQueriesNodeResponse> implements ToXContentObject {

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
     * @param clusterName cluster name
     * @param nodes successful node responses
     * @param failures failed node responses
     */
    public LiveQueriesResponse(ClusterName clusterName, List<LiveQueriesNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    private static final String CLUSTER_LEVEL_RESULTS_KEY = "live_queries";

    /**
     * Get the live queries list from all nodes
     * @return the list of live query records
     */
    public List<SearchQueryRecord> getLiveQueries() {
        List<SearchQueryRecord> allQueries = new ArrayList<>();
        for (LiveQueriesNodeResponse nodeResponse : getNodes()) {
            allQueries.addAll(nodeResponse.getLiveQueries());
        }
        return allQueries;
    }

    @Override
    protected List<LiveQueriesNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(LiveQueriesNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<LiveQueriesNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.startArray(CLUSTER_LEVEL_RESULTS_KEY);

        for (SearchQueryRecord query : getLiveQueries()) {
            query.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
