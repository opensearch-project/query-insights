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
import org.opensearch.plugin.insights.rules.model.LiveQueryRecord;

/**
 * Transport response for cluster/node level live queries information.
 */
public class LiveQueriesResponse extends BaseNodesResponse<LiveQueriesNodeResponse> implements ToXContentObject {

    private static final String CLUSTER_LEVEL_RESULTS_KEY = "live_queries";
    private List<LiveQueryRecord> liveQueries;

    public LiveQueriesResponse(final StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructor for LiveQueriesResponse
     *
     * @param clusterName cluster name
     * @param liveQueries aggregated live query records
     * @param failures failed node responses
     */
    public LiveQueriesResponse(ClusterName clusterName, List<LiveQueryRecord> liveQueries, List<FailedNodeException> failures) {
        super(clusterName, new ArrayList<>(), failures);
        this.liveQueries = liveQueries;
    }

    /**
     * Get the live queries list from all nodes
     * @return the list of live query records
     */
    public List<LiveQueryRecord> getLiveQueries() {
        return liveQueries;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.startArray(CLUSTER_LEVEL_RESULTS_KEY);

        for (LiveQueryRecord query : liveQueries) {
            query.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<LiveQueriesNodeResponse> nodes) throws IOException {
        out.writeList(liveQueries);
    }

    @Override
    protected List<LiveQueriesNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        this.liveQueries = in.readList(LiveQueryRecord::new);
        return new ArrayList<>();
    }
}
