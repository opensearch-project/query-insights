/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.rules.model.FinishedSearches;

/**
 * Aggregated response with resolved live query user info and PPL queries' finished searches from all nodes.
 */
public class LiveQueriesUserInfoResponse extends BaseNodesResponse<LiveQueriesUserInfoNodeResponse> {

    public LiveQueriesUserInfoResponse(StreamInput in) throws IOException {
        super(in);
    }

    public LiveQueriesUserInfoResponse(
        ClusterName clusterName,
        List<LiveQueriesUserInfoNodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<LiveQueriesUserInfoNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(LiveQueriesUserInfoNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<LiveQueriesUserInfoNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    /**
     * Merge all per-node maps into a single task-key to user-info map.
     * Each task key is owned by exactly one node, so there are no cross-node collisions.
     *
     * @return the merged map of task key to user info
     */
    public Map<String, LiveQueryUserInfoDTO> getAllUserInfo() {
        Map<String, LiveQueryUserInfoDTO> merged = new HashMap<>();
        for (LiveQueriesUserInfoNodeResponse nodeResponse : getNodes()) {
            merged.putAll(nodeResponse.getUserInfoByTaskKey());
        }
        return merged;
    }

    /**
     * Merges the per-node maps. A PPL query's searches are all coordinated on its own node, so no id
     * comes from two nodes.
     *
     * @return finished searches by live query id
     */
    public Map<String, FinishedSearches> getAllFinishedSearches() {
        Map<String, FinishedSearches> merged = new HashMap<>();
        for (LiveQueriesUserInfoNodeResponse nodeResponse : getNodes()) {
            merged.putAll(nodeResponse.getFinishedSearchesByQueryId());
        }
        return merged;
    }
}
