/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.Map;
import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.rules.model.FinishedSearches;

/**
 * Per-node response with resolved live query user info (task key to identity) and the searches each
 * running PPL query has finished (live id to count and cost).
 */
public class LiveQueriesUserInfoNodeResponse extends BaseNodeResponse {

    private final Map<String, LiveQueryUserInfoDTO> userInfoByTaskKey;
    private final Map<String, FinishedSearches> finishedSearchesByQueryId;

    public LiveQueriesUserInfoNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.userInfoByTaskKey = in.getVersion().onOrAfter(Version.V_3_8_0)
            ? in.readMap(StreamInput::readString, LiveQueryUserInfoDTO::new)
            : Map.of();
        this.finishedSearchesByQueryId = in.getVersion().onOrAfter(Version.V_3_10_0)
            ? in.readMap(StreamInput::readString, FinishedSearches::new)
            : Map.of();
    }

    public LiveQueriesUserInfoNodeResponse(DiscoveryNode node, Map<String, LiveQueryUserInfoDTO> userInfoByTaskKey) {
        this(node, userInfoByTaskKey, Map.of());
    }

    public LiveQueriesUserInfoNodeResponse(
        DiscoveryNode node,
        Map<String, LiveQueryUserInfoDTO> userInfoByTaskKey,
        Map<String, FinishedSearches> finishedSearchesByQueryId
    ) {
        super(node);
        this.userInfoByTaskKey = userInfoByTaskKey != null ? userInfoByTaskKey : Map.of();
        this.finishedSearchesByQueryId = finishedSearchesByQueryId != null ? finishedSearchesByQueryId : Map.of();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_3_8_0)) {
            out.writeMap(userInfoByTaskKey, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        }
        if (out.getVersion().onOrAfter(Version.V_3_10_0)) {
            out.writeMap(finishedSearchesByQueryId, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        }
    }

    public Map<String, LiveQueryUserInfoDTO> getUserInfoByTaskKey() {
        return userInfoByTaskKey;
    }

    /** @return the finished searches of each running PPL query on this node, by live id */
    public Map<String, FinishedSearches> getFinishedSearchesByQueryId() {
        return finishedSearchesByQueryId;
    }
}
