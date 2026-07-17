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

/**
 * Per-node response carrying resolved live query user info (task key to identity) from one node.
 */
public class LiveQueriesUserInfoNodeResponse extends BaseNodeResponse {

    private final Map<String, LiveQueryUserInfoDTO> userInfoByTaskKey;

    public LiveQueriesUserInfoNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.userInfoByTaskKey = in.getVersion().onOrAfter(Version.V_3_8_0)
            ? in.readMap(StreamInput::readString, LiveQueryUserInfoDTO::new)
            : Map.of();
    }

    public LiveQueriesUserInfoNodeResponse(DiscoveryNode node, Map<String, LiveQueryUserInfoDTO> userInfoByTaskKey) {
        super(node);
        this.userInfoByTaskKey = userInfoByTaskKey != null ? userInfoByTaskKey : Map.of();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_3_8_0)) {
            out.writeMap(userInfoByTaskKey, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        }
    }

    public Map<String, LiveQueryUserInfoDTO> getUserInfoByTaskKey() {
        return userInfoByTaskKey;
    }
}
