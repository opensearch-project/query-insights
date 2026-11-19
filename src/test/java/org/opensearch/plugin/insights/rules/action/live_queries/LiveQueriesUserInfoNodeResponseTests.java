/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.insights.rules.model.FinishedSearches;
import org.opensearch.test.OpenSearchTestCase;

/** Serialization of a node's live queries response: user info and PPL queries' finished searches. */
public class LiveQueriesUserInfoNodeResponseTests extends OpenSearchTestCase {

    private final DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

    private LiveQueriesUserInfoNodeResponse response() {
        return new LiveQueriesUserInfoNodeResponse(
            node,
            Map.of("node1:11", new LiveQueryUserInfoDTO("alice", List.of("user_role"), List.of("backend1"))),
            Map.of("node1:10", new FinishedSearches(604, 7_000_000L, 9_000L))
        );
    }

    private static LiveQueriesUserInfoNodeResponse roundTrip(LiveQueriesUserInfoNodeResponse response, Version version) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        response.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        return new LiveQueriesUserInfoNodeResponse(in);
    }

    public void testFinishedSearchesTravelWithTheUserInfo() throws IOException {
        LiveQueriesUserInfoNodeResponse read = roundTrip(response(), Version.CURRENT);

        assertEquals("alice", read.getUserInfoByTaskKey().get("node1:11").getUsername());
        assertEquals(Map.of("node1:10", new FinishedSearches(604, 7_000_000L, 9_000L)), read.getFinishedSearchesByQueryId());
    }

    public void testNodesBefore3_10GetOnlyTheUserInfo() throws IOException {
        LiveQueriesUserInfoNodeResponse read = roundTrip(response(), Version.V_3_9_0);

        assertEquals("alice", read.getUserInfoByTaskKey().get("node1:11").getUsername());
        assertTrue(read.getFinishedSearchesByQueryId().isEmpty());
    }

    public void testAResponseWithoutFinishedSearchesHasNone() {
        assertTrue(new LiveQueriesUserInfoNodeResponse(node, Map.of()).getFinishedSearchesByQueryId().isEmpty());
        assertTrue(new LiveQueriesUserInfoNodeResponse(node, null, null).getFinishedSearchesByQueryId().isEmpty());
    }

    public void testTheResponseMergesEveryNodesFinishedSearches() {
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        LiveQueriesUserInfoResponse merged = new LiveQueriesUserInfoResponse(
            new ClusterName("test-cluster"),
            List.of(
                response(),
                new LiveQueriesUserInfoNodeResponse(node2, Map.of(), Map.of("node2:7", new FinishedSearches(3, 30L, 300L)))
            ),
            List.of()
        );

        assertEquals(
            Map.of("node1:10", new FinishedSearches(604, 7_000_000L, 9_000L), "node2:7", new FinishedSearches(3, 30L, 300L)),
            merged.getAllFinishedSearches()
        );
    }
}
