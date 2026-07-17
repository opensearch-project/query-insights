/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.List;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request to resolve user info for a set of live query task keys from all (or specific) nodes.
 * Each node resolves the keys it owns from its local map.
 */
public class LiveQueriesUserInfoRequest extends BaseNodesRequest<LiveQueriesUserInfoRequest> {

    private final List<String> taskKeys;

    public LiveQueriesUserInfoRequest(StreamInput in) throws IOException {
        super(in);
        this.taskKeys = in.readStringList();
    }

    public LiveQueriesUserInfoRequest(List<String> taskKeys, String... nodeIds) {
        super(nodeIds);
        this.taskKeys = taskKeys != null ? taskKeys : List.of();
    }

    public List<String> getTaskKeys() {
        return taskKeys;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(taskKeys);
    }
}
