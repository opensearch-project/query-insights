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
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.rules.model.TaskRecord;

/**
 * A response from a single node containing live queries information
 */
public class LiveQueriesNodeResponse extends BaseNodeResponse {

    private List<TaskRecord> tasks;

    public LiveQueriesNodeResponse(DiscoveryNode node, List<TaskRecord> tasks) {
        super(node);
        this.tasks = tasks;
    }

    public LiveQueriesNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.tasks = in.readList(TaskRecord::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(tasks);
    }

    public List<TaskRecord> getTasks() {
        return tasks;
    }
}
