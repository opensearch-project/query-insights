/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * A request to get cluster/node level ongoing live queries information.
 */
public class LiveQueriesRequest extends BaseNodesRequest<LiveQueriesRequest> {

    private final boolean detailed;

    /**
     * Constructor for LiveQueriesRequest
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public LiveQueriesRequest(final StreamInput in) throws IOException {
        super(in);
        this.detailed = in.readBoolean();
    }

    /**
     * Get live queries from nodes based on the nodes ids specified.
     * If none are passed, cluster level live queries will be returned.
     *
     * @param detailed Whether to include detailed information about live queries
     * @param nodesIds The nodeIds specified in the request
     */
    public LiveQueriesRequest(final boolean detailed, final String... nodesIds) {
        super(nodesIds);
        this.detailed = detailed;
    }

    /**
     * Get whether detailed information is requested
     * @return boolean indicating whether detailed information is requested
     */
    public boolean isDetailed() {
        return detailed;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(detailed);
    }
} 