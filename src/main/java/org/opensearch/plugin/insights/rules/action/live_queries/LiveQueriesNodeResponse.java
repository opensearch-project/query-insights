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
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * A response from a single node containing live queries information
 */
public class LiveQueriesNodeResponse extends BaseNodeResponse {

    private List<SearchQueryRecord> liveQueries;

    public LiveQueriesNodeResponse(DiscoveryNode node, List<SearchQueryRecord> liveQueries) {
        super(node);
        this.liveQueries = liveQueries;
    }

    public LiveQueriesNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.liveQueries = in.readList(SearchQueryRecord::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(liveQueries);
    }

    public List<SearchQueryRecord> getLiveQueries() {
        return liveQueries;
    }
}
