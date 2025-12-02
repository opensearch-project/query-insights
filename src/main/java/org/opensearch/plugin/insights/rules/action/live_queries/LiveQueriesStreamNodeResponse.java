/*
 * SPDX-License-Identifier: Apache-2.0
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
 * Node response for streaming live queries
 */
public class LiveQueriesStreamNodeResponse extends BaseNodeResponse {

    private final List<SearchQueryRecord> queries;

    public LiveQueriesStreamNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.queries = in.readList(SearchQueryRecord::new);
    }

    public LiveQueriesStreamNodeResponse(DiscoveryNode node, List<SearchQueryRecord> queries) {
        super(node);
        this.queries = queries;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(queries);
    }

    public List<SearchQueryRecord> getQueries() {
        return queries;
    }
}