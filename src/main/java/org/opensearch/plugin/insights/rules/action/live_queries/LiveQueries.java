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
import java.util.Objects;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Node level live queries response.
 */
public class LiveQueries extends BaseNodeResponse implements ToXContentFragment {

    private final List<SearchQueryRecord> liveQueries;

    /**
     * Constructor for the node response.
     *
     * @param node the node this response is from
     * @param liveQueries the node's live queries
     */
    public LiveQueries(final DiscoveryNode node, final List<SearchQueryRecord> liveQueries) {
        super(node);
        this.liveQueries = liveQueries;
    }

    /**
     * Constructor for the node response from a stream.
     *
     * @param in A {@link StreamInput} to read from
     * @throws IOException if the stream cannot be deserialized
     */
    public LiveQueries(final StreamInput in) throws IOException {
        super(in);
        liveQueries = in.readList(SearchQueryRecord::new);
    }

    /**
     * Get the live queries for the node.
     *
     * @return the live queries for the node
     */
    public List<SearchQueryRecord> getLiveQueries() {
        return liveQueries;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(liveQueries);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("node_id", getNode().getId());
        builder.field("node_name", getNode().getName());
        builder.startArray("live_queries");
        for (SearchQueryRecord query : liveQueries) {
            query.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        LiveQueries that = (LiveQueries) o;
        if (liveQueries.size() != that.liveQueries.size()) return false;
        for (int i = 0; i < liveQueries.size(); i++) {
            if (!liveQueries.get(i).equals(that.liveQueries.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), liveQueries);
    }
}
