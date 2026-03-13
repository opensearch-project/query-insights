/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.top_queries;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.recommendations.Recommendation;

/**
 * Holds all top queries records by resource usage or latency on a node
 * Mainly used in the top N queries node response workflow.
 */
public class TopQueries extends BaseNodeResponse implements ToXContentObject {
    /** The store to keep the top queries records */
    private final List<SearchQueryRecord> topQueriesRecords;

    /** Pre-computed recommendations keyed by record ID */
    private final Map<String, List<Recommendation>> recommendations;

    /**
     * Create the TopQueries Object from StreamInput
     * @param in A {@link StreamInput} object.
     * @throws IOException IOException
     */
    public TopQueries(final StreamInput in) throws IOException {
        super(in);
        topQueriesRecords = in.readList(SearchQueryRecord::new);
        if (in.getVersion().onOrAfter(Version.V_3_1_0)) {
            int mapSize = in.readVInt();
            if (mapSize > 0) {
                recommendations = new HashMap<>(mapSize);
                for (int i = 0; i < mapSize; i++) {
                    String key = in.readString();
                    List<Recommendation> recs = in.readList(Recommendation::new);
                    recommendations.put(key, recs);
                }
            } else {
                recommendations = Collections.emptyMap();
            }
        } else {
            recommendations = Collections.emptyMap();
        }
    }

    /**
     * Create the TopQueries Object
     * @param node A node that is part of the cluster.
     * @param searchQueryRecords A list of SearchQueryRecord associated in this TopQueries.
     */
    public TopQueries(final DiscoveryNode node, final List<SearchQueryRecord> searchQueryRecords) {
        this(node, searchQueryRecords, Collections.emptyMap());
    }

    /**
     * Create the TopQueries Object with pre-computed recommendations
     * @param node A node that is part of the cluster.
     * @param searchQueryRecords A list of SearchQueryRecord associated in this TopQueries.
     * @param recommendations Pre-computed recommendations keyed by record ID.
     */
    public TopQueries(
        final DiscoveryNode node,
        final List<SearchQueryRecord> searchQueryRecords,
        final Map<String, List<Recommendation>> recommendations
    ) {
        super(node);
        topQueriesRecords = searchQueryRecords;
        this.recommendations = recommendations != null ? recommendations : Collections.emptyMap();
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        if (topQueriesRecords != null) {
            for (SearchQueryRecord record : topQueriesRecords) {
                record.toXContent(builder, params);
            }
        }
        return builder;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(topQueriesRecords);
        if (out.getVersion().onOrAfter(Version.V_3_1_0)) {
            out.writeVInt(recommendations.size());
            for (Map.Entry<String, List<Recommendation>> entry : recommendations.entrySet()) {
                out.writeString(entry.getKey());
                out.writeList(entry.getValue());
            }
        }
    }

    /**
     * Get all top queries records
     *
     * @return the top queries records in this node response
     */
    public List<SearchQueryRecord> getTopQueriesRecord() {
        return topQueriesRecords;
    }

    /**
     * Get pre-computed recommendations keyed by record ID
     *
     * @return the recommendations map
     */
    public Map<String, List<Recommendation>> getRecommendations() {
        return recommendations;
    }
}
