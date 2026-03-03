/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.opensearch.Version;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.LiveQueryRecord;

/**
 * Transport response for cluster/node level live queries information.
 */
public class LiveQueriesResponse extends ActionResponse implements ToXContentObject {

    private static final String CLUSTER_LEVEL_RESULTS_KEY = "live_queries";
    private final List<LiveQueryRecord> liveQueries;

    /**
     * Constructor for LiveQueriesResponse.
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public LiveQueriesResponse(final StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_3_6_0)) {
            this.liveQueries = in.readList(LiveQueryRecord::new);
        } else {
            this.liveQueries = Collections.emptyList();
        }
    }

    /**
     * Constructor for LiveQueriesResponse
     *
     * @param liveQueries A flat list containing live queries results from relevant nodes
     */
    public LiveQueriesResponse(final List<LiveQueryRecord> liveQueries) {
        this.liveQueries = liveQueries;
    }

    /**
     * Get the live queries list
     * @return the list of live query records
     */
    public List<LiveQueryRecord> getLiveQueries() {
        return liveQueries;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_3_6_0)) {
            out.writeList(liveQueries);
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.startArray(CLUSTER_LEVEL_RESULTS_KEY);
        for (LiveQueryRecord query : liveQueries) {
            query.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
