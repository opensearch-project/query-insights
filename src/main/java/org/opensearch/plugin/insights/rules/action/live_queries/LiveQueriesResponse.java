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
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Transport response for cluster/node level live queries information.
 */
public class LiveQueriesResponse extends ActionResponse implements ToXContentObject {

    private static final String CLUSTER_LEVEL_RESULTS_KEY = "live_queries";
    private static final String COMPLETED_QUERIES_KEY = "finished_queries";
    private final List<SearchQueryRecord> liveQueries;
    private final List<SearchQueryRecord> finishedQueries;

    /**
     * Constructor for LiveQueriesResponse.
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public LiveQueriesResponse(final StreamInput in) throws IOException {
        this.liveQueries = in.readList(SearchQueryRecord::new);
        this.finishedQueries = in.readList(SearchQueryRecord::new);
    }

    /**
     * Constructor for LiveQueriesResponse
     *
     * @param liveQueries A flat list containing live queries results from relevant nodes
     */
    public LiveQueriesResponse(final List<SearchQueryRecord> liveQueries) {
        this.liveQueries = liveQueries;
        this.finishedQueries = List.of();
    }

    /**
     * Constructor for LiveQueriesResponse
     *
     * @param liveQueries A flat list containing live queries results from relevant nodes
     * @param finishedQueries A flat list containing finished queries from the last 30 seconds
     */
    public LiveQueriesResponse(final List<SearchQueryRecord> liveQueries, final List<SearchQueryRecord> finishedQueries) {
        this.liveQueries = liveQueries;
        this.finishedQueries = finishedQueries;
    }

    /**
     * Get the live queries list
     * @return the list of live query records
     */
    public List<SearchQueryRecord> getLiveQueries() {
        return liveQueries;
    }

    /**
     * Get the finished queries list
     * @return the list of finished query records
     */
    public List<SearchQueryRecord> getCompletedQueries() {
        return finishedQueries;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeList(liveQueries);
        out.writeList(finishedQueries);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.startArray(CLUSTER_LEVEL_RESULTS_KEY);

        for (SearchQueryRecord query : liveQueries) {
            query.toXContent(builder, params);
        }
        builder.endArray();

        builder.startArray(COMPLETED_QUERIES_KEY);
        for (SearchQueryRecord query : finishedQueries) {
            query.toXContent(builder, params);
        }
        builder.endArray();

        builder.endObject();
        return builder;
    }
}
