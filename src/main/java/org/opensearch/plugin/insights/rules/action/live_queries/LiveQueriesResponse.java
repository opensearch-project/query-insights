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
    private final List<SearchQueryRecord> liveQueries;
    private final List<SearchQueryRecord> finishedQueries;
    private final boolean useFinishedCache;

    /**
     * Constructor for LiveQueriesResponse.
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public LiveQueriesResponse(final StreamInput in) throws IOException {
        this.liveQueries = in.readList(SearchQueryRecord::new);
        this.useFinishedCache = in.readBoolean();
        this.finishedQueries = useFinishedCache ? in.readList(SearchQueryRecord::new) : List.of();
    }

    /**
     * Constructor for LiveQueriesResponse
     *
     * @param liveQueries A flat list containing live queries results from relevant nodes
     */
    public LiveQueriesResponse(final List<SearchQueryRecord> liveQueries) {
        this.liveQueries = liveQueries;
        this.finishedQueries = List.of();
        this.useFinishedCache = false;
    }

    /**
     * Constructor for LiveQueriesResponse with finished queries
     */
    public LiveQueriesResponse(
        final List<SearchQueryRecord> liveQueries,
        final List<SearchQueryRecord> finishedQueries,
        boolean useFinishedCache
    ) {
        this.liveQueries = liveQueries;
        this.finishedQueries = finishedQueries;
        this.useFinishedCache = useFinishedCache;
    }

    /**
     * Get the live queries list
     * @return the list of live query records
     */
    public List<SearchQueryRecord> getLiveQueries() {
        return liveQueries;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeList(liveQueries);
        out.writeBoolean(useFinishedCache);
        if (useFinishedCache) {
            out.writeList(finishedQueries);
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.startArray(CLUSTER_LEVEL_RESULTS_KEY);

        for (SearchQueryRecord query : liveQueries) {
            query.toXContent(builder, params);
        }
        builder.endArray();
        if (useFinishedCache) {
            builder.startArray("finished_queries");
            for (SearchQueryRecord query : finishedQueries) {
                query.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }
}
