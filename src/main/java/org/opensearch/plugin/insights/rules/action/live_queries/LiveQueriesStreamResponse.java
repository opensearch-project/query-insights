/*
 * SPDX-License-Identifier: Apache-2.0
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
 * Response for streaming live queries
 */
public class LiveQueriesStreamResponse extends ActionResponse implements ToXContentObject {

    private final List<SearchQueryRecord> queries;

    public LiveQueriesStreamResponse(StreamInput in) throws IOException {
        super(in);
        this.queries = in.readList(SearchQueryRecord::new);
    }

    public LiveQueriesStreamResponse(List<SearchQueryRecord> queries) {
        this.queries = queries;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(queries);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("queries");
        for (SearchQueryRecord query : queries) {
            query.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

    public List<SearchQueryRecord> getQueries() {
        return queries;
    }
}