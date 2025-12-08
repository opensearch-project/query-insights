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
import org.opensearch.plugin.insights.rules.model.CachedQueryRecord;

/**
 * Response for cached live queries with minimal data
 */
public class LiveQueriesCachedResponse extends ActionResponse implements ToXContentObject {

    private final List<CachedQueryRecord> queries;

    public LiveQueriesCachedResponse(StreamInput in) throws IOException {
        super(in);
        this.queries = in.readList(CachedQueryRecord::new);
    }

    public LiveQueriesCachedResponse(List<CachedQueryRecord> queries) {
        this.queries = queries;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(queries);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("live_queries");
        for (CachedQueryRecord query : queries) {
            query.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

    public List<CachedQueryRecord> getQueries() {
        return queries;
    }
}
