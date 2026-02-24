/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.Map;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Extends SearchQueryRecord with finished-query-specific fields: top_n_id and status.
 * top_n_id links to the corresponding Top N record (UUID from QueryInsightsListener).
 * id is set to liveQueryId (nodeId:taskId) for correlation with live queries.
 */
public class FinishedQueryRecord extends SearchQueryRecord {

    private final String topNId;
    private final String status;

    public FinishedQueryRecord(SearchQueryRecord base, String topNId, String status, String liveQueryId) {
        super(
            base.getTimestamp(),
            new java.util.HashMap<>(base.getMeasurements()),
            new java.util.HashMap<>(base.getAttributes()),
            base.getSearchSourceBuilder(),
            base.getUserPrincipalContext(),
            liveQueryId
        );
        this.topNId = topNId;
        this.status = status;
    }

    public FinishedQueryRecord(StreamInput in) throws IOException {
        super(in);
        this.topNId = in.readOptionalString();
        this.status = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(topNId);
        out.writeString(status);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("timestamp", getTimestamp());
        builder.field("id", getId());
        if (topNId != null) builder.field("top_n_id", topNId);
        builder.field("status", status);
        for (Map.Entry<Attribute, Object> entry : getAttributes().entrySet()) {
            if (entry.getKey() == Attribute.TOP_N_QUERY) continue;
            builder.field(entry.getKey().toString(), entry.getValue());
        }
        builder.startObject("measurements");
        for (Map.Entry<MetricType, Measurement> entry : getMeasurements().entrySet()) {
            builder.field(entry.getKey().toString());
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();
        return builder.endObject();
    }

    public String getTopNId() {
        return topNId;
    }

    public String getStatus() {
        return status;
    }
}
