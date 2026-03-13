/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.recommendations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.recommendations.Recommendation;

/**
 * Response containing recommendations for a query
 */
public class AnalyzeQueryResponse extends ActionResponse implements ToXContentObject {
    private final List<Recommendation> recommendations;

    /**
     * Constructor
     * @param recommendations the list of recommendations
     */
    public AnalyzeQueryResponse(List<Recommendation> recommendations) {
        this.recommendations = recommendations != null ? recommendations : new ArrayList<>();
    }

    /**
     * Constructor from stream
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public AnalyzeQueryResponse(StreamInput in) throws IOException {
        super(in);
        this.recommendations = in.readList(Recommendation::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(recommendations);
    }

    /**
     * Get the recommendations
     * @return the list of recommendations
     */
    public List<Recommendation> getRecommendations() {
        return recommendations;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("count", recommendations.size());
        builder.startArray("recommendations");
        for (Recommendation recommendation : recommendations) {
            recommendation.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
