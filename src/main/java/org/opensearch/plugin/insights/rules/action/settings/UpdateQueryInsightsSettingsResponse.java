/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.settings;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response for updating Query Insights settings.
 */
public class UpdateQueryInsightsSettingsResponse extends ActionResponse implements ToXContentObject {

    private final boolean acknowledged;

    /**
     * Constructor for UpdateQueryInsightsSettingsResponse
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public UpdateQueryInsightsSettingsResponse(final StreamInput in) throws IOException {
        super(in);
        this.acknowledged = in.readBoolean();
    }

    /**
     * Constructor for UpdateQueryInsightsSettingsResponse
     *
     * @param acknowledged Whether the update was acknowledged
     */
    public UpdateQueryInsightsSettingsResponse(final boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    /**
     * Check if the update was acknowledged
     * @return true if acknowledged, false otherwise
     */
    public boolean isAcknowledged() {
        return acknowledged;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("acknowledged", acknowledged);
        builder.endObject();
        return builder;
    }
}
