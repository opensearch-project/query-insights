/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.settings;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request to get Query Insights settings.
 */
public class GetQueryInsightsSettingsRequest extends ActionRequest {

    private final String metricType;

    /**
     * Constructor for GetQueryInsightsSettingsRequest
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public GetQueryInsightsSettingsRequest(final StreamInput in) throws IOException {
        super(in);
        this.metricType = in.readOptionalString();
    }

    /**
     * Constructor for GetQueryInsightsSettingsRequest
     *
     * @param metricType Optional metric type filter (e.g., "latency", "cpu", "memory")
     */
    public GetQueryInsightsSettingsRequest(final String metricType) {
        this.metricType = metricType;
    }

    /**
     * Get the metric type filter
     * @return metric type or null for all settings
     */
    public String getMetricType() {
        return metricType;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(metricType);
    }
}
