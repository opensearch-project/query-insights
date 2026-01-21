/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.settings;

import java.io.IOException;
import java.util.Map;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response for getting Query Insights settings.
 */
public class GetQueryInsightsSettingsResponse extends ActionResponse implements ToXContentObject {

    private final Map<String, Object> settings;

    /**
     * Constructor for GetQueryInsightsSettingsResponse
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public GetQueryInsightsSettingsResponse(final StreamInput in) throws IOException {
        super(in);
        this.settings = in.readMap();
    }

    /**
     * Constructor for GetQueryInsightsSettingsResponse
     *
     * @param settings Map of Query Insights settings
     */
    public GetQueryInsightsSettingsResponse(final Map<String, Object> settings) {
        this.settings = settings;
    }

    /**
     * Get the settings map
     * @return Map of Query Insights settings
     */
    public Map<String, Object> getSettings() {
        return settings;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeMap(settings);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();

        // Group settings by category
        builder.startObject("persistent");

        // Latency settings
        if (shouldIncludeMetric("latency", settings)) {
            builder.startObject("latency");
            addIfPresent(builder, settings, "latency.enabled");
            addIfPresent(builder, settings, "latency.top_n_size");
            addIfPresent(builder, settings, "latency.window_size");
            builder.endObject();
        }

        // CPU settings
        if (shouldIncludeMetric("cpu", settings)) {
            builder.startObject("cpu");
            addIfPresent(builder, settings, "cpu.enabled");
            addIfPresent(builder, settings, "cpu.top_n_size");
            addIfPresent(builder, settings, "cpu.window_size");
            builder.endObject();
        }

        // Memory settings
        if (shouldIncludeMetric("memory", settings)) {
            builder.startObject("memory");
            addIfPresent(builder, settings, "memory.enabled");
            addIfPresent(builder, settings, "memory.top_n_size");
            addIfPresent(builder, settings, "memory.window_size");
            builder.endObject();
        }

        // Grouping settings
        if (settings.containsKey("grouping.group_by")) {
            builder.startObject("grouping");
            addIfPresent(builder, settings, "grouping.group_by");
            builder.endObject();
        }

        // Exporter settings
        if (settings.containsKey("exporter.type") || settings.containsKey("exporter.delete_after_days")) {
            builder.startObject("exporter");
            addIfPresent(builder, settings, "exporter.type");
            addIfPresent(builder, settings, "exporter.delete_after_days");
            builder.endObject();
        }

        builder.endObject(); // persistent
        builder.endObject();
        return builder;
    }

    private boolean shouldIncludeMetric(String metric, Map<String, Object> settings) {
        return settings.containsKey(metric + ".enabled")
            || settings.containsKey(metric + ".top_n_size")
            || settings.containsKey(metric + ".window_size");
    }

    private void addIfPresent(XContentBuilder builder, Map<String, Object> settings, String key) throws IOException {
        if (settings.containsKey(key)) {
            String fieldName = key.substring(key.lastIndexOf('.') + 1);
            builder.field(fieldName, settings.get(key));
        }
    }
}
