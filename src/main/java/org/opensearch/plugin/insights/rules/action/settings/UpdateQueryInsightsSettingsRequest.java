/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.settings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request to update Query Insights settings.
 */
public class UpdateQueryInsightsSettingsRequest extends ActionRequest {

    private final Map<String, Object> settings;

    /**
     * Constructor for UpdateQueryInsightsSettingsRequest
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public UpdateQueryInsightsSettingsRequest(final StreamInput in) throws IOException {
        super(in);
        this.settings = in.readMap();
    }

    /**
     * Constructor for UpdateQueryInsightsSettingsRequest
     */
    public UpdateQueryInsightsSettingsRequest() {
        this.settings = new HashMap<>();
    }

    /**
     * Constructor for UpdateQueryInsightsSettingsRequest
     *
     * @param settings Map of settings to update
     */
    public UpdateQueryInsightsSettingsRequest(final Map<String, Object> settings) {
        this.settings = settings != null ? settings : new HashMap<>();
    }

    /**
     * Get the settings to update
     * @return Map of settings
     */
    public Map<String, Object> getSettings() {
        return settings;
    }

    /**
     * Set a setting value
     * @param key Setting key
     * @param value Setting value
     */
    public void setSetting(String key, Object value) {
        this.settings.put(key, value);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(settings);
    }
}
