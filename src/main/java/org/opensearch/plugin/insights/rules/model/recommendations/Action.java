/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.recommendations;

import java.io.IOException;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Represents a recommended action to take
 */
public class Action implements ToXContentObject, Writeable {
    private final String name;
    private final String hint;
    private final String documentationUrl;
    private final String suggestedQuery;

    private Action(Builder builder) {
        this.name = builder.name;
        this.hint = builder.hint;
        this.documentationUrl = builder.documentationUrl;
        this.suggestedQuery = builder.suggestedQuery;
    }

    /**
     * Create an Action from a StreamInput
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public Action(StreamInput in) throws IOException {
        this.name = in.readOptionalString();
        this.hint = in.readOptionalString();
        this.documentationUrl = in.readOptionalString();
        this.suggestedQuery = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        out.writeOptionalString(hint);
        out.writeOptionalString(documentationUrl);
        out.writeOptionalString(suggestedQuery);
    }

    /**
     * @return the action name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the hint describing the action
     */
    public String getHint() {
        return hint;
    }

    /**
     * @return the documentation URL
     */
    public String getDocumentationUrl() {
        return documentationUrl;
    }

    /**
     * @return the suggested query as a full search body JSON string, ready to POST to _search
     */
    public String getSuggestedQuery() {
        return suggestedQuery;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (name != null) {
            builder.field("name", name);
        }
        if (hint != null) {
            builder.field("hint", hint);
        }
        if (documentationUrl != null) {
            builder.field("documentation_url", documentationUrl);
        }
        if (suggestedQuery != null) {
            builder.field("suggested_query", suggestedQuery);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Action action = (Action) o;
        return Objects.equals(name, action.name)
            && Objects.equals(hint, action.hint)
            && Objects.equals(documentationUrl, action.documentationUrl)
            && Objects.equals(suggestedQuery, action.suggestedQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, hint, documentationUrl, suggestedQuery);
    }

    /**
     * Builder for Action
     */
    public static class Builder {
        private String name;
        private String hint;
        private String documentationUrl;
        private String suggestedQuery;

        /**
         * Set the action name
         * @param name the name
         * @return this builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Set the hint
         * @param hint the hint
         * @return this builder
         */
        public Builder hint(String hint) {
            this.hint = hint;
            return this;
        }

        /**
         * Set the documentation URL
         * @param documentationUrl the URL
         * @return this builder
         */
        public Builder documentationUrl(String documentationUrl) {
            this.documentationUrl = documentationUrl;
            return this;
        }

        /**
         * Set the suggested query (full search body JSON, ready to POST to _search)
         * @param suggestedQuery the suggested query JSON string
         * @return this builder
         */
        public Builder suggestedQuery(String suggestedQuery) {
            this.suggestedQuery = suggestedQuery;
            return this;
        }

        /**
         * Build the Action
         * @return the action
         */
        public Action build() {
            return new Action(this);
        }
    }

    /**
     * Create a new builder
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }
}
