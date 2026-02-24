/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * A wrapper class for source strings to enable version-specific serialization
 */
public class SourceString implements ToXContent {
    private final String value;

    public SourceString(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(value);
    }

    /**
     * Parse a source JSON string into a SearchSourceBuilder.
     *
     * @param sourceStr the source JSON string
     * @param xContentRegistry the registry for named xcontent (use {@link NamedXContentRegistry#EMPTY} if unavailable)
     * @return the parsed SearchSourceBuilder, or null if the input is null/empty or parsing fails
     */
    public static SearchSourceBuilder toSearchSourceBuilder(String sourceStr, NamedXContentRegistry xContentRegistry) {
        if (sourceStr == null || sourceStr.isEmpty()) {
            return null;
        }
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, sourceStr)
        ) {
            return SearchSourceBuilder.fromXContent(parser, false);
        } catch (IOException e) {
            return null;
        }
    }
}
