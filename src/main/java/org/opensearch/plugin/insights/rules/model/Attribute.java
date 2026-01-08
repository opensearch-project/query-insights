/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.Version;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Valid attributes for a search query record
 */
public enum Attribute {
    /**
     * The search query type
     */
    SEARCH_TYPE,
    /**
     * The search query source
     */
    SOURCE,
    /**
     * Total shards queried
     */
    TOTAL_SHARDS,
    /**
     * The indices involved
     */
    INDICES,
    /**
     * The per phase level latency map for a search query
     */
    PHASE_LATENCY_MAP,
    /**
     * The node id for this request
     */
    NODE_ID,
    /**
     * Tasks level resource usages in this request
     */
    TASK_RESOURCE_USAGES,
    /**
     * Custom search request labels
     */
    LABELS,
    /**
     * Query Group hashcode
     */
    QUERY_GROUP_HASHCODE,
    /**
     * Grouping type of the query record (none, similarity)
     */
    GROUP_BY,
    /**
     * The description of the search query, often used in live queries.
     */
    DESCRIPTION,
    /**
     * A map indicating for which metric type(s) this record was in the Top N
     */
    TOP_N_QUERY,

    /**
     * The WLM query group ID associated with the query.
     */
    WLM_GROUP_ID,

    /**
     * The cancelled of the search query, often used in live queries.
     */
    IS_CANCELLED,

    /**
     * The username who initiated the search query.
     */
    USERNAME,

    /**
     * The roles of the user who initiated the search query.
     */
    USER_ROLES,

    /**
     * The task ID associated with the search query.
     */
    TASK_ID,

    /**
     * Indicates if the source was truncated due to length limits.
     */
    SOURCE_TRUNCATED;

    /**
     * Read an Attribute from a StreamInput
     *
     * @param in the StreamInput to read from
     * @return Attribute
     * @throws IOException IOException
     */
    static Attribute readFromStream(final StreamInput in) throws IOException {
        return Attribute.valueOf(in.readString().toUpperCase(Locale.ROOT));
    }

    /**
     * Write Attribute to a StreamOutput
     *
     * @param out       the StreamOutput to write
     * @param attribute the Attribute to write
     * @throws IOException IOException
     */
    static void writeTo(final StreamOutput out, final Attribute attribute) throws IOException {
        out.writeString(attribute.toString());
    }

    /**
     * Write Attribute value to a StreamOutput
     *
     * @param out            the StreamOutput to write
     * @param attributeValue the Attribute value to write
     * @throws IOException exception
     */
    @SuppressWarnings("unchecked")
    public static void writeValueTo(StreamOutput out, Object attributeValue) throws IOException {
        if (attributeValue instanceof List) {
            out.writeList((List<? extends Writeable>) attributeValue);
        } else if (attributeValue instanceof SourceString) {
            if (out.getVersion().onOrAfter(Version.V_3_5_0)) {
                out.writeString(((SourceString) attributeValue).getValue());
            } else {
                // Convert source to SearchSourceBuilder and write to stream
                try {
                    // Attempt to convert source to SearchSourceBuilder
                    String sourceStr = ((SourceString) attributeValue).getValue();
                    if (sourceStr != null && !sourceStr.isEmpty()) {
                        XContentParser parser = XContentType.JSON.xContent()
                            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, sourceStr);
                        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser, false);
                        searchSourceBuilder.writeTo(out);
                        parser.close();
                    } else {
                        new SearchSourceBuilder().writeTo(out);
                    }
                } catch (Exception e) {
                    // Unable to convert source to SearchSourceBuilder, sending dummy object instead
                    new SearchSourceBuilder().writeTo(out);
                }
            }
        } else if (attributeValue instanceof SearchSourceBuilder) {
            ((SearchSourceBuilder) attributeValue).writeTo(out);
        } else if (attributeValue instanceof GroupingType) {
            out.writeString(((GroupingType) attributeValue).getValue());
        } else {
            out.writeGenericValue(attributeValue);
        }
    }

    /**
     * Read attribute value from the input stream given the Attribute type
     *
     * @param in        the {@link StreamInput} input to read
     * @param attribute attribute type to differentiate between Source and others
     * @return parse value
     * @throws IOException IOException
     */
    public static Object readAttributeValue(StreamInput in, Attribute attribute) throws IOException {
        if (attribute == Attribute.TASK_RESOURCE_USAGES) {
            return in.readList(TaskResourceInfo::readFromStream);
        } else if (attribute == Attribute.SOURCE) {
            if (in.getVersion().onOrAfter(Version.V_3_5_0)) {
                return new SourceString(in.readString());
            } else {
                return new SourceString(new SearchSourceBuilder(in).toString());
            }
        } else if (attribute == Attribute.GROUP_BY) {
            return GroupingType.valueOf(in.readString().toUpperCase(Locale.ROOT));
        } else {
            return in.readGenericValue();
        }
    }

    /**
     * Read attribute map from the input stream
     *
     * @param in the {@link StreamInput} to read
     * @return parsed attribute map
     * @throws IOException IOException
     */
    public static Map<Attribute, Object> readAttributeMap(StreamInput in) throws IOException {
        int size = readArraySize(in);
        if (size == 0) {
            return Collections.emptyMap();
        }
        Map<Attribute, Object> map = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            Attribute key = readFromStream(in);
            Object value = readAttributeValue(in, key);
            map.put(key, value);
        }
        return map;
    }

    private static int readArraySize(StreamInput in) throws IOException {
        final int arraySize = in.readVInt();
        if (arraySize > ArrayUtil.MAX_ARRAY_LENGTH) {
            throw new IllegalStateException("array length must be <= to " + ArrayUtil.MAX_ARRAY_LENGTH + " but was: " + arraySize);
        }
        if (arraySize < 0) {
            throw new NegativeArraySizeException("array size must be positive but was: " + arraySize);
        }
        return arraySize;
    }

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
