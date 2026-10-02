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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.core.xcontent.NamedXContentRegistry;
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
     * The backend roles of the user who initiated the search query.
     */
    BACKEND_ROLES,

    /**
     * Indicates if the source was truncated due to length limits.
     */
    SOURCE_TRUNCATED,

    /**
     * Indicates if the search request failed during execution.
     */
    FAILED;

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
                // Convert source to SearchSourceBuilder
                String sourceStr = ((SourceString) attributeValue).getValue();
                SearchSourceBuilder ssb = SourceString.toSearchSourceBuilder(sourceStr, NamedXContentRegistry.EMPTY);
                if (ssb == null) {
                    ssb = new SearchSourceBuilder();
                }
                ssb.writeTo(out);
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
            final Attribute key;
            try {
                key = readFromStream(in);
            } catch (IllegalArgumentException e) {
                // The attribute name doesn't match any constant this node's Attribute enum
                // knows about. This happens during a rolling upgrade: a node running a newer
                // version can add new Attribute constants (e.g. USER_ROLES, BACKEND_ROLES) and
                // send them to an older-version coordinator whose enum predates them, and
                // Attribute.valueOf() throws IllegalArgumentException for the unknown name.
                //
                // We can't just let this exception propagate: readAttributeMap is reading a
                // fixed-size sequence of key/value pairs, so any entry we fail to fully consume
                // corrupts the byte alignment for every entry after it in this stream, not just
                // this one. Every attribute defined today other than the handful with bespoke
                // encodings (TASK_RESOURCE_USAGES, SOURCE, GROUP_BY -- all handled explicitly in
                // readAttributeValue below) is written through the self-describing
                // writeGenericValue()/readGenericValue() wire format, so we can read and discard
                // the value here to stay aligned, without needing to know what the attribute
                // means. This trades knowledge of the new attribute for correctness of everything
                // else in the record, which matches the documented expected behavior: "Query
                // insight should not fail, or at least give up on trying to collect this
                // particular event." See https://github.com/opensearch-project/query-insights/issues/510
                //
                // Caveat: writeValueTo() also special-cases java.util.List through the
                // Writeable-based out.writeList(), which is NOT self-describing. No attribute
                // uses a List value today (USER_ROLES/BACKEND_ROLES are String[]), but a future
                // attribute that does would need its own explicit case in readAttributeValue()
                // AND readAttributeMap() -- this generic skip cannot safely consume it.
                in.readGenericValue();
                continue;
            }
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
