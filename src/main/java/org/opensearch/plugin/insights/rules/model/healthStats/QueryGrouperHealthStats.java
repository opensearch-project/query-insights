/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.healthStats;

import java.io.IOException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Represents the health statistics of the query grouper.
 */
public class QueryGrouperHealthStats implements ToXContentFragment, Writeable {
    private final int queryGroupCount;
    private final int queryGroupHeapSize;
    private static final String QUERY_GROUP_COUNT = "QueryGroupCount";
    private static final String QUERY_GROUP_HEAP_SIZE = "QueryGroupHeapSize";

    /**
     * Constructor to read QueryGrouperHealthStats from a StreamInput.
     *
     * @param in the StreamInput to read the QueryGrouperHealthStats from
     * @throws IOException IOException
     */
    public QueryGrouperHealthStats(final StreamInput in) throws IOException {
        this.queryGroupCount = in.readInt();
        this.queryGroupHeapSize = in.readInt();
    }

    /**
     * Constructor of QueryGrouperHealthStats
     *
     * @param queryGroupCount Number of groups in the grouper
     * @param queryGroupHeapSize Heap size of the grouper
     */
    public QueryGrouperHealthStats(final int queryGroupCount, final int queryGroupHeapSize) {
        this.queryGroupCount = queryGroupCount;
        this.queryGroupHeapSize = queryGroupHeapSize;
    }

    /**
     * Write QueryGrouperHealthStats Object to output stream
     * @param out streamOutput
     * @throws IOException IOException
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(queryGroupCount);
        out.writeInt(queryGroupHeapSize);
    }

    /**
     * Write QueryGrouperHealthStats object to XContent
     *
     * @param builder XContentBuilder
     * @param params Parameters
     * @return XContentBuilder
     * @throws IOException IOException
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(QUERY_GROUP_COUNT, queryGroupCount);
        builder.field(QUERY_GROUP_HEAP_SIZE, queryGroupHeapSize);
        return builder;
    }

    /**
     * Gets the number of query groups.
     *
     * @return the query group count
     */
    public int getQueryGroupCount() {
        return queryGroupCount;
    }

    /**
     * Gets the query group heap size.
     *
     * @return the query group heap size
     */
    public int getQueryGroupHeapSize() {
        return queryGroupHeapSize;
    }
}
