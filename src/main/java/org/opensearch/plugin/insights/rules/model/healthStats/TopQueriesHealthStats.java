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
 *  Represents the health statistics of top queries, including heap sizes and query grouper health stats.
 */
public class TopQueriesHealthStats implements ToXContentFragment, Writeable {
    private final int topQueriesHeapSize;
    private final QueryGrouperHealthStats queryGrouperHealthStats;
    private static final String TOP_QUERIES_HEAP_SIZE = "TopQueriesHeapSize";

    /**
     * Constructor to read TopQueriesHealthStats from a StreamInput.
     *
     * @param in the StreamInput to read the TopQueriesHealthStats from
     * @throws IOException if an I/O error occurs
     */
    public TopQueriesHealthStats(final StreamInput in) throws IOException {
        this.topQueriesHeapSize = in.readInt();
        this.queryGrouperHealthStats = new QueryGrouperHealthStats(in);
    }

    /**
     * Constructor of TopQueriesHealthStats
     *
     * @param topQueriesHeapSize Top Queries heap size
     * @param queryGrouperHealthStats Health stats for query grouper
     */
    public TopQueriesHealthStats(final int topQueriesHeapSize, final QueryGrouperHealthStats queryGrouperHealthStats) {
        this.topQueriesHeapSize = topQueriesHeapSize;
        this.queryGrouperHealthStats = queryGrouperHealthStats;
    }

    /**
     * Write TopQueriesHealthStats Object to output stream
     *
     * @param out streamOutput
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(topQueriesHeapSize);
        queryGrouperHealthStats.writeTo(out);
    }

    /**
     * Write TopQueriesHealthStats object to XContent
     *
     * @param builder XContentBuilder
     * @param params Parameters
     * @return XContentBuilder
     * @throws IOException if an I/O error occurs
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TOP_QUERIES_HEAP_SIZE, topQueriesHeapSize);
        queryGrouperHealthStats.toXContent(builder, params);
        return builder;
    }

    /**
     * Gets the top queries heap size.
     *
     * @return the top queries heap size
     */
    public int getTopQueriesHeapSize() {
        return topQueriesHeapSize;
    }

    /**
     * Gets the query grouper health stats.
     *
     * @return the query grouper health stats
     */
    public QueryGrouperHealthStats getQueryGrouperHealthStats() {
        return queryGrouperHealthStats;
    }
}
