/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.healthStats;

import java.io.IOException;
import java.util.Map;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolInfo;

/**
 * QueryInsightsHealthStats holds the stats on one node to indicate the health of the Query Insights plugin.
 */
public class QueryInsightsHealthStats implements ToXContentFragment, Writeable {
    private final ThreadPool.Info threadPoolInfo;
    private final int queryRecordsQueueSize;
    private final Map<MetricType, TopQueriesHealthStats> topQueriesHealthStats;

    private static final String THREAD_POOL_INFO = "ThreadPoolInfo";
    private static final String QUERY_RECORDS_QUEUE_SIZE = "QueryRecordsQueueSize";
    private static final String TOP_QUERIES_HEALTH_STATS = "TopQueriesHealthStats";

    /**
     * Constructor to read QueryInsightsHealthStats from a StreamInput.
     *
     * @param in the StreamInput to read the QueryInsightsHealthStats from
     * @throws IOException if an I/O error occurs
     */
    public QueryInsightsHealthStats(final StreamInput in) throws IOException {
        this.threadPoolInfo = new ThreadPool.Info(in);
        this.queryRecordsQueueSize = in.readInt();
        this.topQueriesHealthStats = in.readMap(MetricType::readFromStream, TopQueriesHealthStats::new);
    }

    /**
     * Constructor of QueryInsightsHealthStats
     *
     * @param threadPoolInfo the {@link ThreadPoolInfo} of the internal Query Insights threadPool
     * @param queryRecordsQueueSize The generic Query Record Queue size
     * @param topQueriesHealthStats Top Queries health stats
     */
    public QueryInsightsHealthStats(
        final ThreadPool.Info threadPoolInfo,
        final int queryRecordsQueueSize,
        final Map<MetricType, TopQueriesHealthStats> topQueriesHealthStats
    ) {
        if (threadPoolInfo == null || topQueriesHealthStats == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }
        this.threadPoolInfo = threadPoolInfo;
        this.queryRecordsQueueSize = queryRecordsQueueSize;
        this.topQueriesHealthStats = topQueriesHealthStats;
    }

    /**
     * Write QueryInsightsHealthStats object to XContent
     *
     * @param builder XContentBuilder
     * @param params Parameters for build xContent
     * @return XContentBuilder
     * @throws IOException if an I/O error occurs
     */
    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        // Write thread pool info object
        builder.startObject(THREAD_POOL_INFO);
        threadPoolInfo.toXContent(builder, params);
        builder.endObject();
        // Write query records queue size
        builder.field(QUERY_RECORDS_QUEUE_SIZE, queryRecordsQueueSize);
        // Write Top Queries health stats object
        builder.startObject(TOP_QUERIES_HEALTH_STATS);
        for (Map.Entry<MetricType, TopQueriesHealthStats> entry : topQueriesHealthStats.entrySet()) {
            builder.startObject(entry.getKey().toString());
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    /**
     * Write QueryInsightsHealthStats Object to output stream
     *
     * @param out streamOutput
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        threadPoolInfo.writeTo(out);
        out.writeInt(queryRecordsQueueSize);
        out.writeMap(
            topQueriesHealthStats,
            MetricType::writeTo,
            (streamOutput, topQueriesHealthStats) -> topQueriesHealthStats.writeTo(out)
        );
    }

    /**
     * Get the thread pool info.
     *
     * @return the thread pool info
     */
    public ThreadPool.Info getThreadPoolInfo() {
        return threadPoolInfo;
    }

    /**
     * Get the query records queue size.
     *
     * @return the query records queue size
     */
    public int getQueryRecordsQueueSize() {
        return queryRecordsQueueSize;
    }

    /**
     * Get the top queries health stats.
     *
     * @return the top queries health stats
     */
    public Map<MetricType, TopQueriesHealthStats> getTopQueriesHealthStats() {
        return topQueriesHealthStats;
    }
}
