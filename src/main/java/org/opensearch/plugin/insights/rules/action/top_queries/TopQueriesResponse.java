/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.top_queries;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Transport response for cluster/node level top queries information.
 */
public class TopQueriesResponse extends BaseNodesResponse<TopQueries> implements ToXContentFragment {

    private static final String CLUSTER_LEVEL_RESULTS_KEY = "top_queries";
    private final MetricType metricType;

    /**
     * Constructor for TopQueriesResponse.
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public TopQueriesResponse(final StreamInput in) throws IOException {
        super(in);
        metricType = in.readEnum(MetricType.class);
    }

    /**
     * Constructor for TopQueriesResponse
     *
     * @param clusterName The current cluster name
     * @param nodes A list that contains top queries results from all nodes
     * @param failures A list that contains FailedNodeException
     * @param metricType the {@link MetricType} to be returned in this response
     */
    public TopQueriesResponse(
        final ClusterName clusterName,
        final List<TopQueries> nodes,
        final List<FailedNodeException> failures,
        final MetricType metricType
    ) {
        super(clusterName, nodes, failures);
        this.metricType = metricType;
    }

    /**
     * Get the metric type for this response.
     *
     * @return The {@link MetricType}.
     */
    public MetricType getMetricType() {
        return metricType;
    }

    @Override
    protected List<TopQueries> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(TopQueries::new);
    }

    @Override
    protected void writeNodesTo(final StreamOutput out, final List<TopQueries> nodes) throws IOException {
        out.writeList(nodes);
        out.writeEnum(metricType);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        final List<TopQueries> results = getNodes();
        builder.startObject();
        toClusterLevelResult(builder, params, results);
        return builder.endObject();
    }

    @Override
    public String toString() {
        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            this.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.toString();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    /**
     * Merge top n queries results from nodes into cluster level results in XContent format.
     *
     * @param builder XContent builder
     * @param params serialization parameters
     * @param results top queries results from all nodes
     * @throws IOException if an error occurs
     */
    private void toClusterLevelResult(final XContentBuilder builder, final Params params, final List<TopQueries> results)
        throws IOException {
        final List<SearchQueryRecord> all_records = results.stream()
            .map(TopQueries::getTopQueriesRecord)
            .flatMap(Collection::stream)
            .sorted((a, b) -> SearchQueryRecord.compare(a, b, metricType) * -1)
            .collect(Collectors.toList());
        builder.startArray(CLUSTER_LEVEL_RESULTS_KEY);
        for (SearchQueryRecord record : all_records) {
            record.toXContent(builder, params);
        }
        builder.endArray();
    }

}
