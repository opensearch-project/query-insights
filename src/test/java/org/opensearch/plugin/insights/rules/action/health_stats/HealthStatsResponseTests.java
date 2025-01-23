/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.health_stats;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.healthStats.QueryInsightsHealthStats;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * Unit tests for the {@link HealthStatsResponse} class.
 */
public class HealthStatsResponseTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private final DiscoveryNode discoveryNode = new DiscoveryNode(
        "node_for_health_stats_test",
        buildNewFakeTransportAddress(),
        emptyMap(),
        emptySet(),
        VersionUtils.randomVersion(random())
    );
    private QueryInsightsHealthStats healthStats;

    @Before
    public void setup() {
        this.threadPool = new TestThreadPool(
            "QueryInsightsHealthStatsTests",
            new ScalingExecutorBuilder(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR, 1, 5, TimeValue.timeValueMinutes(5))
        );
        this.healthStats = new QueryInsightsHealthStats(
            threadPool.info(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR),
            10,
            new HashMap<>(),
            new HashMap<>()
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testConstructorWithClusterNameAndNodes() {
        ClusterName clusterName = new ClusterName("test-cluster");
        List<HealthStatsNodeResponse> nodes = new ArrayList<>();
        List<FailedNodeException> failures = new ArrayList<>();
        HealthStatsResponse response = new HealthStatsResponse(clusterName, nodes, failures);
        assertEquals(clusterName, response.getClusterName());
        assertEquals(nodes, response.getNodes());
        assertEquals(failures, response.failures());
    }

    public void testConstructorWithStreamInput() throws IOException {
        ClusterName clusterName = new ClusterName("test-cluster");
        List<HealthStatsNodeResponse> nodes = new ArrayList<>();
        List<FailedNodeException> failures = new ArrayList<>();
        HealthStatsResponse originalResponse = new HealthStatsResponse(clusterName, nodes, failures);
        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        HealthStatsResponse deserializedResponse = new HealthStatsResponse(in);
        assertEquals(originalResponse.getClusterName(), deserializedResponse.getClusterName());
        assertEquals(originalResponse.getNodes(), deserializedResponse.getNodes());
    }

    public void testWriteNodesToAndReadNodesFrom() throws IOException {
        ClusterName clusterName = new ClusterName("test-cluster");
        List<HealthStatsNodeResponse> nodes = Collections.emptyList();
        List<FailedNodeException> failures = Collections.emptyList();
        HealthStatsResponse response = new HealthStatsResponse(clusterName, nodes, failures);
        BytesStreamOutput out = new BytesStreamOutput();
        // Serialize nodes
        response.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        // Deserialize nodes
        HealthStatsResponse deserializedResponse = new HealthStatsResponse(in);
        assertEquals(response.getNodes().size(), deserializedResponse.getNodes().size());
    }

    public void testToXContent() throws IOException {
        ClusterName clusterName = new ClusterName("test-cluster");
        List<HealthStatsNodeResponse> nodes = List.of(new HealthStatsNodeResponse(discoveryNode, healthStats));
        List<FailedNodeException> failures = Collections.emptyList();
        HealthStatsResponse response = new HealthStatsResponse(clusterName, nodes, failures);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String expectedJson =
            "{\"node_for_health_stats_test\":{\"ThreadPoolInfo\":{\"query_insights_executor\":{\"type\":\"scaling\",\"core\":1,\"max\":5,\"keep_alive\":\"5m\",\"queue_size\":-1}},\"QueryRecordsQueueSize\":10,\"TopQueriesHealthStats\":{},\"FieldTypeCacheStats\":{\"size_in_bytes\":0,\"entry_count\":0,\"evictions\":0,\"hit_count\":0,\"miss_count\":0}}}";
        assertEquals(expectedJson, builder.toString());
    }
}
