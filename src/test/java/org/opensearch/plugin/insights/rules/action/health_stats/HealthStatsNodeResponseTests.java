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
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
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
 * Unit tests for the {@link HealthStatsNodeResponse} class.
 */
public class HealthStatsNodeResponseTests extends OpenSearchTestCase {
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
            new HashMap<>()
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testConstructorWithNodeAndStats() {
        HealthStatsNodeResponse response = new HealthStatsNodeResponse(discoveryNode, healthStats);
        // Verify that the object is correctly initialized
        assertEquals(discoveryNode, response.getNode());
        assertEquals(healthStats, response.getHealthStats());
    }

    public void testConstructorWithStreamInput() throws IOException {
        // Serialize the HealthStatsNodeResponse to a StreamOutput
        HealthStatsNodeResponse originalResponse = new HealthStatsNodeResponse(discoveryNode, healthStats);
        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        HealthStatsNodeResponse response = new HealthStatsNodeResponse(in);
        assertNotNull(response);
        assertEquals(originalResponse.getHealthStats().getQueryRecordsQueueSize(), response.getHealthStats().getQueryRecordsQueueSize());
        assertEquals(originalResponse.getNode(), response.getNode());
    }

    public void testWriteTo() throws IOException {
        HealthStatsNodeResponse response = new HealthStatsNodeResponse(discoveryNode, healthStats);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        HealthStatsNodeResponse deserializedResponse = new HealthStatsNodeResponse(in);
        assertEquals(
            response.getHealthStats().getQueryRecordsQueueSize(),
            deserializedResponse.getHealthStats().getQueryRecordsQueueSize()
        );
        assertEquals(response.getNode(), deserializedResponse.getNode());
    }

    public void testToXContent() throws IOException {
        HealthStatsNodeResponse response = new HealthStatsNodeResponse(discoveryNode, healthStats);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String jsonString = builder.toString();
        assertNotNull(jsonString);
        assertTrue(jsonString.contains(discoveryNode.getId()));
    }

    public void testGetHealthStats() {
        HealthStatsNodeResponse response = new HealthStatsNodeResponse(discoveryNode, healthStats);
        assertEquals(healthStats, response.getHealthStats());
    }
}
