/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.healthStats;

import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.ENTRY_COUNT;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.EVICTIONS;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.HIT_COUNT;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.MISS_COUNT;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.SIZE_IN_BYTES;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * Unit tests for the {@link QueryInsightsHealthStats} class.
 */
public class QueryInsightsHealthStatsTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private ThreadPool.Info threadPoolInfo;
    private int queryRecordsQueueSize;
    private Map<MetricType, TopQueriesHealthStats> topQueriesHealthStats;
    private Map<String, Long> fieldTypeCacheStats;

    @Before
    public void setUpQueryInsightsHealthStats() {
        this.threadPool = new TestThreadPool(
            "QueryInsightsHealthStatsTests",
            new ScalingExecutorBuilder(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR, 1, 5, TimeValue.timeValueMinutes(5))
        );
        threadPoolInfo = threadPool.info(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR);
        queryRecordsQueueSize = 100;
        topQueriesHealthStats = new HashMap<>();
        topQueriesHealthStats.put(MetricType.LATENCY, new TopQueriesHealthStats(10, new QueryGrouperHealthStats(20, 15)));
        fieldTypeCacheStats = Map.of(HIT_COUNT, 5L, MISS_COUNT, 3L, EVICTIONS, 1L, ENTRY_COUNT, 4L, SIZE_IN_BYTES, 300L);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testConstructorAndGetters() {
        QueryInsightsHealthStats healthStats = new QueryInsightsHealthStats(
            threadPoolInfo,
            queryRecordsQueueSize,
            topQueriesHealthStats,
            fieldTypeCacheStats
        );
        assertNotNull(healthStats);
        assertEquals(threadPoolInfo, healthStats.getThreadPoolInfo());
        assertEquals(queryRecordsQueueSize, healthStats.getQueryRecordsQueueSize());
        assertEquals(topQueriesHealthStats, healthStats.getTopQueriesHealthStats());
    }

    public void testSerialization() throws IOException {
        QueryInsightsHealthStats healthStats = new QueryInsightsHealthStats(
            threadPoolInfo,
            queryRecordsQueueSize,
            topQueriesHealthStats,
            fieldTypeCacheStats
        );
        // Write to StreamOutput
        BytesStreamOutput out = new BytesStreamOutput();
        healthStats.writeTo(out);
        // Read from StreamInput
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        QueryInsightsHealthStats deserializedHealthStats = new QueryInsightsHealthStats(in);
        assertEquals(healthStats.getQueryRecordsQueueSize(), deserializedHealthStats.getQueryRecordsQueueSize());
        assertNotNull(deserializedHealthStats.getThreadPoolInfo());
        assertNotNull(deserializedHealthStats.getTopQueriesHealthStats());
    }

    public void testToXContent() throws IOException {
        QueryInsightsHealthStats healthStats = new QueryInsightsHealthStats(
            threadPoolInfo,
            queryRecordsQueueSize,
            topQueriesHealthStats,
            fieldTypeCacheStats
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        healthStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String jsonOutput = builder.prettyPrint().toString();
        // Expected JSON output
        String expectedJson = "{\n"
            + "    \"ThreadPoolInfo\": {\n"
            + "        \"query_insights_executor\": {\n"
            + "            \"type\": \"scaling\",\n"
            + "            \"core\": 1,\n"
            + "            \"max\": 5,\n"
            + "            \"keep_alive\": \"5m\",\n"
            + "            \"queue_size\": -1\n"
            + "        }\n"
            + "    },\n"
            + "    \"QueryRecordsQueueSize\": 100,\n"
            + "    \"TopQueriesHealthStats\": {\n"
            + "        \"latency\": {\n"
            + "            \"TopQueriesHeapSize\": 10,\n"
            + "            \"QueryGroupCount_Total\": 20,\n"
            + "            \"QueryGroupCount_MaxHeap\": 15\n"
            + "        }\n"
            + "    },\n"
            + "    \"FieldTypeCacheStats\": {\n"
            + "        \"size_in_bytes\": 300,\n"
            + "        \"entry_count\": 4,\n"
            + "        \"evictions\": 1,\n"
            + "        \"hit_count\": 5,\n"
            + "        \"miss_count\": 3\n"
            + "    }\n"
            + "}";
        assertEquals(expectedJson.replaceAll("\\s", ""), jsonOutput.replaceAll("\\s", ""));
    }
}
