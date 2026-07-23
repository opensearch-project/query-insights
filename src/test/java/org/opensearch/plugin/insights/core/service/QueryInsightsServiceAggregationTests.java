/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterFactory;
import org.opensearch.plugin.insights.core.exporter.RemoteRepositoryExporter;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReaderFactory;
import org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;
import org.opensearch.transport.client.IndicesAdminClient;

/**
 * Unit Tests for {@link QueryInsightsService#aggregateByExecutionId(List)}.
 */
public class QueryInsightsServiceAggregationTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private final Client client = mock(Client.class);
    private final NamedXContentRegistry namedXContentRegistry = mock(NamedXContentRegistry.class);
    private QueryInsightsService queryInsightsService;
    private final AdminClient adminClient = mock(AdminClient.class);
    private final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
    private final ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
    private ClusterService clusterService;
    private QueryInsightsExporterFactory queryInsightsExporterFactory;
    private QueryInsightsReaderFactory queryInsightsReaderFactory;

    @Before
    public void setup() {
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        this.threadPool = new TestThreadPool(
            "QueryInsightsServiceAggregationTest",
            new ScalingExecutorBuilder(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR, 1, 5, TimeValue.timeValueMinutes(5))
        );
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        clusterService = ClusterServiceUtils.createClusterService(settings, clusterSettings, threadPool);

        queryInsightsExporterFactory = mock(QueryInsightsExporterFactory.class);
        queryInsightsReaderFactory = mock(QueryInsightsReaderFactory.class);

        RemoteRepositoryExporter mockRemoteRepositoryExporter = mock(RemoteRepositoryExporter.class);
        when(
            queryInsightsExporterFactory.createRemoteRepositoryExporter(
                eq(TopQueriesService.TOP_QUERIES_REMOTE_EXPORTER_ID),
                anyString(),
                anyString(),
                anyBoolean()
            )
        ).thenReturn(mockRemoteRepositoryExporter);
        when(mockRemoteRepositoryExporter.getBasePath()).thenReturn("query-insights");
        when(mockRemoteRepositoryExporter.isEnabled()).thenReturn(false);
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_REMOTE_EXPORTER_ID))
            .thenReturn(mockRemoteRepositoryExporter);

        queryInsightsService = new QueryInsightsService(
            clusterService,
            threadPool,
            client,
            NoopMetricsRegistry.INSTANCE,
            namedXContentRegistry,
            queryInsightsExporterFactory,
            queryInsightsReaderFactory
        );
        queryInsightsService.enableCollection(MetricType.LATENCY, true);
        queryInsightsService.enableCollection(MetricType.CPU, true);
        queryInsightsService.enableCollection(MetricType.MEMORY, true);
        queryInsightsService.setQueryShapeGenerator(new QueryShapeGenerator(clusterService));

        MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );
        OperationalMetricsCounter.initialize("cluster", metricsRegistry);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (clusterService != null) {
            IOUtils.close(clusterService);
        }
        if (queryInsightsService != null) {
            queryInsightsService.doClose();
        }
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    /**
     * Test that records without an execution ID pass through unchanged.
     */
    public void testRecordsWithoutExecutionIdPassThrough() {
        List<SearchQueryRecord> records = createRecordsWithoutExecutionId(3);
        List<SearchQueryRecord> result = queryInsightsService.aggregateByExecutionId(records);

        assertEquals("All records without execution ID should pass through unchanged", 3, result.size());
        for (int i = 0; i < 3; i++) {
            assertSame(records.get(i), result.get(i));
        }
    }

    /**
     * Test that records with the same execution ID produce individual records plus one parent record.
     */
    @SuppressWarnings("unchecked")
    public void testRecordsWithSameExecutionIdProduceParent() {
        String execId = "exec-123";
        List<SearchQueryRecord> records = createRecordsWithExecutionId(3, execId, null);

        List<SearchQueryRecord> result = queryInsightsService.aggregateByExecutionId(records);

        // Should produce 3 individual + 1 parent = 4 records
        assertEquals("Should produce individual records plus one parent", 4, result.size());

        // Find the parent record (has SUB_QUERIES attribute)
        SearchQueryRecord parentRecord = null;
        List<SearchQueryRecord> individualRecords = new ArrayList<>();
        for (SearchQueryRecord r : result) {
            if (r.getAttributes().containsKey(Attribute.SUB_QUERIES)) {
                parentRecord = r;
            } else {
                individualRecords.add(r);
            }
        }

        assertNotNull("Parent record should exist", parentRecord);
        assertEquals("Should have 3 individual records", 3, individualRecords.size());

        // Verify sub_queries contains all individual IDs
        List<Map<String, Object>> subQueries = (List<Map<String, Object>>) parentRecord.getAttributes().get(Attribute.SUB_QUERIES);
        assertNotNull("Parent should have sub_queries", subQueries);
        assertEquals("sub_queries should have 3 entries", 3, subQueries.size());

        // Each sub-query entry should have an id
        for (Map<String, Object> sub : subQueries) {
            assertNotNull("Each sub-query should have an id", sub.get("id"));
        }
    }

    /**
     * Test that the parent record has summed measurements.
     */
    @SuppressWarnings("unchecked")
    public void testParentRecordHasSummedMeasurements() {
        String execId = "exec-sum-test";
        // Create 3 records with known measurements
        List<SearchQueryRecord> records = new ArrayList<>();
        long baseTime = System.currentTimeMillis();
        for (int i = 0; i < 3; i++) {
            Map<MetricType, Measurement> measurements = new LinkedHashMap<>();
            measurements.put(MetricType.LATENCY, new Measurement(100L * (i + 1)));  // 100, 200, 300
            measurements.put(MetricType.CPU, new Measurement(50L * (i + 1)));        // 50, 100, 150
            measurements.put(MetricType.MEMORY, new Measurement(1000L * (i + 1)));   // 1000, 2000, 3000

            Map<Attribute, Object> attributes = new HashMap<>();
            Map<String, Object> labels = new HashMap<>();
            labels.put("x-query-execution-id", execId);
            attributes.put(Attribute.LABELS, labels);
            attributes.put(Attribute.INDICES, new String[]{"test-index"});

            records.add(new SearchQueryRecord(baseTime + i, measurements, attributes, UUID.randomUUID().toString()));
        }

        List<SearchQueryRecord> result = queryInsightsService.aggregateByExecutionId(records);

        // Find parent record
        SearchQueryRecord parentRecord = null;
        for (SearchQueryRecord r : result) {
            if (r.getAttributes().containsKey(Attribute.SUB_QUERIES)) {
                parentRecord = r;
                break;
            }
        }

        assertNotNull("Parent record should exist", parentRecord);
        // Latency sum: 100 + 200 + 300 = 600
        assertEquals(600L, parentRecord.getMeasurement(MetricType.LATENCY).longValue());
        // CPU sum: 50 + 100 + 150 = 300
        assertEquals(300L, parentRecord.getMeasurement(MetricType.CPU).longValue());
        // Memory sum: 1000 + 2000 + 3000 = 6000
        assertEquals(6000L, parentRecord.getMeasurement(MetricType.MEMORY).longValue());
    }

    /**
     * Test that the parent record has sub_queries with individual IDs.
     */
    @SuppressWarnings("unchecked")
    public void testParentRecordSubQueriesContainIndividualIds() {
        String execId = "exec-ids-test";
        List<SearchQueryRecord> records = createRecordsWithExecutionId(2, execId, null);

        List<SearchQueryRecord> result = queryInsightsService.aggregateByExecutionId(records);

        // Find parent record
        SearchQueryRecord parentRecord = null;
        for (SearchQueryRecord r : result) {
            if (r.getAttributes().containsKey(Attribute.SUB_QUERIES)) {
                parentRecord = r;
                break;
            }
        }

        assertNotNull("Parent record should exist", parentRecord);
        List<Map<String, Object>> subQueries = (List<Map<String, Object>>) parentRecord.getAttributes().get(Attribute.SUB_QUERIES);
        assertEquals(2, subQueries.size());

        // Verify each sub-query has the original record's ID
        for (int i = 0; i < 2; i++) {
            assertEquals(records.get(i).getId(), subQueries.get(i).get("id"));
        }
    }

    /**
     * Test that parent record has SQL_PHASES parsed from x-query-phases label.
     */
    @SuppressWarnings("unchecked")
    public void testParentRecordHasSqlPhases() {
        String execId = "exec-phases-test";
        String phasesStr = "parse:10|cpu:5,analyze:20|cpu:8,plan:30|cpu:12";
        List<SearchQueryRecord> records = createRecordsWithExecutionId(2, execId, phasesStr);

        List<SearchQueryRecord> result = queryInsightsService.aggregateByExecutionId(records);

        // Find parent record
        SearchQueryRecord parentRecord = null;
        for (SearchQueryRecord r : result) {
            if (r.getAttributes().containsKey(Attribute.SUB_QUERIES)) {
                parentRecord = r;
                break;
            }
        }

        assertNotNull("Parent record should exist", parentRecord);
        Map<String, Map<String, Long>> sqlPhases = (Map<String, Map<String, Long>>) parentRecord.getAttributes().get(Attribute.SQL_PHASES);
        assertNotNull("Parent record should have SQL_PHASES", sqlPhases);

        // Verify parsed phases
        assertTrue(sqlPhases.containsKey("parse"));
        assertEquals(Long.valueOf(10), sqlPhases.get("parse").get("time"));
        assertEquals(Long.valueOf(5), sqlPhases.get("parse").get("cpu"));

        assertTrue(sqlPhases.containsKey("analyze"));
        assertEquals(Long.valueOf(20), sqlPhases.get("analyze").get("time"));
        assertEquals(Long.valueOf(8), sqlPhases.get("analyze").get("cpu"));

        assertTrue(sqlPhases.containsKey("plan"));
        assertEquals(Long.valueOf(30), sqlPhases.get("plan").get("time"));
        assertEquals(Long.valueOf(12), sqlPhases.get("plan").get("cpu"));
    }

    /**
     * Test that single-query groups with x-query-phases also get SQL_PHASES.
     */
    @SuppressWarnings("unchecked")
    public void testSingleQueryGroupGetsSqlPhases() {
        String execId = "exec-single-test";
        String phasesStr = "parse:5,analyze:15";
        List<SearchQueryRecord> records = createRecordsWithExecutionId(1, execId, phasesStr);

        List<SearchQueryRecord> result = queryInsightsService.aggregateByExecutionId(records);

        // Single-query group: only the individual record is returned (no parent created)
        assertEquals("Single-query group should produce 1 record", 1, result.size());

        SearchQueryRecord record = result.get(0);
        Map<String, Map<String, Long>> sqlPhases = (Map<String, Map<String, Long>>) record.getAttributes().get(Attribute.SQL_PHASES);
        assertNotNull("Single query record should have SQL_PHASES when phases header is present", sqlPhases);

        assertTrue(sqlPhases.containsKey("parse"));
        assertEquals(Long.valueOf(5), sqlPhases.get("parse").get("time"));

        assertTrue(sqlPhases.containsKey("analyze"));
        assertEquals(Long.valueOf(15), sqlPhases.get("analyze").get("time"));
    }

    /**
     * Test mixed records: some with execution ID, some without.
     */
    @SuppressWarnings("unchecked")
    public void testMixedRecordsWithAndWithoutExecutionId() {
        String execId = "exec-mixed-test";
        List<SearchQueryRecord> recordsWithExecId = createRecordsWithExecutionId(2, execId, null);
        List<SearchQueryRecord> recordsWithoutExecId = createRecordsWithoutExecutionId(2);

        List<SearchQueryRecord> allRecords = new ArrayList<>();
        allRecords.addAll(recordsWithExecId);
        allRecords.addAll(recordsWithoutExecId);

        List<SearchQueryRecord> result = queryInsightsService.aggregateByExecutionId(allRecords);

        // Should have: 2 individual from exec group + 1 parent + 2 without exec = 5
        assertEquals("Should have 5 total records", 5, result.size());

        int parentCount = 0;
        for (SearchQueryRecord r : result) {
            if (r.getAttributes().containsKey(Attribute.SUB_QUERIES)) {
                parentCount++;
            }
        }
        assertEquals("Should have exactly 1 parent record", 1, parentCount);
    }

    /**
     * Test multiple different execution IDs produce separate parent records.
     */
    @SuppressWarnings("unchecked")
    public void testMultipleExecutionIdsProduceSeparateParents() {
        String execId1 = "exec-group-1";
        String execId2 = "exec-group-2";
        List<SearchQueryRecord> group1 = createRecordsWithExecutionId(2, execId1, null);
        List<SearchQueryRecord> group2 = createRecordsWithExecutionId(3, execId2, null);

        List<SearchQueryRecord> allRecords = new ArrayList<>();
        allRecords.addAll(group1);
        allRecords.addAll(group2);

        List<SearchQueryRecord> result = queryInsightsService.aggregateByExecutionId(allRecords);

        // group1: 2 individual + 1 parent = 3
        // group2: 3 individual + 1 parent = 4
        // total: 7
        assertEquals("Should have 7 total records", 7, result.size());

        int parentCount = 0;
        for (SearchQueryRecord r : result) {
            if (r.getAttributes().containsKey(Attribute.SUB_QUERIES)) {
                parentCount++;
                List<Map<String, Object>> subQueries =
                    (List<Map<String, Object>>) r.getAttributes().get(Attribute.SUB_QUERIES);
                // Verify each parent has the correct number of sub-queries
                assertTrue(
                    "Parent should have 2 or 3 sub-queries",
                    subQueries.size() == 2 || subQueries.size() == 3
                );
            }
        }
        assertEquals("Should have exactly 2 parent records", 2, parentCount);
    }

    /**
     * Test that parent record labels contain sub_query_count.
     */
    @SuppressWarnings("unchecked")
    public void testParentRecordLabelsContainSubQueryCount() {
        String execId = "exec-count-test";
        List<SearchQueryRecord> records = createRecordsWithExecutionId(3, execId, null);

        List<SearchQueryRecord> result = queryInsightsService.aggregateByExecutionId(records);

        // Find parent record
        SearchQueryRecord parentRecord = null;
        for (SearchQueryRecord r : result) {
            if (r.getAttributes().containsKey(Attribute.SUB_QUERIES)) {
                parentRecord = r;
                break;
            }
        }

        assertNotNull("Parent record should exist", parentRecord);
        Map<String, Object> labels = (Map<String, Object>) parentRecord.getAttributes().get(Attribute.LABELS);
        assertNotNull("Parent should have labels", labels);
        assertEquals("sub_query_count should be 3", "3", labels.get("sub_query_count"));
    }

    /**
     * Test that empty records list produces empty result.
     */
    public void testEmptyRecordsList() {
        List<SearchQueryRecord> result = queryInsightsService.aggregateByExecutionId(new ArrayList<>());
        assertTrue("Empty input should produce empty result", result.isEmpty());
    }

    // --- Helper Methods ---

    private List<SearchQueryRecord> createRecordsWithoutExecutionId(int count) {
        List<SearchQueryRecord> records = new ArrayList<>();
        long baseTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Map<MetricType, Measurement> measurements = new LinkedHashMap<>();
            measurements.put(MetricType.LATENCY, new Measurement(100L));
            measurements.put(MetricType.CPU, new Measurement(50L));
            measurements.put(MetricType.MEMORY, new Measurement(1000L));

            Map<Attribute, Object> attributes = new HashMap<>();
            Map<String, Object> labels = new HashMap<>();
            labels.put("some-label", "some-value");
            attributes.put(Attribute.LABELS, labels);

            records.add(new SearchQueryRecord(baseTime + i, measurements, attributes, UUID.randomUUID().toString()));
        }
        return records;
    }

    private List<SearchQueryRecord> createRecordsWithExecutionId(int count, String executionId, String phasesStr) {
        List<SearchQueryRecord> records = new ArrayList<>();
        long baseTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Map<MetricType, Measurement> measurements = new LinkedHashMap<>();
            measurements.put(MetricType.LATENCY, new Measurement(100L * (i + 1)));
            measurements.put(MetricType.CPU, new Measurement(50L * (i + 1)));
            measurements.put(MetricType.MEMORY, new Measurement(1000L * (i + 1)));

            Map<Attribute, Object> attributes = new HashMap<>();
            Map<String, Object> labels = new HashMap<>();
            labels.put("x-query-execution-id", executionId);
            if (phasesStr != null) {
                labels.put("x-query-phases", phasesStr);
            }
            attributes.put(Attribute.LABELS, labels);
            attributes.put(Attribute.INDICES, new String[]{"test-index-" + i});

            records.add(new SearchQueryRecord(baseTime + i, measurements, attributes, UUID.randomUUID().toString()));
        }
        return records;
    }
}
