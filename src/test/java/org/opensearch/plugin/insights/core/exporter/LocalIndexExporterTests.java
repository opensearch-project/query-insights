/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.utils.IndexDiscoveryHelper;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

/**
 * Granular tests for the {@link LocalIndexExporterTests} class.
 */
public class LocalIndexExporterTests extends OpenSearchTestCase {
    private final DateTimeFormatter format = DateTimeFormatter.ofPattern("YYYY.MM.dd", Locale.ROOT);
    private final Client client = mock(Client.class);
    private final AdminClient adminClient = mock(AdminClient.class);
    private final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
    private LocalIndexExporter localIndexExporter;
    private final ThreadPool threadPool = new TestThreadPool("QueryInsightsThreadPool");
    private String indexName;
    private ClusterService clusterService;

    @Before
    public void setup() {
        // Setup metrics registry and counter
        MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );
        OperationalMetricsCounter.initialize("test", metricsRegistry);

        // Setup mocks
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        // Setup exists response
        doAnswer(invocation -> {
            org.opensearch.core.action.ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(indicesAdminClient).exists(any(IndicesExistsRequest.class), any());

        // Setup cluster service with default values
        indexName = IndexDiscoveryHelper.buildLocalIndexName(format, ZonedDateTime.now(ZoneOffset.UTC));
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterState state = ClusterStateCreationUtils.stateWithActivePrimary(indexName, true, 1, 0);
        clusterService = ClusterServiceUtils.createClusterService(threadPool, state.getNodes().getLocalNode(), clusterSettings);
        ClusterServiceUtils.setState(clusterService, state);

        // Create local index exporter
        localIndexExporter = new LocalIndexExporter(client, clusterService, format, "", "id");
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(clusterService);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testExportEmptyRecords() {
        List<SearchQueryRecord> records = List.of();
        try {
            localIndexExporter.export(records);
        } catch (Exception e) {
            fail("No exception should be thrown when exporting empty query insights data");
        }
    }

    public void testExportRecordsWhenIndexExists() throws IOException {
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT);
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, "{}", "id");
        LocalIndexExporter exporterSpy = spy(exporter);

        doReturn(true).when(exporterSpy).checkIndexExists(anyString());
        // Mock the bulk method to track calls
        doAnswer(invocation -> null).when(exporterSpy).bulk(anyString(), any());

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(3);

        exporterSpy.export(records);

        // bulk was called (to add records to existing index)
        verify(exporterSpy).bulk(anyString(), eq(records));

        // createIndexAndBulk was NOT called
        verify(exporterSpy, never()).createIndexAndBulk(anyString(), any());
    }

    public void testExportRecordsWhenIndexNotExist() throws IOException {
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT);
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, "{}", "id");
        LocalIndexExporter exporterSpy = spy(exporter);

        // to simulate index doesn't exist
        doReturn(false).when(exporterSpy).checkIndexExists(anyString());
        doAnswer(invocation -> null).when(exporterSpy).createIndexAndBulk(anyString(), any());
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(3);

        exporterSpy.export(records);

        // createIndexAndBulk was called
        verify(exporterSpy).createIndexAndBulk(anyString(), eq(records));
        // bulk was NOT called directly (it's called inside createIndexAndBulk)
        verify(exporterSpy, never()).bulk(anyString(), any());
    }

    @SuppressWarnings("unchecked")
    public void testExportRecordsWithError() {
        BulkRequestBuilder bulkRequestBuilder = spy(new BulkRequestBuilder(client, BulkAction.INSTANCE));
        final PlainActionFuture<BulkResponse> future = mock(PlainActionFuture.class);
        when(future.actionGet()).thenReturn(null);
        doThrow(new RuntimeException()).when(bulkRequestBuilder).execute();
        when(client.prepareBulk()).thenReturn(bulkRequestBuilder);

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        try {
            localIndexExporter.export(records);
        } catch (Exception e) {
            fail("No exception should be thrown when exporting query insights data");
        }
    }

    public void testClose() {
        try {
            localIndexExporter.close();
        } catch (Exception e) {
            fail("No exception should be thrown when closing local index exporter");
        }
    }

    public void testGetAndSetIndexPattern() {
        final DateTimeFormatter newFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd", Locale.ROOT);
        localIndexExporter.setIndexPattern(newFormatter);
        assert (localIndexExporter.getIndexPattern() == newFormatter);
    }

    /**
     * Test that checkIndexExists returns true when the index exists
     */
    public void testCheckIndexExists() {
        RoutingTable mockRoutingTable = mock(RoutingTable.class);
        when(mockRoutingTable.hasIndex("test-index")).thenReturn(true);

        ClusterState mockState = mock(ClusterState.class);
        when(mockState.getRoutingTable()).thenReturn(mockRoutingTable);
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(mockState);

        // Create a LocalIndexExporter with our mock components
        LocalIndexExporter exporter = new LocalIndexExporter(client, mockClusterService, format, "{}", "id");

        // checkIndexExists returns true for the test index
        assertTrue(exporter.checkIndexExists("test-index"));

        // checkIndexExists returns false for a non-existent index
        when(mockRoutingTable.hasIndex("non-existent-index")).thenReturn(false);
        assertFalse(exporter.checkIndexExists("non-existent-index"));
    }

    /**
     * Test that readIndexMappings returns the provided mapping string when it's not empty
     */
    public void testReadIndexMappingsWithProvidedMapping() throws IOException {
        String customMapping = "{\"properties\":{\"test\":{\"type\":\"keyword\"}}}";
        LocalIndexExporter exporter = new LocalIndexExporter(client, clusterService, format, customMapping, "id");
        assertEquals(customMapping, exporter.readIndexMappings());
    }

    /**
     * Test that readIndexMappings returns empty JSON object when mapping string is empty
     */
    public void testReadIndexMappingsWithEmptyMapping() throws IOException {
        LocalIndexExporter exporter = new LocalIndexExporter(client, clusterService, format, "", "id");
        assertEquals("{}", exporter.readIndexMappings());
    }

    /**
     * Test that createIndex correctly sets auto_expand_replicas
     */
    public void testCreateIndexWithAutoExpandReplicas() throws IOException {
        LocalIndexExporter exporterSpy = spy(new LocalIndexExporter(client, clusterService, format, "{}", "id"));

        ArgumentCaptor<CreateIndexRequest> requestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);

        // Mock the client.admin().indices().create() call
        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesClient = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);

        doNothing().when(indicesClient).create(requestCaptor.capture(), any(ActionListener.class));
        exporterSpy.createIndexAndBulk("test-index", Collections.emptyList());

        // Verify the captured request
        CreateIndexRequest capturedRequest = requestCaptor.getValue();
        Settings settings = capturedRequest.settings();

        // Verify the settings use auto_expand_replicas
        assertEquals("test-index", capturedRequest.index());
        assertEquals("0-2", settings.get("index.auto_expand_replicas"));
        assertEquals("1", settings.get("index.number_of_shards"));

        // Verify there is no number_of_replicas setting.
        assertNull(settings.get("index.number_of_replicas"));
    }

    /**
     * Test that export flow directly creates index with mapping when index doesn't exist
     */
    public void testExportDirectlyCreatesIndexWithMapping() throws IOException {
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT);
        String customMapping = "{\"properties\":{\"test_field\":{\"type\":\"keyword\"}}}";
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, customMapping, "id");
        LocalIndexExporter exporterSpy = spy(exporter);

        // Mock index doesn't exist
        doReturn(false).when(exporterSpy).checkIndexExists(anyString());
        doAnswer(invocation -> null).when(exporterSpy).createIndexAndBulk(anyString(), any());

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        exporterSpy.export(records);

        // Verify createIndexAndBulk was called with the custom mapping
        verify(exporterSpy).createIndexAndBulk(anyString(), eq(records));
    }

    /**
     * Test that createIndexAndBulk sets the correct mapping in CreateIndexRequest
     */
    public void testCreateIndexAndBulkSetsMapping() throws IOException {
        String customMapping = "{\"properties\":{\"custom_field\":{\"type\":\"text\"}}}";
        LocalIndexExporter exporterSpy = spy(new LocalIndexExporter(client, clusterService, format, customMapping, "id"));

        ArgumentCaptor<CreateIndexRequest> requestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);

        // Mock the client.admin().indices().create() call
        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesClient = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);

        doNothing().when(indicesClient).create(requestCaptor.capture(), any(ActionListener.class));
        exporterSpy.createIndexAndBulk("test-index", Collections.emptyList());

        // Verify the captured request has the correct mapping
        CreateIndexRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index", capturedRequest.index());
        assertEquals(customMapping, capturedRequest.mappings());
    }

    /**
     * Test that export works correctly when mapping is empty
     */
    public void testExportWithEmptyMapping() throws IOException {
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT);
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, "", "id");
        LocalIndexExporter exporterSpy = spy(exporter);

        doReturn(false).when(exporterSpy).checkIndexExists(anyString());
        doAnswer(invocation -> null).when(exporterSpy).createIndexAndBulk(anyString(), any());

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(1);
        exporterSpy.export(records);

        // Verify createIndexAndBulk was called with empty mapping converted to {}
        verify(exporterSpy).createIndexAndBulk(anyString(), eq(records));
    }

}
