/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.plugin.insights.core.exporter.LocalIndexExporter.generateLocalIndexDateHash;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_TOP_N_QUERIES_INDEX_PATTERN;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Granular tests for the {@link LocalIndexExporterTests} class.
 */
public class LocalIndexExporterTests extends OpenSearchTestCase {
    private final DateTimeFormatter format = DateTimeFormatter.ofPattern("YYYY.MM.dd", Locale.ROOT);
    private final Client client = mock(Client.class);
    private final AdminClient adminClient = mock(AdminClient.class);
    private final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
    private LocalIndexExporter localIndexExporter;

    @Before
    public void setup() {
        localIndexExporter = new LocalIndexExporter(client, format);

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
    }

    public void testExportEmptyRecords() {
        List<SearchQueryRecord> records = List.of();
        try {
            localIndexExporter.export(records);
        } catch (Exception e) {
            fail("No exception should be thrown when exporting empty query insights data");
        }
    }

    @SuppressWarnings("unchecked")
    public void testExportRecords() {
        BulkRequestBuilder bulkRequestBuilder = spy(new BulkRequestBuilder(client, BulkAction.INSTANCE));
        final PlainActionFuture<BulkResponse> future = mock(PlainActionFuture.class);
        when(future.actionGet()).thenReturn(null);
        doAnswer(invocation -> future).when(bulkRequestBuilder).execute();
        when(client.prepareBulk()).thenReturn(bulkRequestBuilder);

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        try {
            localIndexExporter.export(records);
        } catch (Exception e) {
            fail("No exception should be thrown when exporting query insights data");
        }
        assertEquals(2, bulkRequestBuilder.numberOfActions());
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

    public void testDeleteExpiredTopNIndices() {
        // Reset exporter index pattern to default
        localIndexExporter.setIndexPattern(DateTimeFormatter.ofPattern(DEFAULT_TOP_N_QUERIES_INDEX_PATTERN, Locale.ROOT));

        // Create 9 top_queries-* indices
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        for (int i = 1; i < 10; i++) {
            String indexName = "top_queries-2024.01.0" + i + "-" + generateLocalIndexDateHash();
            long creationTime = Instant.now().minus(i, ChronoUnit.DAYS).toEpochMilli();

            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(SETTING_CREATION_DATE, creationTime)
                )
                .build();
            indexMetadataMap.put(indexName, indexMetadata);
        }
        localIndexExporter.deleteExpiredTopNIndices(indexMetadataMap);
        // Default retention is 7 days
        // Oldest 3 of 10 indices should be deleted
        verify(client, times(3)).admin();
        verify(adminClient, times(3)).indices();
        verify(indicesAdminClient, times(3)).delete(any(), any());
    }
}
