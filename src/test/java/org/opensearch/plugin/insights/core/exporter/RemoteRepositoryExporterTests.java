/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

public class RemoteRepositoryExporterTests extends OpenSearchTestCase {

    @Mock
    private RepositoriesService repositoriesService;

    @Mock
    private BlobStoreRepository blobStoreRepository;

    @Mock
    private BlobStore blobStore;

    @Mock
    private BlobContainer blobContainer;

    @Mock
    private AsyncMultiStreamBlobContainer asyncBlobContainer;

    private RemoteRepositoryExporter remoteRepositoryExporter;

    public void setUp() throws Exception {
        super.setUp();

        MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );
        OperationalMetricsCounter.initialize("test", metricsRegistry);

        MockitoAnnotations.openMocks(this);
        remoteRepositoryExporter = new RemoteRepositoryExporter(
            () -> repositoriesService,
            "test-repo",
            "query-insights/account123/domain-name",
            DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm'UTC'", Locale.ROOT),
            "test-remote-exporter"
        );
    }

    public void testGetId() {
        assertEquals("test-remote-exporter", remoteRepositoryExporter.getId());
    }

    public void testExportWhenDisabled() {
        remoteRepositoryExporter.setEnabled(false);
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        remoteRepositoryExporter.export(records);
        verify(repositoriesService, never()).repository(any());
    }

    public void testExportEmptyRecords() {
        remoteRepositoryExporter.export(null);
        remoteRepositoryExporter.export(Arrays.asList());

        verify(repositoriesService, never()).repository(any());
    }

    public void testExportWithAsyncMultiStreamBlobContainer() throws Exception {
        remoteRepositoryExporter.setEnabled(true);
        when(repositoriesService.repository("test-repo")).thenReturn(blobStoreRepository);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(asyncBlobContainer);

        List<SearchQueryRecord> records = Arrays.asList(
            new SearchQueryRecord(System.currentTimeMillis(), new HashMap<>(), new HashMap<>(), "test-id-1"),
            new SearchQueryRecord(System.currentTimeMillis(), new HashMap<>(), new HashMap<>(), "test-id-2")
        );

        remoteRepositoryExporter.export(records);

        verify(asyncBlobContainer).asyncBlobUpload(any(WriteContext.class), any(ActionListener.class));
    }

    public void testExportWithNonAsyncBlobContainer() throws Exception {
        remoteRepositoryExporter.setEnabled(true);
        when(repositoriesService.repository("test-repo")).thenReturn(blobStoreRepository);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);

        List<SearchQueryRecord> records = Arrays.asList(
            new SearchQueryRecord(System.currentTimeMillis(), new HashMap<>(), new HashMap<>(), "test-id-1")
        );

        remoteRepositoryExporter.export(records);

        verify(asyncBlobContainer, never()).asyncBlobUpload(any(), any());
    }

    public void testExportWithNonBlobStoreRepository() throws IOException {
        remoteRepositoryExporter.setEnabled(true);
        Repository nonBlobStoreRepo = mock(Repository.class);
        when(repositoriesService.repository("test-repo")).thenReturn(nonBlobStoreRepo);

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        remoteRepositoryExporter.export(records);

        verify(asyncBlobContainer, never()).asyncBlobUpload(any(), any());
    }

    public void testExportWithIOException() throws Exception {
        remoteRepositoryExporter.setEnabled(true);
        when(repositoriesService.repository("test-repo")).thenReturn(blobStoreRepository);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(asyncBlobContainer);
        doThrow(new IOException("test exception")).when(asyncBlobContainer).asyncBlobUpload(any(), any());

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        remoteRepositoryExporter.export(records);

        verify(asyncBlobContainer).asyncBlobUpload(any(), any());
    }

    public void testGetAndSetEnabled() {
        remoteRepositoryExporter.setEnabled(true);
        assertTrue(remoteRepositoryExporter.isEnabled());
        remoteRepositoryExporter.setEnabled(false);
        assertFalse(remoteRepositoryExporter.isEnabled());
    }

    public void testGetAndSetRepositoryName() {
        remoteRepositoryExporter.setRepositoryName("new-repo");
        assertEquals("new-repo", remoteRepositoryExporter.getRepositoryName());
    }

    public void testGetAndSetBasePath() {
        remoteRepositoryExporter.setBasePath("new-path");
        assertEquals("new-path", remoteRepositoryExporter.getBasePath());
    }

    public void testClose() {
        try {
            remoteRepositoryExporter.close();
        } catch (Exception e) {
            fail("No exception should be thrown when closing remote repository exporter");
        }
    }
}
