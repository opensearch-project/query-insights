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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
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
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

public class RemoteRepositoryExporterTests extends OpenSearchTestCase {

    @Mock
    private RepositoriesService repositoriesService;

    @Mock
    private ClusterService clusterService;

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
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(localNode.getId()).thenReturn("test-node-id");
        when(clusterService.localNode()).thenReturn(localNode);

        when(repositoriesService.repository("test-repo")).thenReturn(blobStoreRepository);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(asyncBlobContainer);

        remoteRepositoryExporter = new RemoteRepositoryExporter(
            () -> repositoriesService,
            clusterService,
            "test-repo",
            "query-insights",
            DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm'UTC'", Locale.ROOT),
            "test-remote-exporter",
            true
        );
    }

    public void testGetId() {
        assertEquals("test-remote-exporter", remoteRepositoryExporter.getId());
    }

    public void testExportWhenDisabled() {
        remoteRepositoryExporter.setEnabled(false);
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        remoteRepositoryExporter.export(records);
        verify(repositoriesService, times(1)).repository(any());
    }

    public void testExportEmptyRecords() {
        remoteRepositoryExporter.export(null);
        remoteRepositoryExporter.export(Arrays.asList());

        verify(repositoriesService, times(1)).repository(any());
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
        when(repositoriesService.repository("new-repo")).thenReturn(blobStoreRepository);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(asyncBlobContainer);

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

    public void testUploadRetry() throws Exception {
        remoteRepositoryExporter.setEnabled(true);
        when(repositoriesService.repository("test-repo")).thenReturn(blobStoreRepository);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(asyncBlobContainer);

        List<SearchQueryRecord> records = Arrays.asList(
            new SearchQueryRecord(System.currentTimeMillis(), new HashMap<>(), new HashMap<>(), "test-id-1")
        );

        ArgumentCaptor<ActionListener> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        remoteRepositoryExporter.export(records);
        verify(asyncBlobContainer).asyncBlobUpload(any(WriteContext.class), listenerCaptor.capture());

        ActionListener listener = listenerCaptor.getValue();
        listener.onFailure(new IOException("Upload failed"));
        Thread.sleep(500);

        // Verify retries occurred
        verify(asyncBlobContainer, times(2)).asyncBlobUpload(any(WriteContext.class), any(ActionListener.class));
    }

    public void testSetBasePathWithValidCharacters() {
        remoteRepositoryExporter.setBasePath("query-insights/path-with_special.chars*()");
        assertEquals("query-insights/path-with_special.chars*()", remoteRepositoryExporter.getBasePath());
    }

    public void testSetBasePathWithInvalidCharacters() {
        assertThrows(IllegalArgumentException.class, () -> { remoteRepositoryExporter.setBasePath("query-insights/invalid@path"); });
    }

    public void testSetBasePathWithNull() {
        remoteRepositoryExporter.setBasePath(null);
        assertNull(remoteRepositoryExporter.getBasePath());
    }

    public void testInitializationWithNonAsyncBlobContainer() {
        when(repositoriesService.repository("non-async-repo")).thenReturn(blobStoreRepository);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);

        assertThrows(IllegalArgumentException.class, () -> {
            new RemoteRepositoryExporter(
                () -> repositoriesService,
                clusterService,
                "non-async-repo",
                "query-insights",
                DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm'UTC'", Locale.ROOT),
                "test-exporter",
                true
            );
        });
    }

    public void testSetRepositoryNameWithNonAsyncBlobContainer() {
        when(repositoriesService.repository("non-async-repo")).thenReturn(blobStoreRepository);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);

        assertThrows(IllegalArgumentException.class, () -> {
            remoteRepositoryExporter.setRepositoryName("non-async-repo");
        });
    }
}
