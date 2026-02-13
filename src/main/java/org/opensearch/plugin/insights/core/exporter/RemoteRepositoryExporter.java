/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

/**
 * Remote repository exporter for exporting query insights data to blob store repositories.
 * Supports S3, Azure Blob Storage, Google Cloud Storage, and other blob store types.
 * Operates independently of other exporters.
 */
public class RemoteRepositoryExporter implements QueryInsightsExporter {
    private static final Logger logger = LogManager.getLogger(RemoteRepositoryExporter.class);
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 100;

    private static class RepositoryState {
        final String repositoryName;
        final String basePath;
        final AsyncMultiStreamBlobContainer blobContainer;

        RepositoryState(String repositoryName, String basePath, AsyncMultiStreamBlobContainer blobContainer) {
            this.repositoryName = repositoryName;
            this.basePath = basePath;
            this.blobContainer = blobContainer;
        }
    }

    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final DateTimeFormatter dateTimeFormatter;
    private final String id;
    private volatile Boolean enabled;
    private final AtomicReference<RepositoryState> repositoryState;

    /**
     * Constructor
     *
     * @param repositoriesServiceSupplier supplier for repositories service
     * @param clusterService cluster service
     * @param threadPool thread pool
     * @param repositoryName     repository name (S3, Azure, GCS, etc.)
     * @param basePath          base path for organizing files (e.g., "query-insights")
     * @param dateTimeFormatter date time formatter for repository path
     * @param id                exporter id
     */
    public RemoteRepositoryExporter(
        final Supplier<RepositoriesService> repositoriesServiceSupplier,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final String repositoryName,
        final String basePath,
        final DateTimeFormatter dateTimeFormatter,
        final String id,
        final boolean enabled
    ) {
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.dateTimeFormatter = dateTimeFormatter;
        this.id = id;
        this.enabled = enabled;
        if (repositoryName != null && !repositoryName.isEmpty()) {
            AsyncMultiStreamBlobContainer container = validateRepository(repositoryName, basePath);
            this.repositoryState = new AtomicReference<>(new RepositoryState(repositoryName, basePath, container));
        } else {
            this.repositoryState = new AtomicReference<>(new RepositoryState(repositoryName, basePath, null));
        }
    }

    @Override
    public String getId() {
        return id;
    }

    /**
     * Export a list of SearchQueryRecord to remote repository
     *
     * @param records list of {@link SearchQueryRecord}
     */
    @Override
    public void export(final List<SearchQueryRecord> records) {
        RepositoryState state = repositoryState.get();
        if (!enabled || records == null || records.isEmpty() || state.blobContainer == null) {
            return;
        }
        try {
            uploadAsync(state.blobContainer, records);
        } catch (Exception e) {
            logger.error("Failed to export query insights data to remote repository", e);
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.REMOTE_REPOSITORY_EXPORTER_EXCEPTIONS);
        }
    }

    /**
     * Uploads data to remote repository using async blob upload as a single part
     *
     * @param asyncContainer async multi-stream blob container
     * @param records list of records to upload
     * @throws IOException if upload fails
     */
    private void uploadAsync(AsyncMultiStreamBlobContainer asyncContainer, List<SearchQueryRecord> records) throws IOException {
        StringBuilder jsonBuilder = new StringBuilder();
        for (SearchQueryRecord record : records) {
            jsonBuilder.append(record.toXContentForExport(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).toString()).append("\n");
        }
        byte[] data = jsonBuilder.toString().getBytes(StandardCharsets.UTF_8);

        StreamContext streamContext = new StreamContext(
            (partNo, size, position) -> new InputStreamContainer(new ByteArrayInputStream(data), data.length, 0),
            data.length,
            data.length,
            1
        );

        String fileName = buildObjectKey();
        WriteContext writeContext = new WriteContext.Builder().fileName(fileName)
            .streamContextSupplier(partSize -> streamContext)
            .fileSize((long) data.length)
            .failIfAlreadyExists(false)
            .writePriority(WritePriority.NORMAL)
            .uploadFinalizer(bool -> {})
            .build();

        uploadAsyncWithRetry(asyncContainer, fileName, writeContext, 0);
    }

    private void uploadAsyncWithRetry(
        AsyncMultiStreamBlobContainer asyncContainer,
        String fileName,
        WriteContext writeContext,
        int retryCount
    ) throws IOException {

        final int currentRetry = retryCount;
        asyncContainer.asyncBlobUpload(writeContext, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                if (currentRetry < MAX_RETRIES) {
                    long backoffMs = INITIAL_BACKOFF_MS * (1L << currentRetry);
                    logger.warn("Upload failed, retrying in {}ms (attempt {}/{}): {}", backoffMs, currentRetry + 1, MAX_RETRIES, fileName);
                    threadPool.schedule(() -> {
                        try {
                            uploadAsyncWithRetry(asyncContainer, fileName, writeContext, currentRetry + 1);
                        } catch (IOException ex) {
                            logger.error("Failed to retry upload to remote repository: {}", fileName, ex);
                            OperationalMetricsCounter.getInstance()
                                .incrementCounter(OperationalMetric.REMOTE_REPOSITORY_EXPORTER_UPLOAD_FAILURES);
                        }
                    }, TimeValue.timeValueMillis(backoffMs), ThreadPool.Names.GENERIC);
                } else {
                    logger.error("Failed to upload to remote repository after {} retries: {}", MAX_RETRIES, fileName, e);
                    OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.REMOTE_REPOSITORY_EXPORTER_UPLOAD_FAILURES);
                }
            }
        });
    }

    /**
     * Validate repository exists and supports async upload
     */
    private AsyncMultiStreamBlobContainer validateRepository(String repositoryName, String path) {
        RepositoriesService repositoriesService = repositoriesServiceSupplier.get();
        if (repositoriesService == null) {
            throw new IllegalStateException("RepositoriesService is not available");
        }
        Repository repository = repositoriesService.repository(repositoryName);
        if (repository == null) {
            throw new RepositoryMissingException(repositoryName);
        }

        if (!(repository instanceof BlobStoreRepository)) {
            throw new IllegalArgumentException("Repository " + repositoryName + " is not a blob store repository");
        }

        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        BlobPath blobPath = BlobPath.cleanPath().add(path);
        BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(blobPath);

        if (!(blobContainer instanceof AsyncMultiStreamBlobContainer)) {
            throw new IllegalArgumentException("Repository " + repositoryName + " does not support async upload");
        }

        return (AsyncMultiStreamBlobContainer) blobContainer;
    }

    /**
     * Build object key: yyyy/MM/dd/HH/mm'UTC'/{node-id}.json
     */
    private String buildObjectKey() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String nodeId = clusterService.localNode().getId();
        return dateTimeFormatter.format(now) + "/" + nodeId + ".json";
    }

    @Override
    public void close() {}

    /**
     * Set repository name and validate it exists and supports async upload
     */
    public void setRepositoryName(String repositoryName) {
        repositoryState.updateAndGet(current -> {
            if (repositoryName != null && !repositoryName.isEmpty()) {
                AsyncMultiStreamBlobContainer container = validateRepository(repositoryName, current.basePath);
                return new RepositoryState(repositoryName, current.basePath, container);
            } else {
                return new RepositoryState(repositoryName, current.basePath, null);
            }
        });
    }

    /**
     * Get repository name
     */
    public String getRepositoryName() {
        return repositoryState.get().repositoryName;
    }

    /**
     * Set base path and validate it contains only allowed characters
     */
    public void setBasePath(String basePath) {
        if (basePath != null && !basePath.matches("[a-zA-Z0-9/!\\-_.*()']*")) {
            throw new IllegalArgumentException(
                "Base path contains invalid characters. Only alphanumeric, /, !, -, _, ., *, ', (, ) are allowed."
            );
        }
        repositoryState.updateAndGet(current -> {
            if (current.repositoryName != null && !current.repositoryName.isEmpty()) {
                AsyncMultiStreamBlobContainer container = validateRepository(current.repositoryName, basePath);
                return new RepositoryState(current.repositoryName, basePath, container);
            } else {
                return new RepositoryState(current.repositoryName, basePath, null);
            }
        });
    }

    /**
     * Get base path
     */
    public String getBasePath() {
        return repositoryState.get().basePath;
    }

    /**
     * Set enabled state
     */
    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Get enabled state
     */
    public boolean isEnabled() {
        return enabled;
    }
}
