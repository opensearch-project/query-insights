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
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
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

/**
 * Remote repository exporter for exporting query insights data to blob store repositories.
 * Supports S3, Azure Blob Storage, Google Cloud Storage, and other blob store types.
 * Operates independently of other exporters.
 */
public class RemoteRepositoryExporter implements QueryInsightsExporter {
    private final Logger logger = LogManager.getLogger();
    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private String repositoryName;
    private String basePath;
    private final DateTimeFormatter dateTimeFormatter;
    private final String id;
    private boolean enabled = false;

    /**
     * Constructor
     *
     * @param repositoriesServiceSupplier supplier for repositories service
     * @param repositoryName     repository name (S3, Azure, GCS, etc.)
     * @param basePath          base path for organizing files (e.g., "query-insights/ACCOUNT/DOMAIN")
     * @param dateTimeFormatter date time formatter for repository path
     * @param id                exporter id
     */
    public RemoteRepositoryExporter(
        final Supplier<RepositoriesService> repositoriesServiceSupplier,
        final String repositoryName,
        final String basePath,
        final DateTimeFormatter dateTimeFormatter,
        final String id
    ) {
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.repositoryName = repositoryName;
        this.basePath = basePath;
        this.dateTimeFormatter = dateTimeFormatter;
        this.id = id;
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
        if (!enabled || records == null || records.isEmpty() || repositoryName == null || repositoryName.isEmpty()) {
            return;
        }
        try {
            BlobContainer blobContainer = getBlobContainer();
            if (!(blobContainer instanceof AsyncMultiStreamBlobContainer)) {
                logger.warn("Repository does not support async upload, skipping export");
                return;
            }
            uploadAsync((AsyncMultiStreamBlobContainer) blobContainer, records);
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

        String fileName = buildRepositoryPath();
        StreamContext streamContext = new StreamContext(
            (partNo, size, position) -> new InputStreamContainer(new ByteArrayInputStream(data), data.length, 0),
            data.length,
            data.length,
            1
        );

        WriteContext writeContext = new WriteContext.Builder().fileName(fileName)
            .streamContextSupplier(partSize -> streamContext)
            .fileSize((long) data.length)
            .failIfAlreadyExists(false)
            .writePriority(WritePriority.NORMAL)
            .uploadFinalizer(bool -> {})
            .build();

        asyncContainer.asyncBlobUpload(writeContext, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to upload to remote repository: {}", fileName, e);
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.REMOTE_REPOSITORY_EXPORTER_UPLOAD_FAILURES);
            }
        });
    }

    /**
     * Get the blob container for repository operations
     */
    private BlobContainer getBlobContainer() {
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
        BlobPath blobPath = BlobPath.cleanPath().add(basePath).add("top-queries");
        return blobStoreRepository.blobStore().blobContainer(blobPath);
    }

    /**
     * Build repository path: yyyy/MM/dd/HH/mm'UTC'
     */
    private String buildRepositoryPath() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        return dateTimeFormatter.format(now);
    }

    @Override
    public void close() {}

    /**
     * Set repository name
     */
    public void setRepositoryName(String repositoryName) {
        this.repositoryName = repositoryName;
    }

    /**
     * Get repository name
     */
    public String getRepositoryName() {
        return repositoryName;
    }

    /**
     * Set base path
     */
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    /**
     * Get base path
     */
    public String getBasePath() {
        return basePath;
    }

    /**
     * Set enabled state
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Get enabled state
     */
    public boolean isEnabled() {
        return enabled;
    }
}
