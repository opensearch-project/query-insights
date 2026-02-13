/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * Factory class for validating and creating exporters based on provided settings
 */
public class QueryInsightsExporterFactory {
    /**
     * Logger of the query insights exporter factory
     */
    private final Logger logger = LogManager.getLogger();
    final private Client client;
    final private ClusterService clusterService;
    final private ThreadPool threadPool;
    final private Supplier<RepositoriesService> repositoriesServiceSupplier;
    /**
     * Maps exporter identifiers to their corresponding exporter sink instances.
     */
    final private Map<String, QueryInsightsExporter> exporters;

    /**
     * Constructor of QueryInsightsExporterFactory
     *
     * @param client OS client
     * @param clusterService cluster service
     * @param threadPool thread pool
     * @param repositoriesServiceSupplier supplier for repositories service
     */
    public QueryInsightsExporterFactory(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.exporters = new HashMap<>();
    }

    /**
     * Validate exporter sink config
     *
     * @param exporterType exporter sink type
     * @throws IllegalArgumentException if provided exporter sink config settings are invalid
     */
    public void validateExporterType(final String exporterType) throws IllegalArgumentException {
        // Disable exporter if the EXPORTER_TYPE setting is null
        if (exporterType == null) {
            return;
        }
        try {
            SinkType.parse(exporterType);
        } catch (IllegalArgumentException e) {
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.INVALID_EXPORTER_TYPE_FAILURES);
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Invalid exporter type [%s], type should be one of %s", exporterType, SinkType.allSinkTypes())
            );
        }
    }

    /**
     * Create a local index exporter based on provided parameters
     *
     * @param id id of the exporter so that exporters can be retrieved and reused across services
     * @param indexPattern the index pattern if creating an index exporter
     * @param indexMapping index mapping file
     * @return LocalIndexExporter the created exporter sink
     */
    public LocalIndexExporter createLocalIndexExporter(String id, String indexPattern, String indexMapping) {
        LocalIndexExporter exporter = new LocalIndexExporter(
            client,
            clusterService,
            DateTimeFormatter.ofPattern(indexPattern, Locale.ROOT),
            indexMapping,
            id
        );
        this.exporters.put(id, exporter);
        return exporter;
    }

    /**
     * Create a debug exporter based on provided parameters
     *
     * @param id id of the exporter so that exporters can be retrieved and reused across services
     * @return DebugExporter the created exporter sink
     */
    public DebugExporter createDebugExporter(String id) {
        DebugExporter debugExporter = DebugExporter.getInstance();
        this.exporters.put(id, debugExporter);
        return debugExporter;
    }

    /**
     * Create a remote exporter based on provided parameters
     *
     * @param id id of the exporter so that exporters can be retrieved and reused across services
     * @param repositoryName repository name (S3, Azure, GCS, etc.)
     * @param basePath base path for organizing files
     * @return RemoteRepositoryExporter the created exporter sink
     */
    public RemoteRepositoryExporter createRemoteRepositoryExporter(String id, String repositoryName, String basePath, Boolean enabled) {
        RemoteRepositoryExporter remoteRepositoryExporter = new RemoteRepositoryExporter(
            repositoriesServiceSupplier,
            clusterService,
            threadPool,
            repositoryName,
            basePath,
            DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm'UTC'", Locale.ROOT),
            id,
            enabled
        );
        this.exporters.put(id, remoteRepositoryExporter);
        return remoteRepositoryExporter;
    }

    /**
     * Update an exporter based on provided parameters
     *
     * @param exporter The exporter to update
     * @param indexPattern the index pattern if creating a index exporter
     * @return QueryInsightsExporter the updated exporter sink
     */
    public QueryInsightsExporter updateExporter(QueryInsightsExporter exporter, String indexPattern) {
        if (exporter.getClass() == LocalIndexExporter.class) {
            LocalIndexExporter localExporter = (LocalIndexExporter) exporter;
            localExporter.setIndexPattern(DateTimeFormatter.ofPattern(indexPattern, Locale.ROOT));
        }
        return exporter;
    }

    /**
     * Get a exporter by id
     * @param id The id of the exporter
     * @return QueryInsightsReader the Reader
     */
    public QueryInsightsExporter getExporter(String id) {
        return this.exporters.get(id);
    }

    /**
     * Close an exporter
     *
     * @param exporter the exporter to close
     * @throws IOException exception
     */
    public void closeExporter(QueryInsightsExporter exporter) throws IOException {
        if (exporter != null) {
            exporter.close();
            this.exporters.remove(exporter.getId());
        }
    }

    /**
     * Close all exporters
     *
     */
    public void closeAllExporters() {
        for (QueryInsightsExporter exporter : new ArrayList<>(exporters.values())) {
            try {
                closeExporter(exporter);
            } catch (IOException e) {
                logger.error("Fail to close query insights exporter, error: ", e);
            }
        }
    }
}
