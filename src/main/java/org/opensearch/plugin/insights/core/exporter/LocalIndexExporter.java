/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_DELETE_AFTER_VALUE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_TEMPLATE_PRIORITY;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_QUERIES_INDEX_PATTERN_GLOB;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.utils.IndexDiscoveryHelper;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Local index exporter for exporting query insights data to local OpenSearch indices.
 */
public class LocalIndexExporter implements QueryInsightsExporter {
    /**
     * Logger of the local index exporter
     */
    private final Logger logger = LogManager.getLogger();
    private final Client client;
    private final ClusterService clusterService;
    private final String indexMapping;
    private DateTimeFormatter indexPattern;
    private int deleteAfter;
    private final String id;
    private static final int DEFAULT_NUMBER_OF_SHARDS = 1;
    private static final String DEFAULT_AUTO_EXPAND_REPLICAS = "0-2";
    private static final String TEMPLATE_NAME = "query_insights_top_queries_template";
    private long templatePriority;

    /**
     * Constructor
     *
     * @param client         client instance
     * @param clusterService cluster service
     * @param indexPattern   index pattern
     * @param indexMapping   index mapping
     * @param id             exporter id
     */
    public LocalIndexExporter(
        final Client client,
        final ClusterService clusterService,
        final DateTimeFormatter indexPattern,
        final String indexMapping,
        final String id
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.indexPattern = indexPattern;
        this.indexMapping = indexMapping;
        this.id = id;
        this.deleteAfter = DEFAULT_DELETE_AFTER_VALUE;
        this.templatePriority = DEFAULT_TEMPLATE_PRIORITY;
    }

    /**
     * Retrieves the identifier for the local index exporter.
     * <p>
     * Each service can either have its own dedicated local index exporter or share
     * an existing one. This identifier is used by the QueryInsightsExporterFactory
     * to locate and manage the appropriate exporter instance.
     *
     * @return The identifier of the local index exporter
     * @see QueryInsightsExporterFactory
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * Getter of indexPattern
     *
     * @return indexPattern
     */
    public DateTimeFormatter getIndexPattern() {
        return indexPattern;
    }

    /**
     * Setter of indexPattern
     *
     * @param indexPattern index pattern
     */
    public void setIndexPattern(DateTimeFormatter indexPattern) {
        this.indexPattern = indexPattern;
    }

    /**
     * Export a list of SearchQueryRecord to a local index
     *
     * @param records list of {@link SearchQueryRecord}
     */
    @Override
    public void export(final List<SearchQueryRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        try {
            final String indexName = buildLocalIndexName();
            if (!checkIndexExists(indexName)) {
                // First ensure the template exists, then create the index
                ensureTemplateExists().whenComplete((templateCreated, templateException) -> {
                    if (templateException != null) {
                        logger.error("Error ensuring template exists:", templateException);
                        OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
                    }

                    // Proceed with index creation even if there was a template error
                    // The template might already exist or might be created by another node
                    try {
                        createIndexAndBulk(indexName, records);
                    } catch (IOException ioe) {
                        logger.error("Error creating index:", ioe);
                        OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
                    }
                });
            } else {
                // Index already exists, proceed with bulk export
                bulk(indexName, records);
            }
        } catch (IOException e) {
            logger.error("Unable to export query insights data:", e);
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
        }
    }

    /**
     * Creates an index with the specified name and exports records to it
     *
     * @param indexName Name of the index to create
     * @param records Records to export
     * @throws IOException If there's an error reading mappings
     */
    void createIndexAndBulk(String indexName, List<SearchQueryRecord> records) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

        createIndexRequest.settings(
            Settings.builder()
                .put("index.number_of_shards", DEFAULT_NUMBER_OF_SHARDS)
                .put("index.auto_expand_replicas", DEFAULT_AUTO_EXPAND_REPLICAS)
        );
        createIndexRequest.mapping(readIndexMappings());

        client.admin().indices().create(createIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                if (createIndexResponse.isAcknowledged()) {
                    try {
                        bulk(indexName, records);
                    } catch (IOException e) {
                        OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
                        logger.error("Unable to index query insights data: ", e);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof ResourceAlreadyExistsException) {
                    try {
                        bulk(indexName, records);
                    } catch (IOException ioe) {
                        OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
                        logger.error("Unable to index query insights data: ", ioe);
                    }
                } else {
                    OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
                    logger.error("Unable to create query insights index: ", cause);
                }
            }
        });
    }

    void bulk(final String indexName, final List<SearchQueryRecord> records) throws IOException {
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setTimeout(TimeValue.timeValueMinutes(1));
        for (SearchQueryRecord record : records) {
            bulkRequestBuilder.add(
                new IndexRequest(indexName).id(record.getId())
                    .source(record.toXContentForExport(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }
        bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_BULK_FAILURES);
                logger.error("Failed to execute bulk operation for query insights data: ", e);
            }
        });
    }

    /**
     * Close the exporter sink
     */
    @Override
    public void close() {
        logger.debug("Closing the LocalIndexExporter..");
    }

    /**
     * Builds the local index name using the current UTC datetime
     *
     * @return A string representing the index name in the format "top_queries-YYYY.MM.dd-01234".
     */
    String buildLocalIndexName() {
        ZonedDateTime currentTime = ZonedDateTime.now(ZoneOffset.UTC);
        return IndexDiscoveryHelper.buildLocalIndexName(indexPattern, currentTime);
    }

    /**
     * Set local index exporter data retention period
     *
     * @param deleteAfter the number of days after which Top N local indices should be deleted
     */
    public void setDeleteAfter(final int deleteAfter) {
        this.deleteAfter = deleteAfter;
    }

    /**
     * Get local index exporter data retention period
     *
     * @return the number of days after which Top N local indices should be deleted
     */
    public int getDeleteAfter() {
        return deleteAfter;
    }

    /**
     * Deletes the specified index and logs any failure that occurs during the operation.
     *
     * @param indexName The name of the index to delete.
     * @param client    The OpenSearch client used to perform the deletion.
     */
    public void deleteSingleIndex(String indexName, Client client) {
        Logger logger = LogManager.getLogger();
        client.admin().indices().delete(new DeleteIndexRequest(indexName), new ActionListener<>() {
            @Override
            // CS-SUPPRESS-SINGLE: RegexpSingleline It is not possible to use phrase "cluster manager" instead of master here
            public void onResponse(org.opensearch.action.support.master.AcknowledgedResponse acknowledgedResponse) {}

            @Override
            public void onFailure(Exception e) {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof IndexNotFoundException) {
                    return;
                }
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_DELETE_FAILURES);
                logger.error("Failed to delete index '{}': ", indexName, e);
            }
        });
    }

    /**
     * Check if an index exists
     *
     * @param indexName Name of the index to check
     * @return true if the index exists, false otherwise
     */
    boolean checkIndexExists(String indexName) {
        ClusterState clusterState = clusterService.state();
        return clusterState.getRoutingTable().hasIndex(indexName);
    }

    /**
     * Read index mappings from the provided mapping string or from the default resource file
     *
     * @return String containing the index mappings
     * @throws IOException If there's an error reading the mappings
     */
    String readIndexMappings() throws IOException {
        if (indexMapping == null || indexMapping.isEmpty()) {
            return "{}";
        }

        // Check if this is a resource path or direct content
        if (indexMapping.endsWith(".json")) {
            return new String(
                Objects.requireNonNull(LocalIndexExporter.class.getClassLoader().getResourceAsStream(indexMapping)).readAllBytes(),
                Charset.defaultCharset()
            );
        }

        return indexMapping;
    }

    /**
     * Ensures that the template exists. This method first checks if the template exists and
     * only creates it if it doesn't.
     *
     * @return CompletableFuture that completes when the template check/creation is done
     */
    CompletableFuture<Boolean> ensureTemplateExists() {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        // First check if the template already exists
        GetComposableIndexTemplateAction.Request getRequest = new GetComposableIndexTemplateAction.Request();

        client.execute(GetComposableIndexTemplateAction.INSTANCE, getRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetComposableIndexTemplateAction.Response response) {
                // If the template exists and priority has not been changed, we don't need to create/update it
                if (response.indexTemplates().containsKey(TEMPLATE_NAME)
                    && response.indexTemplates().get(TEMPLATE_NAME).priority() == templatePriority) {
                    logger.debug("Template [{}] already exists, skipping creation", TEMPLATE_NAME);
                    future.complete(true);
                    return;
                }

                // Template doesn't exist, create it
                createTemplate(future);
            }

            @Override
            public void onFailure(Exception e) {
                // If we can't retrieve the template info, try creating it anyway
                logger.warn("Failed to check if template [{}] exists: {}", TEMPLATE_NAME, e.getMessage());
                createTemplate(future);
            }
        });

        return future;
    }

    /**
     * Helper method to create the template
     *
     * @param future The CompletableFuture to complete when done
     */
    void createTemplate(CompletableFuture<Boolean> future) {
        try {
            // Create a V2 template (ComposableIndexTemplate)
            CompressedXContent compressedMapping = new CompressedXContent(readIndexMappings());

            // Create template component
            org.opensearch.cluster.metadata.Template template = new org.opensearch.cluster.metadata.Template(
                Settings.builder()
                    .put("index.number_of_shards", DEFAULT_NUMBER_OF_SHARDS)
                    .put("index.auto_expand_replicas", DEFAULT_AUTO_EXPAND_REPLICAS)
                    .build(),
                compressedMapping,
                null
            );

            // Create the composable template
            ComposableIndexTemplate composableTemplate = new ComposableIndexTemplate(
                Collections.singletonList(TOP_QUERIES_INDEX_PATTERN_GLOB),
                template,
                null,
                templatePriority, // Priority using configured value
                null,
                null
            );

            // Use the V2 API to put the template
            PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(TEMPLATE_NAME).indexTemplate(
                composableTemplate
            );

            client.execute(PutComposableIndexTemplateAction.INSTANCE, request, new ActionListener<>() {
                @Override
                // CS-SUPPRESS-SINGLE: RegexpSingleline It is not possible to use phrase "cluster manager" instead of master here
                public void onResponse(org.opensearch.action.support.master.AcknowledgedResponse response) {
                    if (response.isAcknowledged()) {
                        logger.info("Successfully created or updated template [{}] with priority {}", TEMPLATE_NAME, templatePriority);
                        future.complete(true);
                    } else {
                        logger.warn("Failed to create or update template [{}]", TEMPLATE_NAME);
                        future.complete(false);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Error creating or updating template [{}]", TEMPLATE_NAME, e);
                    OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
                    future.completeExceptionally(e);
                }
            });
        } catch (Exception e) {
            logger.error("Failed to manage template", e);
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
            future.completeExceptionally(e);
        }
    }

    /**
     * Set the template priority for the exporter
     *
     * @param templatePriority New template priority value
     */
    public void setTemplatePriority(final long templatePriority) {
        this.templatePriority = templatePriority;
    }

    /**
     * Get the current template priority
     *
     * @return Current template priority value
     */
    public long getTemplatePriority() {
        return templatePriority;
    }

}
