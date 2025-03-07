/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.opensearch.plugin.insights.core.utils.ExporterReaderUtils.generateLocalIndexDateHash;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_DELETE_AFTER_VALUE;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
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
    private static final int DEFAULT_NUMBER_OF_REPLICA = 0;
    private static final int DEFAULT_NUMBER_OF_SHARDS = 1;
    private static final List<String> DEFAULT_SORTED_FIELDS = List.of(
        "measurements.latency.number",
        "measurements.cpu.number",
        "measurements.memory.number"
    );
    private static final List<String> DEFAULT_SORTED_ORDERS = List.of("desc", "desc", "desc");

    /**
     * Constructor of LocalIndexExporter
     *
     * @param client OS client
     * @param clusterService cluster service
     * @param indexPattern the pattern of index to export to
     * @param indexMapping the index mapping file
     * @param id id of the exporter
     */
    public LocalIndexExporter(
        final Client client,
        final ClusterService clusterService,
        final DateTimeFormatter indexPattern,
        final String indexMapping,
        final String id
    ) {
        this.indexPattern = indexPattern;
        this.client = client;
        this.clusterService = clusterService;
        this.indexMapping = indexMapping;
        this.deleteAfter = DEFAULT_DELETE_AFTER_VALUE;
        this.id = id;
    }

    /**
     * Retrieves the identifier for the local index exporter.
     *
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
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

                createIndexRequest.settings(
                    Settings.builder()
                        .putList("index.sort.field", DEFAULT_SORTED_FIELDS)
                        .putList("index.sort.order", DEFAULT_SORTED_ORDERS)
                        .put("index.number_of_shards", DEFAULT_NUMBER_OF_SHARDS)
                        .put("index.number_of_replicas", DEFAULT_NUMBER_OF_REPLICA)
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
                            } catch (IOException ex) {
                                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
                                logger.error("Unable to index query insights data: ", e);
                            }
                        } else {
                            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
                            logger.error("Unable to create query insights index: ", e);
                        }
                    }
                });
            } else {
                bulk(indexName, records);
            }
        } catch (final Exception e) {
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
            logger.error("Unable to index query insights data: ", e);
        }
    }

    private void bulk(final String indexName, final List<SearchQueryRecord> records) throws IOException {
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setTimeout(TimeValue.timeValueMinutes(1));
        for (SearchQueryRecord record : records) {
            bulkRequestBuilder.add(
                new IndexRequest(indexName).id(record.getId())
                    .source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
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
        return indexPattern.format(currentTime) + "-" + generateLocalIndexDateHash(currentTime.toLocalDate());
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
     * @param client The OpenSearch client used to perform the deletion.
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
     * check if index exists
     * @return boolean
     */
    private boolean checkIndexExists(String indexName) {
        ClusterState clusterState = clusterService.state();
        return clusterState.getRoutingTable().hasIndex(indexName);
    }

    /**
     * get correlation rule index mappings
     * @return mappings of correlation rule index
     * @throws IOException IOException
     */
    private String readIndexMappings() throws IOException {
        return new String(
            Objects.requireNonNull(LocalIndexExporter.class.getClassLoader().getResourceAsStream(indexMapping)).readAllBytes(),
            Charset.defaultCharset()
        );
    }

}
