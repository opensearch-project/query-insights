/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.opensearch.plugin.insights.core.service.TopQueriesService.isTopQueriesIndex;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_DELETE_AFTER_VALUE;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.service.TopQueriesService;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Local index exporter for exporting query insights data to local OpenSearch indices.
 */
public final class LocalIndexExporter implements QueryInsightsExporter {
    /**
     * Logger of the local index exporter
     */
    private final Logger logger = LogManager.getLogger();
    private final Client client;
    private DateTimeFormatter indexPattern;
    private int deleteAfter;
    private final String id;

    /**
     * Constructor of LocalIndexExporter
     *
     * @param client OS client
     * @param indexPattern the pattern of index to export to
     */
    public LocalIndexExporter(final Client client, final DateTimeFormatter indexPattern, final String id) {
        this.indexPattern = indexPattern;
        this.client = client;
        this.deleteAfter = DEFAULT_DELETE_AFTER_VALUE;
        this.id = id;
    }

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
    void setIndexPattern(DateTimeFormatter indexPattern) {
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
            final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setTimeout(TimeValue.timeValueMinutes(1));
            for (SearchQueryRecord record : records) {
                bulkRequestBuilder.add(
                    new IndexRequest(indexName).source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
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
        } catch (final Exception e) {
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
            logger.error("Unable to index query insights data: ", e);
        }
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
        return indexPattern.format(ZonedDateTime.now(ZoneOffset.UTC)) + "-" + generateLocalIndexDateHash();
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
     * Delete Top N local indices older than the configured data retention period
     *
     * @param indexMetadataMap Map of index name {@link String} to {@link IndexMetadata}
     */
    public void deleteExpiredTopNIndices(final Map<String, IndexMetadata> indexMetadataMap) {
        long expirationMillisLong = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(deleteAfter);
        for (Map.Entry<String, IndexMetadata> entry : indexMetadataMap.entrySet()) {
            String indexName = entry.getKey();
            if (isTopQueriesIndex(indexName) && entry.getValue().getCreationDate() <= expirationMillisLong) {
                // delete this index
                TopQueriesService.deleteSingleIndex(indexName, client);
            }
        }
    }

    /**
     * Generates a consistent 5-digit numeric hash based on the current UTC date.
     * The generated hash is deterministic, meaning it will return the same result for the same date.
     *
     * @return A 5-digit numeric string representation of the current date's hash.
     */
    public static String generateLocalIndexDateHash() {
        // Get the current date in UTC (yyyy-MM-dd format)
        String currentDate = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT)
            .format(Instant.now().atOffset(ZoneOffset.UTC).toLocalDate());

        // Generate a 5-digit numeric hash from the date's hashCode
        return String.format(Locale.ROOT, "%05d", (currentDate.hashCode() % 100000 + 100000) % 100000);
    }
}
