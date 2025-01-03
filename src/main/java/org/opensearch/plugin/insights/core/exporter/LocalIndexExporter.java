/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_DELETE_AFTER_VALUE;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
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

    /**
     * Constructor of LocalIndexExporter
     *
     * @param client OS client
     * @param indexPattern the pattern of index to export to
     */
    public LocalIndexExporter(final Client client, final DateTimeFormatter indexPattern) {
        this.indexPattern = indexPattern;
        this.client = client;
        this.deleteAfter = DEFAULT_DELETE_AFTER_VALUE;
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
        if (records == null || records.size() == 0) {
            return;
        }
        try {
            final String index = getDateTimeFromFormat();
            final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setTimeout(TimeValue.timeValueMinutes(1));
            for (SearchQueryRecord record : records) {
                bulkRequestBuilder.add(
                    new IndexRequest(index).source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
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

    private String getDateTimeFromFormat() {
        return indexPattern.format(ZonedDateTime.now(ZoneOffset.UTC));
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
    public void deleteExpiredIndices(final Map<String, IndexMetadata> indexMetadataMap) {
        long expirationMillisLong = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(deleteAfter);
        for (Map.Entry<String, IndexMetadata> entry : indexMetadataMap.entrySet()) {
            String indexName = entry.getKey();
            if (!matchesPattern(indexName, indexPattern)) {
                continue;
            }
            if (entry.getValue().getCreationDate() <= expirationMillisLong) {
                // delete this index
                client.admin().indices().delete(new DeleteIndexRequest(indexName));
            }
        }
    }

    /**
     * Checks if the input string matches the given DateTimeFormatter pattern.
     *
     * @param input The input string to check.
     * @param formatter The DateTimeFormatter to validate the string against.
     * @return true if the string matches the pattern, false otherwise.
     */
    static boolean matchesPattern(final String input, final DateTimeFormatter formatter) {
        try {
            // Try parsing the input with the given formatter
            formatter.parse(input);
            return true;  // String matches the pattern
        } catch (Exception e) {
            return false;  // String does not match the pattern
        }
    }
}
