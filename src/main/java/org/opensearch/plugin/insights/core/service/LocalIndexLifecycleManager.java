/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import static org.opensearch.plugin.insights.core.service.TopQueriesService.isTopQueriesIndex;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.utils.IndexDiscoveryHelper;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * Class to manage the lifecycle of Query Insights local indices
 * This includes retention
 */
class LocalIndexLifecycleManager {
    private final ThreadPool threadPool;
    private final Client client;
    private int deleteAfter;

    private static final Logger logger = LogManager.getLogger(LocalIndexLifecycleManager.class);

    /**
     * LocalIndexLifecycleManager Constructor
     *
     * @param threadPool The OpenSearch thread pool to run async tasks
     * @param client OS client
     */
    LocalIndexLifecycleManager(final ThreadPool threadPool, final Client client, final int deleteAfter) {
        this.threadPool = threadPool;
        this.client = client;
        setDeleteAfter(deleteAfter);
    }

    /**
     * Set local index data retention period
     *
     * @param deleteAfter the number of days after which Top N local indices should be deleted
     */
    void setDeleteAfter(final int deleteAfter) {
        logger.info("Setting Query Insights delete after to [{}]", deleteAfter);
        this.deleteAfter = deleteAfter;
    }

    /**
     * Get local index data retention period
     *
     * @return the number of days after which Top N local indices should be deleted
     */
    int getDeleteAfter() {
        return deleteAfter;
    }

    /**
     * Set delete after, then delete expired Top N indices
     *
     * @param deleteAfter the number of days after which Top N local indices should be deleted
     */
    void setDeleteAfterAndDelete(final int deleteAfter) {
        setDeleteAfter(deleteAfter);

        // Immediately delete all local indices when user sets value to '0'
        if (deleteAfter == 0) {
            deleteAllTopNIndices();
        } else {
            deleteExpiredTopNIndices();
        }
    }

    /**
     * Validate the delete after value
     *
     * @param deleteAfter the number of days after which Top N local indices should be deleted
     */
    void validateDeleteAfter(final int deleteAfter) {
        if (deleteAfter < QueryInsightsSettings.MIN_DELETE_AFTER_VALUE || deleteAfter > QueryInsightsSettings.MAX_DELETE_AFTER_VALUE) {
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.INVALID_EXPORTER_TYPE_FAILURES);
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Invalid delete_after_days setting [%d], value should be an integer between %d and %d.",
                    deleteAfter,
                    QueryInsightsSettings.MIN_DELETE_AFTER_VALUE,
                    QueryInsightsSettings.MAX_DELETE_AFTER_VALUE
                )
            );
        }
    }

    /**
     * Delete Top N local indices older than the configured data retention period
     */
    void deleteExpiredTopNIndices() {
        threadPool.executor(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR).execute(() -> {
            try {
                final ClusterStateRequest clusterStateRequest = IndexDiscoveryHelper.createClusterStateRequest(
                    IndicesOptions.strictExpand()
                );

                client.admin().cluster().state(clusterStateRequest, ActionListener.wrap(clusterStateResponse -> {
                    final Map<String, IndexMetadata> indexMetadataMap = clusterStateResponse.getState().metadata().indices();
                    final long startOfTodayUtcMillis = LocalDateTime.now(ZoneOffset.UTC)    // Today at 00:00 UTC
                        .truncatedTo(ChronoUnit.DAYS)
                        .toInstant(ZoneOffset.UTC)
                        .toEpochMilli();
                    final long expirationMillisLong = startOfTodayUtcMillis - TimeUnit.DAYS.toMillis(getDeleteAfter());
                    for (Map.Entry<String, IndexMetadata> entry : indexMetadataMap.entrySet()) {
                        String indexName = entry.getKey();
                        if (isTopQueriesIndex(indexName, entry.getValue()) && entry.getValue().getCreationDate() < expirationMillisLong) {
                            // delete this index
                            deleteSingleIndex(indexName, client);
                        }
                    }
                }, exception -> { logger.error("Error while deleting expired top_queries-* indices: ", exception); }));
            } catch (Exception exception) {
                logger.error("Error while deleting expired top_queries-* indices: ", exception);
            }
        });
    }

    /**
     * Deletes the specified index and logs any failure that occurs during the operation.
     *
     * @param indexName The name of the index to delete.
     * @param client    The OpenSearch client used to perform the deletion.
     */
    void deleteSingleIndex(String indexName, Client client) {
        client.admin().indices().delete(new DeleteIndexRequest(indexName), new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                logger.info("Deleted Query Insights index [{}]", indexName);
            }

            @Override
            public void onFailure(Exception e) {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof IndexNotFoundException) {
                    return;
                }
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_DELETE_FAILURES);
                logger.error("Failed to delete Query Insights index [{}]", indexName, e);
            }
        });
    }

    /**
     * Deletes all Top N local indices
     */
    void deleteAllTopNIndices() {
        final ClusterStateRequest clusterStateRequest = IndexDiscoveryHelper.createClusterStateRequest(IndicesOptions.strictExpand());

        client.admin().cluster().state(clusterStateRequest, ActionListener.wrap(clusterStateResponse -> {
            clusterStateResponse.getState()
                .metadata()
                .indices()
                .entrySet()
                .stream()
                .filter(entry -> isTopQueriesIndex(entry.getKey(), entry.getValue()))
                .forEach(entry -> deleteSingleIndex(entry.getKey(), client));
        }, exception -> { logger.error("Error while deleting Query Insights local indices: ", exception); }));
    }
}
