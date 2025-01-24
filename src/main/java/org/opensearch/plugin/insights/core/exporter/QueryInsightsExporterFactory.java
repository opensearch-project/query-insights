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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;

/**
 * Factory class for validating and creating exporters based on provided settings
 */
public class QueryInsightsExporterFactory {
    /**
     * Logger of the query insights exporter factory
     */
    private final Logger logger = LogManager.getLogger();
    final private Client client;
    final private Map<String, QueryInsightsExporter> exporters;

    /**
     * Constructor of QueryInsightsExporterFactory
     *
     * @param client OS client
     */
    public QueryInsightsExporterFactory(final Client client) {
        this.client = client;
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
     * Create an exporter based on provided parameters
     *
     * @param type The type of exporter to create
     * @param indexPattern the index pattern if creating a index exporter
     * @return QueryInsightsExporter the created exporter sink
     */
    public QueryInsightsExporter createExporter(String id, SinkType type, String indexPattern) {
        if (SinkType.LOCAL_INDEX.equals(type)) {
            QueryInsightsExporter exporter = new LocalIndexExporter(client, DateTimeFormatter.ofPattern(indexPattern, Locale.ROOT), id);
            this.exporters.put(id, exporter);
            return exporter;
        }
        return DebugExporter.getInstance();
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
            ((LocalIndexExporter) exporter).setIndexPattern(DateTimeFormatter.ofPattern(indexPattern, Locale.ROOT));
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
        for (QueryInsightsExporter exporter : exporters.values()) {
            try {
                closeExporter(exporter);
            } catch (IOException e) {
                logger.error("Fail to close query insights exporter, error: ", e);
            }
        }
    }
}
