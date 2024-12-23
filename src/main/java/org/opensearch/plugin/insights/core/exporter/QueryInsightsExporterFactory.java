/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_TOP_QUERIES_EXPORTER_TYPE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.EXPORTER_TYPE;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
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
    final private Set<QueryInsightsExporter> exporters;

    /**
     * Constructor of QueryInsightsExporterFactory
     *
     * @param client OS client
     */
    public QueryInsightsExporterFactory(final Client client) {
        this.client = client;
        this.exporters = new HashSet<>();
    }

    /**
     * Validate exporter sink config
     *
     * @param settings exporter sink config {@link Settings}
     * @throws IllegalArgumentException if provided exporter sink config settings are invalid
     */
    public void validateExporterConfig(final Settings settings) throws IllegalArgumentException {
        // Disable exporter if the EXPORTER_TYPE setting is null
        if (settings.get(EXPORTER_TYPE) == null) {
            return;
        }
        SinkType type;
        try {
            type = SinkType.parse(settings.get(EXPORTER_TYPE, DEFAULT_TOP_QUERIES_EXPORTER_TYPE));
        } catch (IllegalArgumentException e) {
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.INVALID_EXPORTER_TYPE_FAILURES);
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Invalid exporter type [%s], type should be one of %s",
                    settings.get(EXPORTER_TYPE),
                    SinkType.allSinkTypes()
                )
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
    public QueryInsightsExporter createExporter(SinkType type, String indexPattern) {
        if (SinkType.LOCAL_INDEX.equals(type)) {
            QueryInsightsExporter exporter = new LocalIndexExporter(client, DateTimeFormatter.ofPattern(indexPattern, Locale.ROOT));
            this.exporters.add(exporter);
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
     * Close an exporter
     *
     * @param exporter the exporter to close
     * @throws IOException exception
     */
    public void closeExporter(QueryInsightsExporter exporter) throws IOException {
        if (exporter != null) {
            exporter.close();
            this.exporters.remove(exporter);
        }
    }

    /**
     * Close all exporters
     *
     */
    public void closeAllExporters() {
        for (QueryInsightsExporter exporter : exporters) {
            try {
                closeExporter(exporter);
            } catch (IOException e) {
                logger.error("Fail to close query insights exporter, error: ", e);
            }
        }
    }
}
