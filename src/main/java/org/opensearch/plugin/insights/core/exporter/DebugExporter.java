/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Debug exporter for development purpose
 */
public class DebugExporter implements QueryInsightsExporter {
    /**
     * Logger of the debug exporter
     */
    private final Logger logger = LogManager.getLogger();
    private static final String DEBUG_EXPORTER_ID = "debug_exporter";

    /**
     * Constructor of DebugExporter
     */
    private DebugExporter() {}

    @Override
    public String getId() {
        return DEBUG_EXPORTER_ID;
    }

    /**
     * Singleton holder class for the DebugExporter instance.
     * A single DebugExporter instance is shared across all services, using the default
     * debug exporter identifier EXPORTER_ID.
     */
    private static class InstanceHolder {
        private static final DebugExporter INSTANCE = new DebugExporter();
    }

    /**
     Get the singleton instance of DebugExporter
     *
     @return DebugExporter instance
     */
    public static DebugExporter getInstance() {
        return InstanceHolder.INSTANCE;
    }

    /**
     * Write the list of SearchQueryRecord to debug log
     *
     * @param records list of {@link SearchQueryRecord}
     */
    @Override
    public void export(final List<SearchQueryRecord> records) {
        logger.debug("QUERY_INSIGHTS_RECORDS: " + records.toString());
    }

    /**
     * Close the debugger exporter sink
     */
    @Override
    public void close() {
        logger.debug("Closing the DebugExporter..");
    }
}
