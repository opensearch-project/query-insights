/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.io.Closeable;
import java.util.List;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Base interface for Query Insights exporters
 */
public interface QueryInsightsExporter extends Closeable {
    /**
     * Export a list of SearchQueryRecord to the exporter sink
     *
     * @param records list of {@link SearchQueryRecord}
     */
    void export(final List<SearchQueryRecord> records);
}
