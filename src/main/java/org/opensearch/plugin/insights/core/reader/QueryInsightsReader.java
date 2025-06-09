/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.reader;

import java.io.Closeable;
import java.util.List;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Base interface for Query Insights readers
 */
public interface QueryInsightsReader extends Closeable {
    /**
     * Reader a list of SearchQueryRecord
     *
     * @param from       string
     * @param to         string
     * @param id         query/group id
     * @param verbose    whether to return full output
     * @param metricType metric type to read
     * @param listener   listener to be called when the read operation is complete
     */
    void read(
        final String from,
        final String to,
        final String id,
        final Boolean verbose,
        final MetricType metricType,
        final ActionListener<List<SearchQueryRecord>> listener
    );

    String getId();
}
