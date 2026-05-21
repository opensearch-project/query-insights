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

    /**
     * Read a list of SearchQueryRecord with optional RBAC filtering pushed into the search query.
     *
     * @param from       start timestamp string
     * @param to         end timestamp string
     * @param id         query/group id
     * @param verbose    whether to return full output
     * @param metricType metric type to read
     * @param username   optional username for RBAC filtering
     * @param backendRoles optional backend roles for RBAC filtering
     * @param listener   listener to be called when the read operation is complete
     */
    default void read(
        final String from,
        final String to,
        final String id,
        final Boolean verbose,
        final MetricType metricType,
        final String username,
        final List<String> backendRoles,
        final ActionListener<List<SearchQueryRecord>> listener
    ) {
        read(from, to, id, verbose, metricType, listener);
    }

    String getId();
}
