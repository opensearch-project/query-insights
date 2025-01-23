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
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Base interface for Query Insights readers
 */
public interface QueryInsightsReader extends Closeable {
    /**
     * Reader a list of SearchQueryRecord
     *
     * @param from string
     * @param to   string
     * @param id query/group id
     * @return List of SearchQueryRecord
     */
    List<SearchQueryRecord> read(final String from, final String to, final String id);
}
