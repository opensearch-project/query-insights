/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.reader;

import java.io.IOException;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;


public class QueryInsightsReaderIT extends QueryInsightsRestTestCase {

    public void testQueryInsightsHistoricalTopQueriesRead() throws IOException, InterruptedException {
        createDocument();
        defaultExporterSettings();
        setLatencyWindowSize("1m");
        performSearch();
        Thread.sleep(70000);// Wait time for the window to pass and create Local Index
        checkLocalIndices();
        fetchHistoricalTopQueries();
        cleanup();
    }
}
