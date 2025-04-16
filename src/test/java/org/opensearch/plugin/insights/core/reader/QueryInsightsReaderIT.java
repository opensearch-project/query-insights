/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.reader;

import java.io.IOException;
import java.util.List;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

public class QueryInsightsReaderIT extends QueryInsightsRestTestCase {

    public void testQueryInsightsHistoricalTopQueriesRead() throws IOException, InterruptedException {
        try {
            createDocument();
            defaultExporterSettings();
            setLatencyWindowSize("1m");
            performSearch();
            Thread.sleep(80000);
            checkLocalIndices();
            List<String[]> allPairs = fetchHistoricalTopQueries("null", "null", "null");
            assertFalse("Expected at least one top query", allPairs.isEmpty());
            String selectedId = allPairs.get(0)[0];
            String selectedNodeId = allPairs.get(0)[1];
            List<String[]> filteredPairs = fetchHistoricalTopQueries(selectedId, "null", "null");
            List<String[]> filteredPairs1 = fetchHistoricalTopQueries("null", selectedNodeId, "null");
            List<String[]> filteredPairs2 = fetchHistoricalTopQueries(selectedId, selectedNodeId, "null");
            List<String[]> filteredPairs3 = fetchHistoricalTopQueries(selectedId, selectedNodeId, "latency");

        } catch (Exception e) {
            fail("Test failed with exception: " + e.getMessage());
        } finally {
            cleanup();
        }
    }

    public void testInvalidDateRangeParameters() throws IOException {
        String[] invalidEndpoints = new String[] {
            "/_insights/top_queries?from=2024-00-01T00:00:00.000Z&to=2024-04-07T00:00:00.000Z", // Invalid month
            "/_insights/top_queries?from=2024-13-01T00:00:00.000Z&to=2024-04-07T00:00:00.000Z", // Month out of range
            "/_insights/top_queries?from=abcd&to=efgh", // Not a date
            "/_insights/top_queries?from=&to=", // Empty values
            "/_insights/top_queries?from=2024-04-10T00:00:00Z", // Missing `to`
            "/_insights/top_queries?to=2024-04-10T00:00:00Z", // Missing `from`

            // Invalid metric type
            "/_insights/top_queries?from=2025-04-15T17:00:00.000Z&to=2025-04-15T18:00:00.000Z&type=Latency",
            "/_insights/top_queries?from=2025-04-15T17:00:00.000Z&to=2025-04-15T18:00:00.000Z&type=xyz",

            // Unexpected param
            "/_insights/top_queries?from=2025-04-15T17:00:00.000Z&to=2025-04-15T18:00:00.000Z&foo=bar",
            "/_insights/top_queries?from=2025-04-15T17:59:42.304Z&to=2025-04-15T20:39:42.304Zabdncmdkdkssmcmd", };

        for (String endpoint : invalidEndpoints) {
            runInvalidDateRequest(endpoint);
        }
    }

    private void runInvalidDateRequest(String endpoint) throws IOException {
        Request request = new Request("GET", endpoint);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    }
}
