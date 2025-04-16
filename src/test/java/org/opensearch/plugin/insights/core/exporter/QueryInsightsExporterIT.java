/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

/** Rest Action tests for query */
public class QueryInsightsExporterIT extends QueryInsightsRestTestCase {

    public void testQueryInsightsExporterSettings() throws Exception {
        createDocument();
        for (String setting : invalidExporterSettings()) {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(setting);
            try {
                client().performRequest(request);
                fail("Should not succeed with invalid exporter settings");
            } catch (ResponseException e) {
                assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
            }
        }
        defaultExporterSettings();// Enabling Local index Setting
        performSearch();
        setLatencyWindowSize("1m");
        Thread.sleep(70000); // Allow time for export to local index
        checkLocalIndices();
        checkQueryInsightsIndexTemplate();
        cleanupIndextemplate();
        disableLocalIndexExporter();
        defaultExporterSettings();// Re-enabling the Local Index
        setLocalIndexToDebug();// Ensuring it is able to toggle Local to Debug
        cleanup();
    }
}
