/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.io.IOException;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

/** Rest Action tests for query */
public class QueryInsightsExporterIT extends QueryInsightsRestTestCase {

    /**
     * Test Top Queries setting endpoints with document creation, search, local index exporter control,
     * and window size configuration.
     *
     * @throws IOException IOException
     */
    public void testQueryInsightsExporterSettings() throws IOException, InterruptedException {
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
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(defaultExporterSettings());
        Response response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        setLatencyWindowSize("1m");
        waitForWindowToPass(60);
        checkLocalIndices("After enabling exporter");
        disableLocalIndexExporter();
        checkLocalIndices("After disabling exporter");
        reEnableLocalIndexExporter();
        checkLocalIndices("After re-enabling exporter");
        performSearch();
        checkLocalIndices("After search and waiting for data export");
        setLocalIndexToDebug();
        checkLocalIndices("After setting local index to debug");
    }

    private void createDocument() throws IOException {
        String documentJson = "{\n"
            + "  \"title\": \"Test Document\",\n"
            + "  \"content\": \"This is a test document for OpenSearch\"\n"
            + "}";

        Request createDocumentRequest = new Request("POST", "/my-index-0/_doc/");
        createDocumentRequest.setJsonEntity(documentJson);
        Response response = client().performRequest(createDocumentRequest);
        assertEquals(201, response.getStatusLine().getStatusCode());
    }

    private void performSearch() throws IOException {
        String searchJson = "{\n"
            + "  \"query\": {\n"
            + "    \"match\": {\n"
            + "      \"content\": \"test\"\n"
            + "    }\n"
            + "  }\n"
            + "}";
        Request searchRequest = new Request("POST", "/my-index-0/_search");
        searchRequest.setJsonEntity(searchJson);
        Response response = client().performRequest(searchRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void checkLocalIndices(String context) throws IOException {
        Request indicesRequest = new Request("GET", "/_cat/indices?v");
        Response response = client().performRequest(indicesRequest);
        System.out.println(context + ": " + response.getEntity().getContent());
    }

    private void disableLocalIndexExporter() throws IOException {
        String disableExporterJson = "{\n"
            + "  \"persistent\": {\n"
            + "    \"search.insights.top_queries.exporter.type\": \"none\"\n"
            + "  }\n"
            + "}";
        Request disableExporterRequest = new Request("PUT", "/_cluster/settings");
        disableExporterRequest.setJsonEntity(disableExporterJson);
        Response response = client().performRequest(disableExporterRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void reEnableLocalIndexExporter() throws IOException {
        String enableExporterJson = "{\n"
            + "  \"persistent\": {\n"
            + "    \"search.insights.top_queries.exporter.type\": \"local_index\"\n"
            + "  }\n"
            + "}";
        Request enableExporterRequest = new Request("PUT", "/_cluster/settings");
        enableExporterRequest.setJsonEntity(enableExporterJson);
        Response response = client().performRequest(enableExporterRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void setLocalIndexToDebug() throws IOException {
        String debugExporterJson = "{\n"
            + "  \"persistent\": {\n"
            + "    \"search.insights.top_queries.exporter.type\": \"debug\"\n"
            + "  }\n"
            + "}";
        Request debugExporterRequest = new Request("PUT", "/_cluster/settings");
        debugExporterRequest.setJsonEntity(debugExporterJson);
        Response response = client().performRequest(debugExporterRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void setLatencyWindowSize(String windowSize) throws IOException {
        String windowSizeJson = "{\n"
            + "  \"persistent\": {\n"
            + "    \"search.insights.top_queries.latency.window_size\": \"" + windowSize + "\"\n"
            + "  }\n"
            + "}";
        Request windowSizeRequest = new Request("PUT", "/_cluster/settings");
        windowSizeRequest.setJsonEntity(windowSizeJson);
        Response response = client().performRequest(windowSizeRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void waitForWindowToPass(int seconds) throws InterruptedException {
        System.out.println("Waiting for " + seconds + " seconds for window size to pass...");
        Thread.sleep(seconds * 1000); // Sleep for the specified number of seconds
    }

    private String defaultExporterSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.exporter.type\" : \"local_index\"\n"
            + "    }\n"
            + "}";
    }

    private String[] invalidExporterSettings() {
        return new String[] {
            "{\n" + "    \"persistent\" : {\n" + "        \"search.insights.top_queries.exporter.type\" : invalid_type\n" + "    }\n" + "}",
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.exporter.type\" : local_index,\n"
                + "        \"search.insights.top_queries.exporter.config.index\" : \"1a2b\"\n"
                + "    }\n"
                + "}" };
    }
}
