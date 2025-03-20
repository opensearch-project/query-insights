/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

/** Rest Action tests for query */
public class QueryInsightsExporterIT extends QueryInsightsRestTestCase {

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
        performSearch();
        waitForWindowToPass(70);
        checkLocalIndices("After search and waiting for data export");
        checkQueryInsightsIndexTemplate();
        disableLocalIndexExporter();
        reEnableLocalIndexExporter();
        setLocalIndexToDebug();
    }

    private void performSearch() throws IOException {
        String searchJson = "{\n" + "  \"query\": {\n" + "    \"term\": {\n" + "      \"user.id\": \"cyji\"\n" + "    }\n" + "  }\n" + "}";

        Request searchRequest = new Request("POST", "/my-index-0/_search");
        searchRequest.setJsonEntity(searchJson);
        Response response = client().performRequest(searchRequest);

        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        assertTrue("Expected search results for user.id='cyji'", responseContent.contains("\"hits\":"));
    }

    private void createDocument() throws IOException {
        String documentJson = "{\n"
            + "  \"@timestamp\": \"2099-11-15T13:12:00\",\n"
            + "  \"message\": \"this is document 0\",\n"
            + "  \"user\": { \"id\": \"cyji\", \"age\": 1 }\n"
            + "}";

        Request createDocumentRequest = new Request("POST", "/my-index-0/_doc/");
        createDocumentRequest.setJsonEntity(documentJson);
        Response response = client().performRequest(createDocumentRequest);
        assertEquals(201, response.getStatusLine().getStatusCode());
    }

    private void checkLocalIndices(String context) throws IOException {
        Request indicesRequest = new Request("GET", "/_cat/indices?v");
        Response response = client().performRequest(indicesRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("Expected top_queries-* index to be present", responseContent.contains("top_queries-"));

        // Fetch and check documents in top_queries-* index
        Request fetchRequest = new Request("GET", "/top_queries-*/_search?size=10");
        Response fetchResponse = client().performRequest(fetchRequest);
        assertEquals(200, fetchResponse.getStatusLine().getStatusCode());

        String fetchResponseContent = new String(fetchResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(
            "Expected user.id field with value 'cyji' in query insights data",
            fetchResponseContent.contains("\"user.id\":{\"value\":\"cyji\",\"boost\":1.0}")
        );
    }

    private void checkQueryInsightsIndexTemplate() throws IOException {
        Request request = new Request("GET", "/_index_template");
        Response response = client().performRequest(request);
        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(
            "Expected default index template for Query Insights to be present",
            responseContent.contains("\"query_insights_top_queries_template\"")
        );
    }

    private void disableLocalIndexExporter() throws IOException {
        String disableExporterJson = "{ \"persistent\": { \"search.insights.top_queries.exporter.type\": \"none\" } }";
        Request disableExporterRequest = new Request("PUT", "/_cluster/settings");
        disableExporterRequest.setJsonEntity(disableExporterJson);
        client().performRequest(disableExporterRequest);
    }

    private void reEnableLocalIndexExporter() throws IOException {
        Request enableExporterRequest = new Request("PUT", "/_cluster/settings");
        enableExporterRequest.setJsonEntity(defaultExporterSettings());
        client().performRequest(enableExporterRequest);
    }

    private void setLocalIndexToDebug() throws IOException {
        String debugExporterJson = "{ \"persistent\": { \"search.insights.top_queries.exporter.type\": \"debug\" } }";
        Request debugExporterRequest = new Request("PUT", "/_cluster/settings");
        debugExporterRequest.setJsonEntity(debugExporterJson);
        client().performRequest(debugExporterRequest);
    }

    private void setLatencyWindowSize(String windowSize) throws IOException {
        String windowSizeJson = "{ \"persistent\": { \"search.insights.top_queries.latency.window_size\": \"" + windowSize + "\" } }";
        Request windowSizeRequest = new Request("PUT", "/_cluster/settings");
        windowSizeRequest.setJsonEntity(windowSizeJson);
        client().performRequest(windowSizeRequest);
    }

    private void waitForWindowToPass(int seconds) throws InterruptedException {
        Thread.sleep(seconds * 1000);
    }

    private String defaultExporterSettings() {
        return "{ \"persistent\" : { \"search.insights.top_queries.exporter.type\" : \"local_index\" } }";
    }

    private String[] invalidExporterSettings() {
        return new String[] {
            "{ \"persistent\" : { \"search.insights.top_queries.exporter.type\" : invalid_type } }",
            "{ \"persistent\" : { \"search.insights.top_queries.exporter.type\" : local_index, \"search.insights.top_queries.exporter.config.index\" : \"1a2b\" } }" };
    }
}
