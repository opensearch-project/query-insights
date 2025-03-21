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
        createIndexTemplate();
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
        waitForWindowToPass(10);
        performSearch();
        waitForWindowToPass(70);
        checkLocalIndices("After search and waiting for data export");
        checkQueryInsightsIndexTemplate();
        disableLocalIndexExporter();
        reEnableLocalIndexExporter();
        setLocalIndexToDebug();
    }

    private void performSearch() throws IOException {
        String searchJson = "{\n"
            + "  \"query\": {\n"
            + "    \"match\": {\n"
            + "      \"title\": \"Test Document\"\n"
            + "    }\n"
            + "  }\n"
            + "}";

        Request searchRequest = new Request("POST", "/my-index-0/_search?size=20&pretty");
        searchRequest.setJsonEntity(searchJson);
        Response response = client().performRequest(searchRequest);

        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        assertTrue("Expected search results for title='Test Document'", responseContent.contains("\"Test Document\""));
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

    private void checkLocalIndices(String context) throws IOException {
        Request indicesRequest = new Request("GET", "/_cat/indices?v");
        Response response = client().performRequest(indicesRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("Expected top_queries-* index to be green", responseContent.contains("green"));
        assertTrue("Expected top_queries-* index to be present", responseContent.contains("top_queries-"));

        String suffix = null;

        // Loop through each line to find the top_queries-* index
        for (String line : responseContent.split("\n")) {
            line = line.trim();
            if (line.contains("top_queries-")) {
                String[] columns = line.split("\\s+");
                for (String col : columns) {
                    if (col.matches("top_queries-\\d{4}\\.\\d{2}\\.\\d{2}-\\d+")) {
                        String[] parts = col.split("-");
                        suffix = parts[parts.length - 1];
                        break;
                    }
                }
            }
            if (suffix != null) break;
        }

        assertNotNull("Failed to extract suffix from top_queries-* index", suffix);
        String fullIndexName = "top_queries-" + suffix;

        // Fetch and check documents in the top_queries-* index
        Request fetchRequest = new Request("GET", "/" + fullIndexName + "/_search?size=10");
        Response fetchResponse = client().performRequest(fetchRequest);
        assertEquals(200, fetchResponse.getStatusLine().getStatusCode());

        String fetchResponseContent = new String(fetchResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(
            "Expected title field with value 'Test Document' in query insights data in local Indices",
            fetchResponseContent.contains("\"Test Document\"")
        );
    }

    private void checkQueryInsightsIndexTemplate() throws IOException {
        Request request = new Request("GET", "/_index_template");
        Response response = client().performRequest(request);
        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(
            "Expected default index template for Query Insights to be present",
            responseContent.contains(
                "{\"index_templates\":[{\"name\":\"my_template\",\"index_template\":{\"index_patterns\":[\"*\"],\"template\":{\"settings\":{\"index\":{\"number_of_shards\":\"1\",\"number_of_replicas\":\"1\",\"blocks\":{\"write\":\"true\"}}},\"mappings\":{\"properties\":{\"group_by\":{\"type\":\"keyword\"}}},\"aliases\":{\"my_alias\":{}}},\"composed_of\":[],\"priority\":2000}}]}"
            )
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

    private void createIndexTemplate() throws IOException {
        String templateJson = "{\n"
            + " \"index_patterns\": [\"*\"],\n"
            + " \"template\": {\n"
            + "  \"settings\": {\n"
            + "   \"number_of_shards\": 1,\n"
            + "   \"number_of_replicas\": 1,\n"
            + "   \"index.blocks.write\": true\n"
            + "  },\n"
            + "  \"mappings\": {\n"
            + "   \"properties\": {\n"
            + "    \"group_by\": {\n"
            + "     \"type\": \"keyword\"\n"
            + "    }\n"
            + "   }\n"
            + "  },\n"
            + "  \"aliases\": {\n"
            + "   \"my_alias\": {}\n"
            + "  }\n"
            + " },\n"
            + " \"priority\" : 2000\n"
            + "}";

        Request templateRequest = new Request("PUT", "/_index_template/my_template?pretty");
        templateRequest.setJsonEntity(templateJson);
        client().performRequest(templateRequest);
    }
}
