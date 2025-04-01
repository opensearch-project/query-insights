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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
        createIndexTemplate();
        waitForWindowToPass(10);
        performSearch();

        waitForWindowToPass(70);
        String fullIndexName = null;
        fullIndexName = checkLocalIndices();
        checkQueryInsightsIndexTemplate();
        disableLocalIndexExporter();
        reEnableLocalIndexExporter();
        setLocalIndexToDebug();
        Thread.sleep(20000);

        // Delete top_queries index with verification
        if (fullIndexName != null) {
            deleteIndexWithRetry(fullIndexName);
        }

        // Delete test index with verification
        deleteIndexWithRetry("my-index-0");

        // Delete index template with verification
        deleteTemplateWithRetry();

        // Reset settings with verification
        resetSettingsWithRetry();

        // Verify all cleanup operations
        verifyCleanup();

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

    private String checkLocalIndices() throws IOException {
        Request indicesRequest = new Request("GET", "/_cat/indices?v");
        Response response = client().performRequest(indicesRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("Expected top_queries-* index to be green", responseContent.contains("green"));
        assertTrue("Expected top_queries-* index to be present", responseContent.contains("top_queries-"));
        String suffix = null;
        Pattern pattern = Pattern.compile("top_queries-(\\d{4}\\.\\d{2}\\.\\d{2}-\\d+)");
        Matcher matcher = pattern.matcher(responseContent);
        if (matcher.find()) {
            suffix = matcher.group(1);
        } else {
            fail("Failed to extract top_queries index suffix");
        }

        assertNotNull("Failed to extract suffix from top_queries-* index", suffix);
        String fullIndexName = "top_queries-" + suffix;
        Request fetchRequest = new Request("GET", "/" + fullIndexName + "/_search?size=10");
        Response fetchResponse = client().performRequest(fetchRequest);
        assertEquals(200, fetchResponse.getStatusLine().getStatusCode());

        String fetchResponseContent = new String(fetchResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(
            "Expected title field with value 'Test Document' in query insights data in local Indices",
            fetchResponseContent.contains("\"Test Document\"")
        );
        return fullIndexName;
    }

    private void checkQueryInsightsIndexTemplate() throws IOException {
        Request request = new Request("GET", "/_index_template");
        Response response = client().performRequest(request);
        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("Expected default index template for my_template to be present", responseContent.contains("my_template"));
        assertTrue("Expected priority for my_template to be 2000", responseContent.contains("2000"));
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
        return "{ \"persistent\" : { \"search.insights.top_queries.exporter.type\" : \"local_index\" ,"
            + "\"search.insights.top_queries.latency.enabled\": \"true\"} }";
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
            + "   \"index.blocks.write\": false\n"
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

    private void cleanup(String fullIndexName) throws IOException, InterruptedException {
        Thread.sleep(10000);

        try {
            client().performRequest(new Request("DELETE", "/" + fullIndexName));
        } catch (ResponseException ignored) {}

        try {
            client().performRequest(new Request("DELETE", "/my-index-0"));
        } catch (ResponseException ignored) {}

        try {
            client().performRequest(new Request("DELETE", "/index_template"));
        } catch (ResponseException ignored) {}

        String resetSettings = "{ \"persistent\": { "
            + "\"search.insights.top_queries.exporter.type\": \"none\", "
            + "\"search.insights.top_queries.latency.enabled\": \"false\""
            + "} }";

        Request resetRequest = new Request("PUT", "/_cluster/settings");
        resetRequest.setJsonEntity(resetSettings);
        client().performRequest(resetRequest);

    }

    private void deleteIndexWithRetry(String indexName) throws IOException {
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                Request deleteRequest = new Request("DELETE", "/" + indexName);
                Response response = client().performRequest(deleteRequest);
                if (response.getStatusLine().getStatusCode() == 200) {
                    logger.info("Successfully deleted index: {}", indexName);

                    // Verify index is actually deleted
                    if (!WhetherindexExists(indexName)) {
                        return;
                    }
                }
            } catch (ResponseException e) {
                if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                    // Index doesn't exist, which is fine
                    logger.info("Index {} already doesn't exist", indexName);
                    return;
                }
                logger.warn("Failed to delete index: {} (attempt {}/{})", indexName, i + 1, maxRetries, e);
            }

            if (i < maxRetries - 1) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting between retries", ie);
                }
            }
        }
        throw new IOException("Failed to delete index: " + indexName + " after " + maxRetries + " attempts");
    }

    private void deleteTemplateWithRetry() throws IOException {
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                Request deleteRequest = new Request("DELETE", "/index_template");
                Response response = client().performRequest(deleteRequest);
                if (response.getStatusLine().getStatusCode() == 200) {

                    // Verify template is actually deleted
                    if (!templateExists()) {
                        return;
                    }
                }
            } catch (ResponseException e) {
                if (e.getResponse().getStatusLine().getStatusCode() == 404) {

                    return;
                }

            }

            if (i < maxRetries - 1) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting between retries", ie);
                }
            }
        }
        throw new IOException("Failed to delete template after " + maxRetries + " attempts");
    }

    private void resetSettingsWithRetry() throws IOException {
        String resetSettings = "{ \"persistent\": { "
            + "\"search.insights.top_queries.exporter.type\": \"none\", "
            + "\"search.insights.top_queries.latency.enabled\": \"false\""
            + "} }";

        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                Request resetRequest = new Request("PUT", "/_cluster/settings");
                resetRequest.setJsonEntity(resetSettings);
                Response response = client().performRequest(resetRequest);
                if (response.getStatusLine().getStatusCode() == 200) {
                    return;
                }
            } catch (ResponseException e) {}

            if (i < maxRetries - 1) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting between retries", ie);
                }
            }
        }
        throw new IOException("Failed to reset settings after " + maxRetries + " attempts");
    }

    private boolean WhetherindexExists(String indexName) throws IOException {
        try {
            Request request = new Request("HEAD", "/" + indexName);
            Response response = client().performRequest(request);
            return response.getStatusLine().getStatusCode() == 200;
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    private boolean templateExists() throws IOException {
        try {
            Request request = new Request("GET", "/_index_template");
            Response response = client().performRequest(request);
            String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
            return !responseBody.contains("my_template");
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    private void verifyCleanup() throws IOException {

        // Verify indices are deleted
        Request indicesRequest = new Request("GET", "/_cat/indices?v");
        Response response = client().performRequest(indicesRequest);
        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        if (responseBody.contains("my-index-0") || responseBody.contains("top_queries-")) {
            throw new IOException("Cleanup verification failed: Some indices still exist\n" + responseBody);
        }

        // Verify templates are deleted
        if (templateExists()) {
            throw new IOException("Cleanup verification failed: Template still exists");
        }

    }

}
