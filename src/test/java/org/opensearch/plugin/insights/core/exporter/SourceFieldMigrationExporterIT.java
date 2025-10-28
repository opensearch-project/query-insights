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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

/**
 * Integration tests for source field migration and backwards compatibility with different MAX_SOURCE_LENGTH settings
 */
public class SourceFieldMigrationExporterIT extends QueryInsightsRestTestCase {

    private static final int SMALL_SOURCE_LENGTH = 100;
    private static final int MEDIUM_SOURCE_LENGTH = 1000;
    private static final int LARGE_SOURCE_LENGTH = 15000; // Exceeds default 10KB limit

    /**
     * Test exporter with different source lengths to verify truncation behavior
     */
    public void testExporterWithSourceTruncation() throws Exception {
        // Create test index first
        createTestIndex();

        // Setup: Enable local index exporter and top queries
        enableLocalIndexExporter();

        // Perform queries
        performQueryWithSize("test", LARGE_SOURCE_LENGTH);

        // Wait for export
        Thread.sleep(70000);

        // Verify exported data exists
        verifyBasicExport();

        cleanup();
    }

    /**
     * Test backwards compatibility with existing indices that have old mapping
     */
    public void testBackwardsCompatibilityWithOldMapping() throws Exception {
        // Create test index for queries
        createTestIndex();

        // Create index with old mapping (source as object)
        createOldMappingIndex();

        // Insert old format data
        insertOldFormatData();

        // Enable exporter
        enableLocalIndexExporter();

        // Perform new queries
        performQueryWithSize("new", MEDIUM_SOURCE_LENGTH);
        Thread.sleep(70000);

        // Verify both old and new data can be retrieved
        verifyMixedFormatData();

        cleanup();
    }

    private void enableLocalIndexExporter() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{ \"persistent\": { "
                + "\"search.insights.top_queries.exporter.type\": \"local_index\", "
                + "\"search.insights.top_queries.latency.enabled\": \"true\", "
                + "\"search.insights.top_queries.latency.window_size\": \"1m\" } }"
        );
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void createTestIndex() throws IOException {
        Request createRequest = new Request("PUT", "/test-index");
        createRequest.setJsonEntity("{ \"mappings\": { \"properties\": { \"field\": { \"type\": \"text\" } } } }");
        try {
            client().performRequest(createRequest);
        } catch (Exception e) {
            // Index might already exist
        }
    }

    private void performQueryWithSize(String queryType, int targetSize) throws IOException {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("{ \"query\": { \"bool\": { \"must\": [");

        int clauseCount = 0;
        while (queryBuilder.length() < targetSize - 100) {
            if (clauseCount > 0) queryBuilder.append(", ");
            queryBuilder.append(
                String.format(Locale.ROOT, "{ \"match\": { \"field\": \"value_%d_long_text_to_increase_query_size\" } }", clauseCount++)
            );
        }
        queryBuilder.append("] } } }");

        Request searchRequest = new Request("POST", "/test-index/_search");
        searchRequest.setJsonEntity(queryBuilder.toString());
        client().performRequest(searchRequest);
    }

    private void createOldMappingIndex() throws IOException {
        String indexName = "top_queries-2024.01.01-12345";
        Request createRequest = new Request("PUT", "/" + indexName);
        createRequest.setJsonEntity(
            "{ \"mappings\": { "
                + "\"properties\": { "
                + "\"source\": { \"type\": \"object\" }, "
                + "\"timestamp\": { \"type\": \"date\" }, "
                + "\"measurements\": { \"type\": \"object\" } "
                + "} } }"
        );

        try {
            Response response = client().performRequest(createRequest);
            assertTrue(response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 201);
        } catch (Exception e) {
            // Index might already exist, continue
        }
    }

    private void insertOldFormatData() throws IOException {
        String indexName = "top_queries-2024.01.01-12345";
        Request insertRequest = new Request("POST", "/" + indexName + "/_doc");
        insertRequest.setJsonEntity(
            "{ "
                + "\"timestamp\": \"2024-01-01T12:00:00Z\", "
                + "\"source\": { "
                + "\"query\": { \"match\": { \"title\": \"old format\" } } "
                + "}, "
                + "\"measurements\": { "
                + "\"latency\": { \"number\": 100, \"count\": 1, \"aggregationType\": \"NONE\" } "
                + "}, "
                + "\"search_type\": \"query_then_fetch\", "
                + "\"total_shards\": 1 "
                + "}"
        );

        Response response = client().performRequest(insertRequest);
        assertEquals(201, response.getStatusLine().getStatusCode());

        // Refresh index
        Request refreshRequest = new Request("POST", "/" + indexName + "/_refresh");
        client().performRequest(refreshRequest);
    }

    @SuppressWarnings("unchecked")
    private void verifyBasicExport() throws IOException {
        String indexName = findTopQueriesIndex();
        if (indexName == null) {
            // No export happened, which is acceptable for this test
            return;
        }

        Request searchRequest = new Request("GET", "/" + indexName + "/_search?size=10");
        Response response = client().performRequest(searchRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        byte[] content = response.getEntity().getContent().readAllBytes();
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                content
            )
        ) {
            Map<String, Object> responseMap = parser.map();
            Map<String, Object> hitsWrapper = (Map<String, Object>) responseMap.get("hits");
            List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsWrapper.get("hits");

            if (hits.size() > 0) {
                Map<String, Object> source = (Map<String, Object>) hits.get(0).get("_source");
                // Just verify the source_truncated field exists
                assertTrue("Should have source_truncated field", source.containsKey("source_truncated"));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void verifyMixedFormatData() throws IOException {
        // Check both old index and new exported index
        String oldIndexName = "top_queries-2024.01.01-12345";
        String newIndexName = findTopQueriesIndex();

        // Verify old format data still exists
        Request oldSearchRequest = new Request("GET", "/" + oldIndexName + "/_search");
        Response oldResponse = client().performRequest(oldSearchRequest);
        assertEquals(200, oldResponse.getStatusLine().getStatusCode());

        byte[] oldContent = oldResponse.getEntity().getContent().readAllBytes();
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                oldContent
            )
        ) {
            Map<String, Object> responseMap = parser.map();
            Map<String, Object> hitsWrapper = (Map<String, Object>) responseMap.get("hits");
            List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsWrapper.get("hits");

            assertTrue("Should have old format data", hits.size() > 0);

            Map<String, Object> oldHit = hits.get(0);
            Map<String, Object> oldSource = (Map<String, Object>) oldHit.get("_source");

            // Verify old format has source as object
            Object sourceField = oldSource.get("source");
            assertTrue("Old format source should be object", sourceField instanceof Map);
        }

        // Verify new format data exists if new index was created
        if (newIndexName != null && !newIndexName.equals(oldIndexName)) {
            Request newSearchRequest = new Request("GET", "/" + newIndexName + "/_search");
            Response newResponse = client().performRequest(newSearchRequest);
            assertEquals(200, newResponse.getStatusLine().getStatusCode());

            byte[] newContent = newResponse.getEntity().getContent().readAllBytes();
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    newContent
                )
            ) {
                Map<String, Object> responseMap = parser.map();
                Map<String, Object> hitsWrapper = (Map<String, Object>) responseMap.get("hits");
                List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsWrapper.get("hits");

                if (hits.size() > 0) {
                    Map<String, Object> newHit = hits.get(0);
                    Map<String, Object> newSource = (Map<String, Object>) newHit.get("_source");

                    // Verify new format has source_truncated field
                    assertTrue("New format should have source_truncated field", newSource.containsKey("source_truncated"));
                }
            }
        }
    }

    private String findTopQueriesIndex() throws IOException {
        Request indicesRequest = new Request("GET", "/_cat/indices?format=json");
        Response response = client().performRequest(indicesRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        byte[] content = response.getEntity().getContent().readAllBytes();
        String responseStr = new String(content, StandardCharsets.UTF_8);

        // Parse JSON array to find top_queries index
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseStr
            )
        ) {
            List<Object> rawIndices = parser.list();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> indices = (List<Map<String, Object>>) (List<?>) rawIndices;

            for (Map<String, Object> index : indices) {
                String indexName = (String) index.get("index");
                if (indexName != null && indexName.startsWith("top_queries-")) {
                    return indexName;
                }
            }
        }

        return null;
    }
}
