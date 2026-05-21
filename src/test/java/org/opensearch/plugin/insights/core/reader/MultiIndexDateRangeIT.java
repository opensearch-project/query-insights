/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.reader;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.INDEX_DATE_FORMAT_PATTERN;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;
import org.opensearch.plugin.insights.core.utils.IndexDiscoveryHelper;

public class MultiIndexDateRangeIT extends QueryInsightsRestTestCase {
    private static final DateTimeFormatter indexPattern = DateTimeFormatter.ofPattern(INDEX_DATE_FORMAT_PATTERN, Locale.ROOT);

    void createLocalIndices() throws IOException, ParseException, InterruptedException {
        // Explicitly enable local_index exporter to ensure the reader is initialized,
        // regardless of the default exporter type (which may be NONE in some builds).
        defaultExporterSettings();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(INDEX_DATE_FORMAT_PATTERN, Locale.ROOT);

        List<String> inputDates = List.of("2022.06.21", "2020.10.04", "2023.02.15", "2021.12.29", "2024.03.08");

        for (String dateStr : inputDates) {
            LocalDate localDate = LocalDate.parse(dateStr, formatter);
            ZonedDateTime zdt = localDate.atStartOfDay(ZoneOffset.UTC);
            long timestamp = zdt.toInstant().toEpochMilli();
            String indexName = buildLocalIndexName(zdt);
            createTopQueriesIndex(indexName, timestamp);
        }

        Thread.sleep(10000);
    }

    public void testMultiIndexDateRangeRetrieval() throws IOException, ParseException, InterruptedException {
        createLocalIndices();

        Request request = new Request("GET", "/_insights/top_queries?from=2021-04-01T00:00:00Z&to=2025-04-02T00:00:00Z");

        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertFalse("Expected non-empty top_queries but got empty list", responseBody.contains("\"top_queries\":[]"));
        byte[] bytes = response.getEntity().getContent().readAllBytes();

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                bytes
            )
        ) {
            Map<String, Object> parsed = parser.map();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> topQueries = (List<Map<String, Object>>) parsed.get("top_queries");

            // Assert the expected count
            Assert.assertEquals("Expected 4 top queries", 4, topQueries.size());
        }
        cleanup();
    }

    public void testReaderTopNLabels() throws IOException, ParseException, InterruptedException {
        createLocalIndices();

        // type=cpu, 4 top queries
        Request request = new Request("GET", "/_insights/top_queries?from=2021-04-01T00:00:00Z&to=2025-04-02T00:00:00Z&type=cpu");

        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertFalse("Expected non-empty top_queries but got empty list", responseBody.contains("\"top_queries\":[]"));
        byte[] bytes = response.getEntity().getContent().readAllBytes();

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                bytes
            )
        ) {
            Map<String, Object> parsed = parser.map();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> topQueries = (List<Map<String, Object>>) parsed.get("top_queries");

            // Assert the expected count
            Assert.assertEquals("Expected 4 top queries", 4, topQueries.size());
        }

        // type=memory, 0 top queries
        request = new Request("GET", "/_insights/top_queries?from=2021-04-01T00:00:00Z&to=2025-04-02T00:00:00Z&type=memory");

        response = client().performRequest(request);
        responseBody = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertTrue("Expected empty top_queries", responseBody.contains("\"top_queries\":[]"));

        cleanup();
    }

    public void testInvalidMultiIndexDateRangeRetrieval() throws IOException, ParseException, InterruptedException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(INDEX_DATE_FORMAT_PATTERN, Locale.ROOT);

        List<String> inputDates = List.of("2022.06.21", "2020.10.04", "2023.02.15", "2021.12.29", "2024.03.08");

        for (String dateStr : inputDates) {
            LocalDate localDate = LocalDate.parse(dateStr, formatter);
            ZonedDateTime zdt = localDate.atStartOfDay(ZoneOffset.UTC);
            long timestamp = zdt.toInstant().toEpochMilli();
            String indexName = buildbadLocalIndexName(zdt);
            createTopQueriesIndex(indexName, timestamp);
        }

        Thread.sleep(10000);

        Request request = new Request("GET", "/_insights/top_queries?from=2021-04-01T00:00:00Z&to=2025-04-02T00:00:00Z");

        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertTrue("Expected empty top_queries", responseBody.contains("\"top_queries\":[]"));
        cleanup();
    }

    private void createTopQueriesIndex(String indexName, long timestamp) throws IOException, ParseException, InterruptedException {
        String mappingJson = new String(
            getClass().getClassLoader().getResourceAsStream("mappings/top-queries-record.json").readAllBytes(),
            StandardCharsets.UTF_8
        );
        String jsonBody = ""
            + "{\n"
            + "  \"settings\": {\n"
            + "    \"index.number_of_shards\": 1,\n"
            + "    \"index.auto_expand_replicas\": \"0-2\"\n"
            + "  },\n"
            + "  \"mappings\": "
            + mappingJson
            + "\n"
            + "}";

        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(jsonBody);

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Request docrequest = new Request("POST", "/" + indexName + "/_doc");
        String docBody = createDocumentsBody(timestamp);

        docrequest.setJsonEntity(docBody);

        Response docresponse = client().performRequest(docrequest);

        Assert.assertEquals(201, docresponse.getStatusLine().getStatusCode());

        Thread.sleep(3000);

        Request searchTest = new Request("GET", "/" + indexName + "/_search");
        searchTest.setJsonEntity("{ \"query\": { \"match_all\": {} } }");
        Response searchResp = client().performRequest(searchTest);
        Assert.assertEquals(200, searchResp.getStatusLine().getStatusCode());

    }

    protected String createDocumentsBody(long timestamp) {
        return String.format(
            Locale.ROOT,
            "{\n"
                + "  \"timestamp\": %d,\n"
                + "  \"id\": \"6ac36175-e48e-4b90-9dbb-ee711a7ec629\",\n"
                + "  \"node_id\": \"TL1FYh4DR36PmFp9JRCtaA\",\n"
                + "  \"total_shards\": 1,\n"
                + "  \"group_by\": \"NONE\",\n"
                + "  \"search_type\": \"query_then_fetch\",\n"
                + "  \"phase_latency_map\": {\n"
                + "    \"expand\": 0,\n"
                + "    \"query\": 37,\n"
                + "    \"fetch\": 1\n"
                + "  },\n"
                + "  \"task_resource_usages\": [\n"
                + "    {\n"
                + "      \"action\": \"indices:data/read/search[phase/query]\",\n"
                + "      \"taskId\": 41,\n"
                + "      \"parentTaskId\": 40,\n"
                + "      \"nodeId\": \"TL1FYh4DR36PmFp9JRCtaA\",\n"
                + "      \"taskResourceUsage\": {\n"
                + "        \"cpu_time_in_nanos\": 29965000,\n"
                + "        \"memory_in_bytes\": 3723960\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"action\": \"indices:data/read/search\",\n"
                + "      \"taskId\": 40,\n"
                + "      \"parentTaskId\": -1,\n"
                + "      \"nodeId\": \"TL1FYh4DR36PmFp9JRCtaA\",\n"
                + "      \"taskResourceUsage\": {\n"
                + "        \"cpu_time_in_nanos\": 1104000,\n"
                + "        \"memory_in_bytes\": 106176\n"
                + "      }\n"
                + "    }\n"
                + "  ],\n"
                + "  \"measurements\": {\n"
                + "    \"latency\": {\n"
                + "      \"number\": 48,\n"
                + "      \"count\": 1,\n"
                + "      \"aggregationType\": \"NONE\"\n"
                + "    },\n"
                + "    \"memory\": {\n"
                + "      \"number\": 3830136,\n"
                + "      \"count\": 1,\n"
                + "      \"aggregationType\": \"NONE\"\n"
                + "    },\n"
                + "    \"cpu\": {\n"
                + "      \"number\": 31069000,\n"
                + "      \"count\": 1,\n"
                + "      \"aggregationType\": \"NONE\"\n"
                + "    }\n"
                + "  },\n"
                + "  \"top_n_query\": {\n"
                + "    \"latency\": true,\n"
                + "    \"cpu\": true,\n"
                + "    \"memory\": false\n"
                + "  }\n"
                + "}",
            timestamp
        );

    }

    private String buildLocalIndexName(ZonedDateTime current) {
        return "top_queries-" + IndexDiscoveryHelper.buildLocalIndexName(indexPattern, current);
    }

    private String buildbadLocalIndexName(ZonedDateTime current) {
        return "top_queries-" + current.format(indexPattern) + "-" + "10000";
    }
}
