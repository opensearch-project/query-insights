/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.reader;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

public class MultiIndexDateRangeIT extends QueryInsightsRestTestCase {
    private static final DateTimeFormatter indexPattern = DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT);

    public void testMultiIndexDateRangeRetrieval() throws IOException, InterruptedException {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT);

        List<String> inputDates = List.of("2022.06.21", "2020.10.04", "2023.02.15", "2021.12.29", "2024.03.08");

        for (String dateStr : inputDates) {
            LocalDate localDate = LocalDate.parse(dateStr, formatter);
            ZonedDateTime zdt = localDate.atStartOfDay(ZoneOffset.UTC);
            long timestamp = zdt.toInstant().toEpochMilli();
            String indexName = buildLocalIndexName(zdt);
            createTopQueriesIndex(indexName, timestamp);
        }

        Thread.sleep(10000);

        Request request = new Request("GET", "/_insights/top_queries?from=2021-04-01T00:00:00Z&to=2025-04-02T00:00:00Z");

        try {
            Response response = client().performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());

            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    responseBody
                )
            ) {
                Map<String, Object> parsed = parser.map();
                List<Map<String, Object>> topQueries = (List<Map<String, Object>>) parsed.get("top_queries");

                Assert.assertNotNull("Expected 'top_queries' key in response, but was null", topQueries);
                Assert.assertEquals("Expected 4 top queries, but got: " + topQueries.size(), 4, topQueries.size());
            }
            

        } catch (Exception e) {

            throw e;
        }
        cleanup();

    }

    private void createTopQueriesIndex(String indexName, long timestamp) throws IOException, InterruptedException {
        String mapping = "{\n"
            + "  \"mappings\": {\n"
            + "    \"dynamic\": true,\n"
            + "    \"_meta\": {\n"
            + "      \"schema_version\": 1,\n"
            + "      \"query_insights_feature_space\": \"top_n_queries\"\n"
            + "    },\n"
            + "    \"properties\": {\n"
            + "      \"id\": {\n"
            + "        \"type\": \"text\",\n"
            + "        \"fields\": {\n"
            + "          \"keyword\": {\n"
            + "            \"type\": \"keyword\",\n"
            + "            \"ignore_above\": 256\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      \"node_id\": {\n"
            + "        \"type\": \"text\",\n"
            + "        \"fields\": {\n"
            + "          \"keyword\": {\n"
            + "            \"type\": \"keyword\",\n"
            + "            \"ignore_above\": 256\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      \"timestamp\": { \"type\": \"long\" },\n"
            + "      \"total_shards\": { \"type\": \"long\" },\n"
            + "      \"group_by\": {\n"
            + "        \"type\": \"text\",\n"
            + "        \"fields\": {\n"
            + "          \"keyword\": {\n"
            + "            \"type\": \"keyword\",\n"
            + "            \"ignore_above\": 256\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      \"phase_latency_map\": {\n"
            + "        \"properties\": {\n"
            + "          \"expand\": { \"type\": \"long\" },\n"
            + "          \"fetch\": { \"type\": \"long\" },\n"
            + "          \"query\": { \"type\": \"long\" }\n"
            + "        }\n"
            + "      },\n"
            + "      \"search_type\": {\n"
            + "        \"type\": \"text\",\n"
            + "        \"fields\": {\n"
            + "          \"keyword\": {\n"
            + "            \"type\": \"keyword\",\n"
            + "            \"ignore_above\": 256\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      \"task_resource_usages\": {\n"
            + "        \"properties\": {\n"
            + "          \"action\": {\n"
            + "            \"type\": \"text\",\n"
            + "            \"fields\": {\n"
            + "              \"keyword\": {\n"
            + "                \"type\": \"keyword\",\n"
            + "                \"ignore_above\": 256\n"
            + "              }\n"
            + "            }\n"
            + "          },\n"
            + "          \"nodeId\": {\n"
            + "            \"type\": \"text\",\n"
            + "            \"fields\": {\n"
            + "              \"keyword\": {\n"
            + "                \"type\": \"keyword\",\n"
            + "                \"ignore_above\": 256\n"
            + "              }\n"
            + "            }\n"
            + "          },\n"
            + "          \"parentTaskId\": { \"type\": \"long\" },\n"
            + "          \"taskId\": { \"type\": \"long\" },\n"
            + "          \"taskResourceUsage\": {\n"
            + "            \"properties\": {\n"
            + "              \"cpu_time_in_nanos\": { \"type\": \"long\" },\n"
            + "              \"memory_in_bytes\": { \"type\": \"long\" }\n"
            + "            }\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      \"measurements\": {\n"
            + "        \"properties\": {\n"
            + "          \"latency\": {\n"
            + "            \"properties\": {\n"
            + "              \"number\": { \"type\": \"double\" },\n"
            + "              \"count\": { \"type\": \"integer\" },\n"
            + "              \"aggregationType\": { \"type\": \"keyword\" }\n"
            + "            }\n"
            + "          },\n"
            + "          \"cpu\": {\n"
            + "            \"properties\": {\n"
            + "              \"number\": { \"type\": \"double\" },\n"
            + "              \"count\": { \"type\": \"integer\" },\n"
            + "              \"aggregationType\": { \"type\": \"keyword\" }\n"
            + "            }\n"
            + "          },\n"
            + "          \"memory\": {\n"
            + "            \"properties\": {\n"
            + "              \"number\": { \"type\": \"double\" },\n"
            + "              \"count\": { \"type\": \"integer\" },\n"
            + "              \"aggregationType\": { \"type\": \"keyword\" }\n"
            + "            }\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  },\n"
            + "  \"settings\": {\n"
            + "    \"index.number_of_shards\": 1,\n"
            + "    \"index.auto_expand_replicas\": \"0-2\"\n"
            + "  }\n"
            + "}";
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(mapping);

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
                + "  }\n"
                + "}",
            timestamp
        );

    }

    private String buildLocalIndexName(ZonedDateTime current) {
        return "top_queries-" + current.format(indexPattern) + "-" + generateLocalIndexDateHash(current.toLocalDate());
    }

    private String buildbadLocalIndexName(ZonedDateTime current) {
        return "top_queries-" + current.format(indexPattern) + "-" + "10000";
    }

    public static String generateLocalIndexDateHash(LocalDate date) {
        String dateString = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT).format(date);
        return String.format(Locale.ROOT, "%05d", (dateString.hashCode() % 100000 + 100000) % 100000);
    }

    public void testInvalidMultiIndexDateRangeRetrieval() throws IOException, InterruptedException {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT);

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

        try {
            Response response = client().performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());
            Assert.assertTrue("Expected empty top_queries", responseBody.contains("\"top_queries\":[]"));

        } catch (Exception e) {

            throw e;
        }
        cleanup();

    }

}
