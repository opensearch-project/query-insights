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

public class MultiIndexDateRangeIT extends QueryInsightsRestTestCase {
    private static final DateTimeFormatter indexPattern = DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT);

    public void testMultiIndexDateRangeRetrieval() throws IOException, ParseException, InterruptedException {

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
                List<Map<String, Object>> topQueries = (List<Map<String, Object>>) parsed.get("top_queries");

                // Assert the expected count
                Assert.assertEquals("Expected 4 top queries", 4, topQueries.size());
            }

        } catch (Exception e) {

            throw e;
        }
        cleanup();

    }

    private void createTopQueriesIndex(String indexName, long timestamp) throws IOException, ParseException, InterruptedException {
        String mapping = """
            {
              "mappings": {
                "dynamic": true,
                "_meta": {
                  "schema_version": 1,
                  "query_insights_feature_space": "top_n_queries"
                },
                "properties": {
                  "id": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "node_id": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "timestamp": { "type": "long" },
                  "total_shards": { "type": "long" },
                  "group_by": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "phase_latency_map": {
                    "properties": {
                      "expand": { "type": "long" },
                      "fetch": { "type": "long" },
                      "query": { "type": "long" }
                    }
                  },
                  "search_type": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "task_resource_usages": {
                    "properties": {
                      "action": {
                        "type": "text",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          }
                        }
                      },
                      "nodeId": {
                        "type": "text",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          }
                        }
                      },
                      "parentTaskId": { "type": "long" },
                      "taskId": { "type": "long" },
                      "taskResourceUsage": {
                        "properties": {
                          "cpu_time_in_nanos": { "type": "long" },
                          "memory_in_bytes": { "type": "long" }
                        }
                      }
                    }
                  },
                  "measurements": {
                    "properties": {
                      "latency": {
                        "properties": {
                          "number": { "type": "double" },
                          "count": { "type": "integer" },
                          "aggregationType": { "type": "keyword" }
                        }
                      },
                      "cpu": {
                        "properties": {
                          "number": { "type": "double" },
                          "count": { "type": "integer" },
                          "aggregationType": { "type": "keyword" }
                        }
                      },
                      "memory": {
                        "properties": {
                          "number": { "type": "double" },
                          "count": { "type": "integer" },
                          "aggregationType": { "type": "keyword" }
                        }
                      }
                    }
                  }
                }
              },
              "settings": {
                "index.number_of_shards": 1,
                "index.auto_expand_replicas": "0-2"
              }
            }
            """;
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
        return String.format(Locale.ROOT, """
            {
              "timestamp": %d,
              "id": "6ac36175-e48e-4b90-9dbb-ee711a7ec629",
              "node_id": "TL1FYh4DR36PmFp9JRCtaA",
              "total_shards": 1,
              "group_by": "NONE",
              "search_type": "query_then_fetch",
              "phase_latency_map": {
                "expand": 0,
                "query": 37,
                "fetch": 1
              },
              "task_resource_usages": [
                {
                  "action": "indices:data/read/search[phase/query]",
                  "taskId": 41,
                  "parentTaskId": 40,
                  "nodeId": "TL1FYh4DR36PmFp9JRCtaA",
                  "taskResourceUsage": {
                    "cpu_time_in_nanos": 29965000,
                    "memory_in_bytes": 3723960
                  }
                },
                {
                  "action": "indices:data/read/search",
                  "taskId": 40,
                  "parentTaskId": -1,
                  "nodeId": "TL1FYh4DR36PmFp9JRCtaA",
                  "taskResourceUsage": {
                    "cpu_time_in_nanos": 1104000,
                    "memory_in_bytes": 106176
                  }
                }
              ],
              "measurements": {
                "latency": {
                  "number": 48,
                  "count": 1,
                  "aggregationType": "NONE"
                },
                "memory": {
                  "number": 3830136,
                  "count": 1,
                  "aggregationType": "NONE"
                },
                "cpu": {
                  "number": 31069000,
                  "count": 1,
                  "aggregationType": "NONE"
                }
              }
            }
            """, timestamp);
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

    public void testInvalidMultiIndexDateRangeRetrieval() throws IOException, ParseException, InterruptedException {

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
