/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.service.grouper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/**
 * ITs for Grouping Top Queries by similarity
 */
public class MinMaxQueryGrouperBySimilarityIT extends QueryInsightsRestTestCase {

    /**
     * test grouping top queries
     *
     * @throws IOException IOException
     */
    public void testGroupingBySimilarity() throws IOException, InterruptedException {

        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        waitForEmptyTopQueriesResponse();

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(3, "latency");
    }

    /**
     * Test grouping with field_name enabled - same query types group together
     *
     * @throws IOException IOException
     */
    public void testGroupingWithFieldName() throws IOException, InterruptedException {
        waitForEmptyTopQueriesResponse();
        updateClusterSettings(this::defaultTopQueryGroupingSettings);
        waitForEmptyTopQueriesResponse();

        // With field_name enabled, match queries on different fields should NOT group together
        doSearch("match_text_field", 2);
        doSearch("match_keyword_field", 2);
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis());
        validateGroups(2, 2); // Two groups with 2 queries each
    }

    /**
     * Test grouping with field_name disabled
     *
     * @throws IOException IOException
     */
    public void testGroupingWithFieldNameDisabled() throws IOException, InterruptedException {
        waitForEmptyTopQueriesResponse();
        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::disableFieldNameSettings);
        waitForEmptyTopQueriesResponse();

        // With field_name disabled, match queries on different fields should group together
        doSearch("match_text_field", 2);
        doSearch("match_keyword_field", 2);
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis());
        validateGroups(4); // One group with 4 queries
    }

    /**
     * Test grouping with field_type enabled - different query types stay separate
     *
     * @throws IOException IOException
     */
    public void testGroupingWithFieldType() throws IOException, InterruptedException {
        waitForEmptyTopQueriesResponse();
        updateClusterSettings(this::defaultTopQueryGroupingSettings);
        waitForEmptyTopQueriesResponse();

        // With field_type enabled, match queries on fields with different data types should NOT group together
        doSearch("match_text_field", 2);
        doSearch("match_keyword_field", 2);
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis());
        validateGroups(2, 2); // Two groups with 2 queries each
    }

    /**
     * Test grouping with field_type disabled
     *
     * @throws IOException IOException
     */
    public void testGroupingWithFieldTypeDisabled() throws IOException, InterruptedException {
        waitForEmptyTopQueriesResponse();
        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        waitForEmptyTopQueriesResponse();

        // First disable field_name so field names don't interfere
        updateClusterSettings(this::disableFieldNameSettings);
        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::disableFieldTypeSettings);
        waitForEmptyTopQueriesResponse();

        // With both field_name and field_type disabled, match queries should group together
        doSearch("match_text_field", 2);
        doSearch("match_keyword_field", 2);
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis());
        validateGroups(4); // One group with 4 queries
    }

    /**
     * Validates both group sizes and attributes using same logic as assertTopQueriesCount
     */
    private void validateGroups(int... expectedGroupSizes) throws IOException, InterruptedException {
        // Ensure records are drained to the top queries service
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis());

        List<Map<String, Object>> topQueries = null;
        // run ten times to make sure the records are drained to the top queries services
        for (int i = 0; i < 10; i++) {
            // Parse the response to validate group structure
            Request request = new Request("GET", "/_insights/top_queries?type=latency");
            Response response = client().performRequest(request);
            Map<String, Object> responseMap = entityAsMap(response);
            topQueries = (List<Map<String, Object>>) responseMap.get("top_queries");

            if (topQueries != null && topQueries.size() >= expectedGroupSizes.length) {
                // Validate group count
                assertEquals("Number of groups should match expected", expectedGroupSizes.length, topQueries.size());

                // Validate each group
                for (int j = 0; j < topQueries.size(); j++) {
                    Map<String, Object> query = topQueries.get(j);

                    // Validate group attributes
                    assertTrue("Query should have group_by", query.containsKey("group_by"));
                    assertEquals("group_by should be SIMILARITY", GroupingType.SIMILARITY.toString(), query.get("group_by"));
                    assertTrue("Query should have query_group_hashcode", query.containsKey("query_group_hashcode"));
                    assertNotNull("query_group_hashcode should not be null", query.get("query_group_hashcode"));

                    Map<String, Object> measurements = (Map<String, Object>) query.get("measurements");
                    Map<String, Object> latency = (Map<String, Object>) measurements.get("latency");

                    // Validate group size
                    assertTrue("Latency should have count", latency.containsKey("count"));
                    Integer count = (Integer) latency.get("count");
                    assertEquals("Group " + j + " size should match expected", expectedGroupSizes[j], count.intValue());

                    // Validate other attributes
                    assertTrue("Query should have source", query.containsKey("source"));
                    Map<String, Object> source = (Map<String, Object>) query.get("source");
                    assertNotNull("Source should not be null", source);
                    assertTrue("Source should contain query", source.containsKey("query"));
                    assertEquals(
                        "aggregationType should be AVERAGE for grouped queries",
                        AggregationType.AVERAGE.toString(),
                        latency.get("aggregationType")
                    );
                }
                return;
            }

            if (i < 9) {
                Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis());
            }
        }
        fail(
            "Failed to validate groups after 10 attempts. Expected groups: "
                + Arrays.toString(expectedGroupSizes)
                + ", but got: "
                + (topQueries != null ? topQueries.size() : "null")
        );
    }

    /**
     * Test invalid query grouping settings
     *
     * @throws IOException IOException
     */
    public void testInvalidQueryGroupingSettings() throws IOException {
        for (String setting : invalidQueryGroupingSettings()) {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(setting);
            try {
                client().performRequest(request);
                fail("Should not succeed with invalid query grouping settings");
            } catch (ResponseException e) {
                assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
            }
        }
    }

    /**
     * Test valid query grouping settings
     *
     * @throws IOException IOException
     */
    public void testValidQueryGroupingSettings() throws IOException {
        for (String setting : validQueryGroupingSettings()) {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(setting);
            Response response = client().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
        }
    }

    private String[] invalidQueryGroupingSettings() {
        return new String[] {
            // Invalid max_groups: below minimum (-1)
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.grouping.max_groups_excluding_topn\" : -1\n"
                + "    }\n"
                + "}",

            // Invalid max_groups: above maximum (10001)
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.grouping.max_groups_excluding_topn\" : 10001\n"
                + "    }\n"
                + "}",

            // Invalid group_by: unsupported value
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.grouping.group_by\" : \"unsupported_value\"\n"
                + "    }\n"
                + "}" };
    }

    private String[] validQueryGroupingSettings() {
        return new String[] {
            // Valid max_groups: minimum value (0)
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.grouping.max_groups_excluding_topn\" : 0\n"
                + "    }\n"
                + "}",

            // Valid max_groups: maximum value (10000)
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.grouping.max_groups_excluding_topn\" : 10000\n"
                + "    }\n"
                + "}",

            // Valid group_by: supported value (SIMILARITY)
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.grouping.group_by\" : \"SIMILARITY\"\n"
                + "    }\n"
                + "}" };
    }

    private String groupByNoneSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 100,\n"
            + "        \"search.insights.top_queries.grouping.group_by\" : \"none\",\n"
            + "        \"search.insights.top_queries.grouping.max_groups_excluding_topn\" : 5\n"
            + "    }\n"
            + "}";
    }
}
