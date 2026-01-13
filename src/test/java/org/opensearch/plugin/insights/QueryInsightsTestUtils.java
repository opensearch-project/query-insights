/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.DEFAULT_TOP_N_QUERY_MAP;
import static org.opensearch.test.OpenSearchTestCase.buildNewFakeTransportAddress;
import static org.opensearch.test.OpenSearchTestCase.random;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLengthBetween;
import static org.opensearch.test.OpenSearchTestCase.randomArray;
import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;
import static org.opensearch.test.OpenSearchTestCase.randomLong;
import static org.opensearch.test.OpenSearchTestCase.randomLongBetween;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueries;
import org.opensearch.plugin.insights.rules.model.AggregationType;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.SourceString;
import org.opensearch.plugin.insights.settings.QueryCategorizationSettings;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.VersionUtils;

final public class QueryInsightsTestUtils {

    static String randomId = UUID.randomUUID().toString();

    public QueryInsightsTestUtils() {}

    /**
     * Returns list of randomly generated search query records with a specific id
     * @param count number of records
     * @return List of records
     */
    public static List<SearchQueryRecord> generateQueryInsightRecords(int count, String id) {
        return generateQueryInsightRecords(count, count, System.currentTimeMillis(), 0, AggregationType.DEFAULT_AGGREGATION_TYPE, id);
    }

    /**
     * Returns list of randomly generated search query records.
     * @param count number of records
     * @return List of records
     */
    public static List<SearchQueryRecord> generateQueryInsightRecords(int count) {
        return generateQueryInsightRecords(count, count, System.currentTimeMillis(), 0, AggregationType.DEFAULT_AGGREGATION_TYPE, randomId);
    }

    /**
     * Returns list of randomly generated search query records with specific searchSourceBuilder
     * @param count number of records
     * @param searchSourceBuilder source
     * @return List of records
     */
    public static List<SearchQueryRecord> generateQueryInsightRecords(int count, SearchSourceBuilder searchSourceBuilder) {
        List<SearchQueryRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            // Generate base record data
            List<SearchQueryRecord> baseRecords = generateQueryInsightRecords(1);
            SearchQueryRecord baseRecord = baseRecords.get(0);

            // Create new record with SearchSourceBuilder for categorization
            SearchQueryRecord recordWithSource = new SearchQueryRecord(
                baseRecord.getTimestamp(),
                baseRecord.getMeasurements(),
                baseRecord.getAttributes(),
                searchSourceBuilder,  // Pass SearchSourceBuilder for categorization
                baseRecord.getId()
            );

            // Update SOURCE attribute to SourceString
            recordWithSource.getAttributes().put(Attribute.SOURCE, new SourceString(searchSourceBuilder.toString()));
            records.add(recordWithSource);
        }
        return records;
    }

    /**
     * Returns list of randomly generated search query records with specific aggregation type for measurements
     * @param count number of records
     * @param aggregationType source
     * @return List of records
     */
    public static List<SearchQueryRecord> generateQueryInsightRecords(int count, AggregationType aggregationType) {
        return generateQueryInsightRecords(count, count, System.currentTimeMillis(), 0, aggregationType, randomId);
    }

    /**
     * Creates a List of random Query Insight Records for testing purpose
     */
    public static List<SearchQueryRecord> generateQueryInsightRecords(int lower, int upper, long startTimeStamp, long interval) {
        return generateQueryInsightRecords(lower, upper, startTimeStamp, interval, AggregationType.NONE, randomId);
    }

    /**
     * Creates a List of random Query Insight Records for testing purpose with dimenstion type specified
     */
    public static List<SearchQueryRecord> generateQueryInsightRecords(
        int lower,
        int upper,
        long startTimeStamp,
        long interval,
        AggregationType aggregationType,
        String id
    ) {
        List<SearchQueryRecord> records = new ArrayList<>();
        int countOfRecords = randomIntBetween(lower, upper);
        long timestamp = startTimeStamp;
        for (int i = 0; i < countOfRecords; ++i) {
            long latencyValue = randomLongBetween(1000, 10000); // Replace with actual method to generate a random long
            long cpuValue = randomLongBetween(1000, 10000);
            long memoryValue = randomLongBetween(1000, 10000);
            Map<MetricType, Measurement> measurements = new LinkedHashMap<>();
            measurements.put(MetricType.LATENCY, new Measurement(latencyValue, aggregationType));
            measurements.put(MetricType.CPU, new Measurement(cpuValue, aggregationType));
            measurements.put(MetricType.MEMORY, new Measurement(memoryValue, aggregationType));

            Map<String, Long> phaseLatencyMap = new LinkedHashMap<>();
            int countOfPhases = randomIntBetween(2, 5);
            for (int j = 0; j < countOfPhases; ++j) {
                phaseLatencyMap.put(randomAlphaOfLengthBetween(5, 10), randomLong());
            }

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(20); // Set the size parameter as needed

            Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.SEARCH_TYPE, SearchType.QUERY_THEN_FETCH.toString().toLowerCase(Locale.ROOT));
            attributes.put(Attribute.SOURCE, new SourceString(searchSourceBuilder.toString()));
            attributes.put(Attribute.TOTAL_SHARDS, randomIntBetween(1, 100));
            attributes.put(Attribute.INDICES, randomArray(1, 3, Object[]::new, () -> randomAlphaOfLengthBetween(5, 10)));
            attributes.put(Attribute.PHASE_LATENCY_MAP, phaseLatencyMap);
            attributes.put(Attribute.QUERY_GROUP_HASHCODE, Objects.hashCode(i));
            attributes.put(Attribute.GROUP_BY, GroupingType.NONE);
            attributes.put(Attribute.NODE_ID, "node_for_top_queries_test");
            attributes.put(Attribute.TOP_N_QUERY, DEFAULT_TOP_N_QUERY_MAP);
            attributes.put(Attribute.USERNAME, randomAlphaOfLengthBetween(5, 10));
            attributes.put(Attribute.USER_ROLES, new String[] { randomAlphaOfLengthBetween(4, 8), randomAlphaOfLengthBetween(4, 8) });
            attributes.put(
                Attribute.TASK_RESOURCE_USAGES,
                List.of(
                    new TaskResourceInfo(
                        randomAlphaOfLengthBetween(5, 10),
                        randomLongBetween(1, 1000),
                        randomLongBetween(1, 1000),
                        randomAlphaOfLengthBetween(5, 10),
                        new TaskResourceUsage(randomLongBetween(1, 1000), randomLongBetween(1, 1000))
                    ),
                    new TaskResourceInfo(
                        randomAlphaOfLengthBetween(5, 10),
                        randomLongBetween(1, 1000),
                        randomLongBetween(1, 1000),
                        randomAlphaOfLengthBetween(5, 10),
                        new TaskResourceUsage(randomLongBetween(1, 1000), randomLongBetween(1, 1000))
                    )
                )
            );

            records.add(new SearchQueryRecord(timestamp, measurements, attributes, searchSourceBuilder, id));
            timestamp += interval;
        }
        return records;
    }

    public static List<SearchQueryRecord> generateQueryInsightsRecordsWithMeasurement(
        int count,
        MetricType metricType,
        Number measurement
    ) {
        List<SearchQueryRecord> records = generateQueryInsightRecords(count);

        for (SearchQueryRecord record : records) {
            record.getMeasurements().get(metricType).setMeasurement(measurement);
        }
        return records;
    }

    public static List<List<SearchQueryRecord>> generateMultipleQueryInsightsRecordsWithMeasurement(
        int count,
        MetricType metricType,
        List<Number> measurements
    ) {
        List<List<SearchQueryRecord>> multipleRecordLists = new ArrayList<>();

        for (int i = 0; i < measurements.size(); i++) {
            List<SearchQueryRecord> records = generateQueryInsightRecords(count);
            multipleRecordLists.add(records);
            for (SearchQueryRecord record : records) {
                record.getMeasurements().get(metricType).setMeasurement(measurements.get(i));
            }
            QueryInsightsTestUtils.populateHashcode(records, i);
        }
        return multipleRecordLists;
    }

    public static void populateSameQueryHashcodes(List<SearchQueryRecord> searchQueryRecords) {
        for (SearchQueryRecord record : searchQueryRecords) {
            record.getAttributes().put(Attribute.QUERY_GROUP_HASHCODE, 1);
        }
    }

    public static void populateHashcode(List<SearchQueryRecord> searchQueryRecords, int hash) {
        for (SearchQueryRecord record : searchQueryRecords) {
            record.getAttributes().put(Attribute.QUERY_GROUP_HASHCODE, hash);
        }
    }

    public static TopQueries createRandomTopQueries() {
        DiscoveryNode node = new DiscoveryNode(
            "node_for_top_queries_test",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        List<SearchQueryRecord> records = generateQueryInsightRecords(10);

        return new TopQueries(node, records);
    }

    public static TopQueries createFixedTopQueries(String id) {
        DiscoveryNode node = new DiscoveryNode(
            "node_for_top_queries_test",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(createFixedSearchQueryRecord(id));

        return new TopQueries(node, records);
    }

    public static SearchQueryRecord createFixedSearchQueryRecord(String id) {
        long timestamp = 1706574180000L;
        Map<MetricType, Measurement> measurements = Map.of(MetricType.LATENCY, new Measurement(1L));

        Map<String, Long> phaseLatencyMap = new HashMap<>();
        phaseLatencyMap.put("expand", 1L);
        phaseLatencyMap.put("query", 10L);
        phaseLatencyMap.put("fetch", 1L);
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.SEARCH_TYPE, SearchType.QUERY_THEN_FETCH.toString().toLowerCase(Locale.ROOT));
        attributes.put(Attribute.PHASE_LATENCY_MAP, phaseLatencyMap);
        attributes.put(Attribute.NODE_ID, "node_for_top_queries_test");
        attributes.put(
            Attribute.TASK_RESOURCE_USAGES,
            List.of(
                new TaskResourceInfo("action", 2L, 1L, "id", new TaskResourceUsage(1000L, 2000L)),
                new TaskResourceInfo("action2", 3L, 1L, "id2", new TaskResourceUsage(2000L, 1000L))
            )
        );
        attributes.put(Attribute.TOP_N_QUERY, DEFAULT_TOP_N_QUERY_MAP);
        attributes.put(Attribute.USERNAME, "testuser");
        attributes.put(Attribute.USER_ROLES, new String[] { "admin", "user" });

        return new SearchQueryRecord(timestamp, measurements, attributes, id);
    }

    public static void compareJson(ToXContent param1, ToXContent param2) throws IOException {
        if (param1 == null || param2 == null) {
            assertNull(param1);
            assertNull(param2);
            return;
        }

        ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        XContentBuilder param1Builder = jsonBuilder();
        param1.toXContent(param1Builder, params);

        XContentBuilder param2Builder = jsonBuilder();
        param2.toXContent(param2Builder, params);

        assertEquals(param1Builder.toString(), param2Builder.toString());
    }

    public static boolean checkRecordsEquals(List<SearchQueryRecord> records1, List<SearchQueryRecord> records2) {
        if (records1.size() != records2.size()) {
            return false;
        }
        for (int i = 0; i < records1.size(); i++) {
            SearchQueryRecord record1 = records1.get(i);
            SearchQueryRecord record2 = records2.get(i);
            if (record1.getTimestamp() != record2.getTimestamp()) {
                return false;
            }
            if (!record1.getMeasurements().equals(record2.getMeasurements())) {
                return false;
            }
            if (!compareAttributes(record1.getAttributes(), record2.getAttributes())) {
                return false;
            }
        }
        return true;
    }

    private static boolean compareAttributes(Map<Attribute, Object> attributes1, Map<Attribute, Object> attributes2) {
        if (attributes1.size() != attributes2.size()) {
            return false;
        }
        for (Map.Entry<Attribute, Object> entry : attributes1.entrySet()) {
            Attribute key = entry.getKey();
            Object value1 = entry.getValue();
            Object value2 = attributes2.get(key);
            if (key == Attribute.SOURCE) {
                // Both values should be SourceString
                String source1 = value1 instanceof SourceString ? ((SourceString) value1).getValue() : null;
                String source2 = value2 instanceof SourceString ? ((SourceString) value2).getValue() : null;
                if (!Objects.equals(source1, source2)) {
                    return false;
                }
            } else if (value1 instanceof Object[] && value2 instanceof Object[]) {
                if (!Arrays.deepEquals((Object[]) value1, (Object[]) value2)) {
                    return false;
                }
            } else {
                if (!Objects.equals(value1, value2)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean checkRecordsEqualsWithoutOrder(
        List<SearchQueryRecord> records1,
        List<SearchQueryRecord> records2,
        MetricType metricType
    ) {
        Set<SearchQueryRecord> set2 = new TreeSet<>((a, b) -> SearchQueryRecord.compare(a, b, metricType));
        set2.addAll(records2);
        if (records1.size() != records2.size()) {
            return false;
        }
        for (int i = 0; i < records1.size(); i++) {
            if (!set2.contains(records1.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static void registerAllQueryInsightsSettings(ClusterSettings clusterSettings) {
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_CPU_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_CPU_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_CPU_QUERIES_WINDOW_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_MEMORY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_MEMORY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_MEMORY_QUERIES_WINDOW_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_EXPORTER_TYPE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_QUERIES_GROUP_BY);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_QUERIES_GROUPING_FIELD_NAME);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_QUERIES_GROUPING_FIELD_TYPE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_EXPORTER_DELETE_AFTER);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_QUERIES_EXCLUDED_INDICES);
        clusterSettings.registerSetting(QueryCategorizationSettings.SEARCH_QUERY_METRICS_ENABLED_SETTING);
    }
}
