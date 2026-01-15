/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Granular tests for the {@link SearchQueryRecord} class.
 */
public class SearchQueryRecordTests extends OpenSearchTestCase {

    /**
     * Check that if the serialization, deserialization and equals functions are working as expected
     */
    public void testSerializationAndEquals() throws Exception {
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        List<SearchQueryRecord> copiedRecords = new ArrayList<>();
        for (SearchQueryRecord record : records) {
            copiedRecords.add(roundTripRecord(record));
        }
        assertTrue(QueryInsightsTestUtils.checkRecordsEquals(records, copiedRecords));

    }

    public void testAllMetricTypes() {
        Set<MetricType> allMetrics = MetricType.allMetricTypes();
        Set<MetricType> expected = new HashSet<>(Arrays.asList(MetricType.LATENCY, MetricType.CPU, MetricType.MEMORY));
        assertEquals(expected, allMetrics);
    }

    public void testCompare() {
        SearchQueryRecord record1 = QueryInsightsTestUtils.createFixedSearchQueryRecord("id");
        SearchQueryRecord record2 = QueryInsightsTestUtils.createFixedSearchQueryRecord("id");
        assertEquals(0, SearchQueryRecord.compare(record1, record2, MetricType.LATENCY));
    }

    public void testEqual() {
        SearchQueryRecord record1 = QueryInsightsTestUtils.createFixedSearchQueryRecord("id");
        SearchQueryRecord record2 = QueryInsightsTestUtils.createFixedSearchQueryRecord("id");
        assertEquals(record1, record2);
    }

    public void testToXContent() throws IOException {
        SearchQueryRecord originalRecord = QueryInsightsTestUtils.createFixedSearchQueryRecord("id");

        // Serialize with toXContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        originalRecord.toXContent(builder, null);
        builder.flush();

        // Deserialize with fromXContent
        String json = builder.toString();
        XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json);
        SearchQueryRecord parsedRecord = SearchQueryRecord.fromXContent(parser);

        originalRecord.getAttributes().remove(Attribute.TOP_N_QUERY);
        assertEquals(originalRecord, parsedRecord);
    }

    public void testToXContentForExport() throws IOException {
        SearchQueryRecord originalRecord = QueryInsightsTestUtils.createFixedSearchQueryRecord("id");

        // Serialize with toXContentForExport
        XContentBuilder builder = XContentFactory.jsonBuilder();
        originalRecord.toXContentForExport(builder, null);
        builder.flush();

        // Deserialize with fromXContent
        String json = builder.toString();
        XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json);
        SearchQueryRecord parsedRecord = SearchQueryRecord.fromXContent(parser);

        assertEquals(originalRecord, parsedRecord);
    }

    public void testFromXContent() {
        SearchQueryRecord record = QueryInsightsTestUtils.createFixedSearchQueryRecord("id");
        try (XContentParser recordParser = createParser(JsonXContent.jsonXContent, record.toString())) {
            SearchQueryRecord parsedRecord = SearchQueryRecord.fromXContent(recordParser);
            QueryInsightsTestUtils.checkRecordsEquals(List.of(record), List.of(parsedRecord));
        } catch (Exception e) {
            fail("Test should not throw exceptions when parsing search query record");
        }
    }

    public void testFromXContentWithSearchSourceBuilderObject() throws IOException {
        // Test backward compatibility: existing Local Index data may have SearchSourceBuilder objects
        // After parsing, SOURCE should be converted to string format
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(10);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("timestamp", 1706574180000L);
        builder.field("id", "test-id");
        builder.field("source");
        sourceBuilder.toXContent(builder, null); // Store as object (old format)
        builder.startObject("measurements");
        builder.startObject("latency");
        builder.field("number", 1L);
        builder.field("count", 1);
        builder.field("aggregationType", "NONE");
        builder.endObject();
        builder.endObject();
        builder.endObject();

        String json = builder.toString();
        XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json);
        SearchQueryRecord parsedRecord = SearchQueryRecord.fromXContent(parser);

        // Verify SOURCE is converted to SourceString format for consistency
        Object sourceValue = parsedRecord.getAttributes().get(Attribute.SOURCE);
        assertTrue("SOURCE should be converted to SourceString format", sourceValue instanceof SourceString);
        assertEquals("SOURCE content should match original", sourceBuilder.toString(), ((SourceString) sourceValue).getValue());

        // SearchSourceBuilder is null when parsing from persisted data
        assertNull("SearchSourceBuilder should be null when parsing from persisted data", parsedRecord.getSearchSourceBuilder());
    }

    public void testBackwardCompatibilityWithMatchAllQuery() throws IOException {
        // Test backward compatibility with simple SearchSourceBuilder (no complex queries)
        // After parsing, SOURCE should be converted to string format
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(5);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("timestamp", 1234567890L);
        builder.field("id", "test-id");
        builder.field("source");
        sourceBuilder.toXContent(builder, null);
        builder.startObject("measurements");
        builder.startObject("latency");
        builder.field("number", 1L);
        builder.field("count", 1);
        builder.field("aggregationType", "NONE");
        builder.endObject();
        builder.endObject();
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, builder.toString());
        SearchQueryRecord record = SearchQueryRecord.fromXContent(parser);

        Object source = record.getAttributes().get(Attribute.SOURCE);
        assertNotNull("Source should not be null", source);
        assertTrue("SOURCE should be converted to SourceString format", source instanceof SourceString);
        assertTrue("Source should contain size parameter", ((SourceString) source).getValue().contains("size"));

        // SearchSourceBuilder is null when parsing from persisted data
        assertNull("SearchSourceBuilder should be null when parsing from persisted data", record.getSearchSourceBuilder());
    }

    public void testToXContentForExportWithObjectSource() throws IOException {
        // Create record with SearchSourceBuilder for object source test
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(10);
        java.util.Map<MetricType, Measurement> measurements = new java.util.HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(100.0));
        java.util.Map<Attribute, Object> attributes = new java.util.HashMap<>();
        attributes.put(Attribute.SOURCE, new SourceString(sourceBuilder.toString()));

        SearchQueryRecord record = new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, sourceBuilder, "test-id");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContentForExport(builder, null, true);

        String json = builder.toString();

        // Verify the source field is serialized as an object (SearchSourceBuilder), not a string
        assertTrue("JSON should contain source as object with size", json.contains("\"source\":{\"size\":10}"));
    }

    public void testSourceStringToString() {
        String testValue = "test source string";
        SourceString sourceString = new SourceString(testValue);
        assertEquals("toString should return the wrapped value", testValue, sourceString.toString());
    }

    /**
     * Test that all fields added to SearchQueryRecord code are properly mapped in top-queries-record.json.
     * This test validates enum values to ensure mapping completeness independent of test data generation.
     *
     * This prevents mapping conflicts when users have custom index templates by ensuring all fields
     * that can appear in SearchQueryRecord have explicit mappings defined.
     */
    @SuppressWarnings("unchecked")
    public void testAllEnumFieldsAreMappedInIndexMapping() throws Exception {
        // Load the mapping file
        Map<String, Object> mapping = loadMappingFile();
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        assertNotNull("Properties field missing in mapping", properties);

        // Attributes that are explicitly excluded from serialization and don't need mapping
        Set<String> excludedAttributes = new HashSet<>(Arrays.asList("top_n_query", "description"));

        // Check ALL Attribute enum values are mapped (except excluded ones)
        List<String> missingAttributes = new ArrayList<>();
        for (Attribute attr : Attribute.values()) {
            String fieldName = attr.toString(); // converts to lowercase
            if (!excludedAttributes.contains(fieldName) && !properties.containsKey(fieldName)) {
                missingAttributes.add("Attribute." + attr.name() + " (field: '" + fieldName + "')");
            }
        }
        assertTrue("The following Attribute enum values are missing from mapping: " + missingAttributes, missingAttributes.isEmpty());

        // Check ALL MetricType enum values have complete measurement mappings
        assertTrue("Field 'measurements' is missing from mapping", properties.containsKey("measurements"));
        Map<String, Object> measurementsMapping = (Map<String, Object>) properties.get("measurements");
        Map<String, Object> measurementsProps = (Map<String, Object>) measurementsMapping.get("properties");
        assertNotNull("measurements should have properties", measurementsProps);

        List<String> missingMeasurements = new ArrayList<>();
        for (MetricType metric : MetricType.values()) {
            String metricName = metric.toString();
            if (!measurementsProps.containsKey(metricName)) {
                missingMeasurements.add("MetricType." + metric.name() + " (field: '" + metricName + "')");
                continue;
            }

            // Verify each metric has number, count, aggregationType (from Measurement class)
            Map<String, Object> metricMapping = (Map<String, Object>) measurementsProps.get(metricName);
            Map<String, Object> metricProps = (Map<String, Object>) metricMapping.get("properties");
            if (metricProps == null) {
                missingMeasurements.add(metricName + " has no properties");
                continue;
            }

            if (!metricProps.containsKey("number")) {
                missingMeasurements.add(metricName + ".number");
            }
            if (!metricProps.containsKey("count")) {
                missingMeasurements.add(metricName + ".count");
            }
            if (!metricProps.containsKey("aggregationType")) {
                missingMeasurements.add(metricName + ".aggregationType");
            }
        }
        assertTrue("The following measurement fields are missing from mapping: " + missingMeasurements, missingMeasurements.isEmpty());

        // Check all SearchPhaseName enum values are in phase_latency_map
        assertTrue("Field 'phase_latency_map' is missing from mapping", properties.containsKey("phase_latency_map"));
        Map<String, Object> phaseLatencyMapping = (Map<String, Object>) properties.get("phase_latency_map");
        Map<String, Object> phaseProperties = (Map<String, Object>) phaseLatencyMapping.get("properties");
        assertNotNull("phase_latency_map should have properties", phaseProperties);

        List<String> missingPhases = new ArrayList<>();
        for (SearchPhaseName phase : SearchPhaseName.values()) {
            String phaseName = phase.getName();
            if (!phaseProperties.containsKey(phaseName)) {
                missingPhases.add("SearchPhaseName." + phase.name() + " (field: '" + phaseName + "')");
            }
        }
        assertTrue(
            "The following SearchPhaseName enum values are missing from phase_latency_map: " + missingPhases,
            missingPhases.isEmpty()
        );
    }

    /**
     * Load the mapping file from resources.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> loadMappingFile() throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("mappings/top-queries-record.json")) {
            assertNotNull("Mapping file mappings/top-queries-record.json not found", is);
            XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, is);
            return parser.map();
        }
    }

    /**
     * Serialize and deserialize a SearchQueryRecord.
     * @param record A SearchQueryRecord to serialize.
     * @return The deserialized, "round-tripped" record.
     */
    private static SearchQueryRecord roundTripRecord(SearchQueryRecord record) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            record.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new SearchQueryRecord(in);
            }
        }
    }
}
