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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;
import org.opensearch.plugin.insights.rules.model.recommendations.Recommendation;
import org.opensearch.plugin.insights.rules.model.recommendations.RecommendationType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

/**
 * Granular tests for the {@link SearchQueryRecord} class.
 */
public class SearchQueryRecordTests extends OpenSearchTestCase {

    private final ThreadPool threadPool = QueryInsightsTestUtils.createMockThreadPool();

    /** A reported SQL/PPL record, with no SearchSourceBuilder, survives the transport round trip. */
    public void testReportQueryRecordAttributesSurviveRoundTrip() throws Exception {
        java.util.Map<MetricType, Measurement> measurements = new java.util.HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(1559L, AggregationType.NONE));
        measurements.put(MetricType.CPU, new Measurement(683111000L, AggregationType.NONE));
        measurements.put(MetricType.MEMORY, new Measurement(266494392L, AggregationType.NONE));

        java.util.Map<Attribute, Object> attributes = new java.util.HashMap<>();
        attributes.put(Attribute.QUERY_SOURCE, "PPL");
        attributes.put(Attribute.NODE_ID, "node-1");
        attributes.put(Attribute.CHILD_SEARCHES, childSearches());

        SearchQueryRecord record = new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, "PPL:node-1:42");

        SearchQueryRecord roundTripped = roundTripRecord(record);
        Map<Attribute, Object> after = roundTripped.getAttributes();
        assertEquals("PPL", after.get(Attribute.QUERY_SOURCE));
        assertEquals("node-1", after.get(Attribute.NODE_ID));
        assertEquals(childSearches(), after.get(Attribute.CHILD_SEARCHES));
    }

    /** A SQL/PPL record read back from its document, as the detail view reads it, keeps its searches. */
    public void testChildSearchesReadBackFromTheRecordsDocument() throws Exception {
        java.util.Map<Attribute, Object> attributes = new java.util.HashMap<>();
        attributes.put(Attribute.QUERY_SOURCE, "PPL");
        attributes.put(Attribute.CHILD_SEARCHES, childSearches());
        SearchQueryRecord record = new SearchQueryRecord(1_700_000_000_000L, new java.util.HashMap<>(), attributes, "ppl-1");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContentForExport(builder, ToXContent.EMPTY_PARAMS);
        String document = BytesReference.bytes(builder).utf8ToString();

        // Stored as a JSON string, which a reader before 3.11 skips; it would walk into an object.
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, document)) {
            assertEquals(childSearches().toJson(), parser.map().get("child_searches"));
        }
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, document)) {
            SearchQueryRecord read = SearchQueryRecord.fromXContent(parser);

            assertEquals(childSearches(), read.getAttributes().get(Attribute.CHILD_SEARCHES));
            assertEquals("PPL", read.getAttributes().get(Attribute.QUERY_SOURCE));
        }
    }

    /** A response carries the searches as an object; only the stored document has the string. */
    public void testChildSearchesAreAnObjectInAResponse() throws Exception {
        java.util.Map<Attribute, Object> attributes = new java.util.HashMap<>();
        attributes.put(Attribute.CHILD_SEARCHES, childSearches());
        SearchQueryRecord record = new SearchQueryRecord(1_700_000_000_000L, new java.util.HashMap<>(), attributes, "ppl-1");

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, record.toString())) {
            assertEquals(3, ((Map<?, ?>) parser.map().get("child_searches")).get("count"));
        }
    }

    public void testAFieldThisVersionDoesNotKnowIsSkippedWhole() throws Exception {
        // A later version's nested field, ahead of fields this version reads.
        String document = "{\"timestamp\":5,\"id\":\"a\","
            + "\"later_field\":{\"timestamp\":9,\"indices\":[\"other\"],\"nested\":{\"failed\":true}},"
            + "\"indices\":[\"logs\"],"
            + "\"measurements\":{\"latency\":{\"number\":7,\"count\":1,\"aggregationType\":\"NONE\"}}}";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, document)) {
            SearchQueryRecord read = SearchQueryRecord.fromXContent(parser);

            assertEquals(5L, read.getTimestamp());
            assertArrayEquals(new Object[] { "logs" }, (Object[]) read.getAttributes().get(Attribute.INDICES));
            assertNull(read.getAttributes().get(Attribute.FAILED));
            assertEquals(7L, read.getMeasurement(MetricType.LATENCY).longValue());
        }
    }

    /** Only a SQL/PPL record gets a null source; a DSL record without one is written as before. */
    public void testADslRecordWithoutASourceGetsNoNullSource() throws Exception {
        java.util.Map<Attribute, Object> attributes = new java.util.HashMap<>();
        attributes.put(Attribute.NODE_ID, "node-1");
        SearchQueryRecord record = new SearchQueryRecord(1_700_000_000_000L, new java.util.HashMap<>(), attributes, "dsl-1");

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, record.toString())) {
            assertFalse(parser.map().containsKey("source"));
        }
    }

    public void testChildSearchesAreLeftOutOfANonVerboseRecord() {
        java.util.Map<Attribute, Object> attributes = new java.util.HashMap<>();
        attributes.put(Attribute.QUERY_SOURCE, "PPL");
        attributes.put(Attribute.CHILD_SEARCHES, childSearches());
        SearchQueryRecord record = new SearchQueryRecord(1_700_000_000_000L, new java.util.HashMap<>(), attributes, "ppl-1");

        assertNull(record.copyAndSimplifyRecord().getAttributes().get(Attribute.CHILD_SEARCHES));
        assertEquals(childSearches(), record.getAttributes().get(Attribute.CHILD_SEARCHES));
    }

    /**
     * A SQL/PPL query's text is query_text in responses and documents. A response has source as null,
     * which Dashboards 3.0 to 3.4 parse without failing; the document has no source.
     */
    public void testAPplQuerysTextIsQueryTextNotSource() throws Exception {
        java.util.Map<Attribute, Object> attributes = new java.util.HashMap<>();
        attributes.put(Attribute.QUERY_SOURCE, "PPL");
        attributes.put(Attribute.QUERY_TEXT, "source=logs | head 5");
        attributes.put(Attribute.SOURCE_TRUNCATED, false);
        SearchQueryRecord record = new SearchQueryRecord(1_700_000_000_000L, new java.util.HashMap<>(), attributes, "ppl-1");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContentForExport(builder, ToXContent.EMPTY_PARAMS);
        String document = BytesReference.bytes(builder).utf8ToString();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, record.toString())) {
            Map<String, Object> response = parser.map();
            assertEquals("source=logs | head 5", response.get("query_text"));
            assertTrue("a response has source, as null", response.containsKey("source"));
            assertNull(response.get("source"));
        }
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, document)) {
            Map<String, Object> stored = parser.map();
            assertEquals("source=logs | head 5", stored.get("query_text"));
            assertFalse(stored.containsKey("source"));
        }
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, document)) {
            SearchQueryRecord read = SearchQueryRecord.fromXContent(parser);
            assertEquals("source=logs | head 5", read.getAttributes().get(Attribute.QUERY_TEXT));
            assertNull(read.getAttributes().get(Attribute.SOURCE));
        }
        // Like a DSL source, it's left out of the overview.
        assertNull(record.copyAndSimplifyRecord().getAttributes().get(Attribute.QUERY_TEXT));
    }

    private static ChildSearches childSearches() {
        return new ChildSearches(
            3,
            List.of(
                new ChildSearches.Search(1_700_000_000_000L, List.of("logs"), 2, 12L, 4_000_000L, 2_048L, false, "analytics"),
                new ChildSearches.Search(1_700_000_000_100L, List.of("logs"), 2, 7L, 1_000_000L, 512L, true, null)
            )
        );
    }

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
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS,
            json
        );
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
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS,
            json
        );
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
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS,
            json
        );
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

        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS,
            builder.toString()
        );
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

        SearchQueryRecord record = new SearchQueryRecord(
            System.currentTimeMillis(),
            measurements,
            attributes,
            sourceBuilder,
            new UserPrincipalContext(threadPool),
            "test-id"
        );

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

    public void testFromXContentWithFailedField() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("timestamp", 1706574180000L);
        builder.field("id", "test-id");
        builder.field(SearchQueryRecord.FAILED, true);
        builder.startObject("measurements");
        builder.startObject("latency");
        builder.field("number", 1L);
        builder.field("count", 1);
        builder.field("aggregationType", "NONE");
        builder.endObject();
        builder.endObject();
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS,
            builder.toString()
        );
        SearchQueryRecord record = SearchQueryRecord.fromXContent(parser);

        assertEquals(true, record.getAttributes().get(Attribute.FAILED));
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

    public void testToXContentWithRecommendations() throws IOException {
        SearchQueryRecord record = QueryInsightsTestUtils.createFixedSearchQueryRecord("test_rec_id");

        Recommendation rec = Recommendation.builder()
            .ruleId("test-rule")
            .title("Test Title")
            .description("Test Description")
            .type(RecommendationType.QUERY_REWRITE)
            .confidence(0.85)
            .build();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContentWithRecommendations(builder, ToXContent.EMPTY_PARAMS, List.of(rec));
        builder.flush();

        String json = builder.toString();
        assertTrue(json.contains("\"recommendations\""));
        assertTrue(json.contains("\"rule_id\":\"test-rule\""));
        assertTrue(json.contains("\"title\":\"Test Title\""));
        assertTrue(json.contains("\"description\":\"Test Description\""));
        assertTrue(json.contains("\"type\":\"query_rewrite\""));
        assertTrue(json.contains("\"confidence\":0.85"));
        assertTrue(json.contains("\"timestamp\":1706574180000"));
        assertTrue(json.contains("\"id\":\"test_rec_id\""));
    }

    public void testToXContentWithEmptyRecommendations() throws IOException {
        SearchQueryRecord record = QueryInsightsTestUtils.createFixedSearchQueryRecord("test_id");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContentWithRecommendations(builder, ToXContent.EMPTY_PARAMS, List.of());
        builder.flush();

        String json = builder.toString();
        assertTrue("Empty recommendations list should produce empty recommendations array", json.contains("\"recommendations\":[]"));
    }

    public void testToXContentWithNullRecommendations() throws IOException {
        SearchQueryRecord record = QueryInsightsTestUtils.createFixedSearchQueryRecord("test_id");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContentWithRecommendations(builder, ToXContent.EMPTY_PARAMS, null);
        builder.flush();

        String json = builder.toString();
        assertFalse("Null recommendations should not produce recommendations field", json.contains("\"recommendations\""));
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
