/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

        // Verify SOURCE is converted to string format for consistency
        Object sourceValue = parsedRecord.getAttributes().get(Attribute.SOURCE);
        assertTrue("SOURCE should be converted to string format", sourceValue instanceof String);
        assertEquals("SOURCE content should match original", sourceBuilder.toString(), sourceValue.toString());

        // Verify SearchSourceBuilder is available for categorization
        assertNotNull("SearchSourceBuilder should be available for categorization", parsedRecord.getSearchSourceBuilder());
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
        assertTrue("SOURCE should be converted to string format", source instanceof String);
        assertTrue("Source should contain size parameter", source.toString().contains("size"));

        // Verify SearchSourceBuilder is available for categorization
        assertNotNull("SearchSourceBuilder should be available for categorization", record.getSearchSourceBuilder());
    }

    public void testToXContentForExportWithObjectSource() throws IOException {
        // Create record with SearchSourceBuilder for object source test
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(10);
        java.util.Map<MetricType, Measurement> measurements = new java.util.HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(100.0));
        java.util.Map<Attribute, Object> attributes = new java.util.HashMap<>();
        attributes.put(Attribute.SOURCE, sourceBuilder.toString());

        SearchQueryRecord record = new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, sourceBuilder, "test-id");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContentForExport(builder, null, true);

        String json = builder.toString();

        // Verify the source field is serialized as an object (SearchSourceBuilder), not a string
        assertTrue("JSON should contain source as object with size", json.contains("\"source\":{\"size\":10}"));
    }

    public void testToXContentForExportWithObjectSource_InvalidJson() throws IOException {
        SearchQueryRecord record = QueryInsightsTestUtils.createFixedSearchQueryRecord("test-id");
        record.getAttributes().put(Attribute.SOURCE, "invalid-json-string");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContentForExport(builder, null, true);

        String json = builder.toString();

        // Verify fallback to string when JSON parsing fails
        assertTrue("JSON should contain source as string fallback", json.contains("\"source\":\"invalid-json-string\""));
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
