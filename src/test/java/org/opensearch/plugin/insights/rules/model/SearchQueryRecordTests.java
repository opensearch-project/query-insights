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

        // Verify SOURCE maintains its original format (SearchSourceBuilder) for backward compatibility
        // but can be converted to string when needed
        Object sourceValue = parsedRecord.getAttributes().get(Attribute.SOURCE);
        assertTrue("SOURCE should maintain SearchSourceBuilder format for old data", sourceValue instanceof SearchSourceBuilder);
        assertEquals("SOURCE content should match when converted to string", sourceBuilder.toString(), sourceValue.toString());
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
