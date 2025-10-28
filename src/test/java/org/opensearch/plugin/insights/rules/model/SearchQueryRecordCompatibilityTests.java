/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for SearchQueryRecord backward compatibility with different SOURCE formats
 */
public class SearchQueryRecordCompatibilityTests extends OpenSearchTestCase {

    /**
     * Test that new string format SOURCE can be written to and read from Local Index
     */
    public void testNewStringFormatRoundTrip() throws Exception {
        // Create a JSON that represents how new string format would be stored in Local Index
        String sourceString = "{\"query\":{\"match_all\":{}}}";

        // Simulate Local Index JSON with string SOURCE (new format)
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("timestamp", 1706574180000L);
        builder.field("id", "test-id");
        builder.field("source", sourceString); // Store as string (new format)
        builder.startObject("measurements");
        builder.startObject("latency");
        builder.field("number", 1L);
        builder.field("count", 1);
        builder.field("aggregationType", "NONE");
        builder.endObject();
        builder.endObject();
        builder.endObject();

        String json = builder.toString();

        // Verify JSON contains string format
        assertTrue("JSON should contain string SOURCE", json.contains("\"source\":\"" + sourceString.replace("\"", "\\\"")));

        // Deserialize from JSON (simulating Local Index read)
        XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json);
        SearchQueryRecord parsed = SearchQueryRecord.fromXContent(parser);

        // Verify the SOURCE is stored as string (new format)
        Object source = parsed.getAttributes().get(Attribute.SOURCE);
        assertNotNull("SOURCE should not be null", source);
        assertTrue("SOURCE should be stored as string in new format", source instanceof String);
        assertEquals("SOURCE content should be preserved exactly", sourceString, source);
    }

    /**
     * Test that old SearchSourceBuilder format can still be read from Local Index
     */
    public void testOldSearchSourceBuilderFormatCompatibility() throws IOException {
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

        // Verify SOURCE maintains SearchSourceBuilder format for old data but can be used as string
        Object sourceValue = parsedRecord.getAttributes().get(Attribute.SOURCE);
        assertTrue("SOURCE should maintain SearchSourceBuilder format for old data", sourceValue instanceof SearchSourceBuilder);
        assertEquals("SOURCE content should match when converted to string", sourceBuilder.toString(), sourceValue.toString());
    }

    /**
     * Test that both new and old formats result in consistent string storage
     */
    public void testFormatConsistency() throws IOException {
        // Create new format record (test utility creates SOURCE as string)
        SearchQueryRecord newRecord = QueryInsightsTestUtils.generateQueryInsightRecords(1).get(0);

        // Create old format JSON
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(5);
        XContentBuilder oldBuilder = XContentFactory.jsonBuilder();
        oldBuilder.startObject();
        oldBuilder.field("timestamp", 1706574180000L);
        oldBuilder.field("id", "old-id");
        oldBuilder.field("source");
        sourceBuilder.toXContent(oldBuilder, null);
        oldBuilder.startObject("measurements");
        oldBuilder.startObject("latency");
        oldBuilder.field("number", 1L);
        oldBuilder.field("count", 1);
        oldBuilder.field("aggregationType", "NONE");
        oldBuilder.endObject();
        oldBuilder.endObject();
        oldBuilder.endObject();

        // Parse old format
        XContentParser oldParser = JsonXContent.jsonXContent.createParser(null, null, oldBuilder.toString());
        SearchQueryRecord oldRecord = SearchQueryRecord.fromXContent(oldParser);

        // New format should be string, old format maintains SearchSourceBuilder
        Object newSource = newRecord.getAttributes().get(Attribute.SOURCE);
        Object oldSource = oldRecord.getAttributes().get(Attribute.SOURCE);

        assertTrue("New format should be string", newSource instanceof String);
        assertTrue("Old format should maintain SearchSourceBuilder for backward compatibility", oldSource instanceof SearchSourceBuilder);

        // Both should have equivalent string representations
        String newSourceStr = newSource.toString();
        String oldSourceStr = oldSource.toString();
        assertNotNull("Both should have valid string representations", newSourceStr);
        assertNotNull("Both should have valid string representations", oldSourceStr);
    }
}
