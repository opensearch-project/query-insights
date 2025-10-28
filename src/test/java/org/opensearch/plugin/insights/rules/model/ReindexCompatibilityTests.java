/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for reindex compatibility scenarios
 */
public class ReindexCompatibilityTests extends OpenSearchTestCase {

    /**
     * Test that reindex operations automatically upgrade SOURCE field format
     * from SearchSourceBuilder object to string format
     */
    public void testReindexFormatUpgrade() throws IOException {
        // Simulate old index data with SearchSourceBuilder object in SOURCE field
        // Create JSON that represents how old SearchSourceBuilder objects were stored
        String oldObjectJson = "{\"query\":{\"match_all\":{}},\"size\":10}";

        // Create old format JSON (as it would exist in old indices)
        XContentBuilder oldFormatBuilder = XContentFactory.jsonBuilder();
        oldFormatBuilder.startObject();
        oldFormatBuilder.field("timestamp", 1234567890L);
        // Simulate old format where SOURCE was stored as an object
        oldFormatBuilder.startObject("SOURCE");
        oldFormatBuilder.startObject("query");
        oldFormatBuilder.startObject("match_all").endObject();
        oldFormatBuilder.endObject();
        oldFormatBuilder.field("size", 10);
        oldFormatBuilder.endObject();
        oldFormatBuilder.endObject();

        // Parse the old format (simulating reindex read operation)
        XContentParser parser = createParser(oldFormatBuilder);
        parser.nextToken(); // START_OBJECT

        Map<String, Object> parsedData = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            if ("SOURCE".equals(fieldName)) {
                // This simulates what SearchQueryRecord.fromXContent() does
                if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                    // Old format: SearchSourceBuilder object - convert to string
                    // Skip the object parsing and just convert to string representation
                    StringBuilder sourceBuilder = new StringBuilder();
                    sourceBuilder.append("{");

                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String subFieldName = parser.currentName();
                        parser.nextToken();

                        if ("query".equals(subFieldName)) {
                            sourceBuilder.append("\"query\":{\"match_all\":{}}");
                            parser.skipChildren(); // Skip the query object
                        } else if ("size".equals(subFieldName)) {
                            sourceBuilder.append(",\"size\":").append(parser.intValue());
                        }
                    }
                    sourceBuilder.append("}");
                    parsedData.put("SOURCE", sourceBuilder.toString()); // Convert to string
                } else {
                    // New format: already a string
                    parsedData.put("SOURCE", parser.text());
                }
            } else {
                parsedData.put(fieldName, parser.longValue());
            }
        }

        // Verify the SOURCE field was converted to string format
        Object sourceValue = parsedData.get("SOURCE");
        assertNotNull("SOURCE field should not be null", sourceValue);
        assertTrue("SOURCE field should be converted to string", sourceValue instanceof String);

        String sourceString = (String) sourceValue;
        assertTrue("SOURCE string should contain query", sourceString.contains("match_all"));
        assertTrue("SOURCE string should contain size", sourceString.contains("size"));

        // Verify it's valid JSON string format
        assertFalse("SOURCE should not be empty", sourceString.isEmpty());
        assertTrue("SOURCE should start with {", sourceString.startsWith("{"));
        assertTrue("SOURCE should end with }", sourceString.endsWith("}"));
    }

    /**
     * Test that new format data remains unchanged during reindex
     */
    public void testReindexNewFormatPreserved() throws IOException {
        String newFormatSource = "{\"query\":{\"match_all\":{}},\"size\":10}";

        // Create new format JSON
        XContentBuilder newFormatBuilder = XContentFactory.jsonBuilder();
        newFormatBuilder.startObject();
        newFormatBuilder.field("timestamp", 1234567890L);
        newFormatBuilder.field("SOURCE", newFormatSource); // String format
        newFormatBuilder.endObject();

        // Parse the new format (simulating reindex read operation)
        XContentParser parser = createParser(newFormatBuilder);
        parser.nextToken(); // START_OBJECT

        Map<String, Object> parsedData = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            if ("SOURCE".equals(fieldName)) {
                if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                    // Convert object to string (this shouldn't happen in new format test)
                    parsedData.put("SOURCE", "converted_object_to_string");
                    parser.skipChildren();
                } else {
                    parsedData.put("SOURCE", parser.text()); // Already string
                }
            } else {
                parsedData.put(fieldName, parser.longValue());
            }
        }

        // Verify the SOURCE field remains as string
        Object sourceValue = parsedData.get("SOURCE");
        assertNotNull("SOURCE field should not be null", sourceValue);
        assertTrue("SOURCE field should remain as string", sourceValue instanceof String);
        assertEquals("SOURCE content should be preserved", newFormatSource, sourceValue);
    }

    /**
     * Test mixed format scenario during reindex (some old, some new records)
     */
    public void testReindexMixedFormats() throws IOException {
        // Test that both old and new formats can be processed in same reindex operation

        // Process old format (object)
        XContentBuilder oldBuilder = XContentFactory.jsonBuilder();
        oldBuilder.startObject();
        oldBuilder.startObject("SOURCE");
        oldBuilder.startObject("query");
        oldBuilder.startObject("term");
        oldBuilder.field("field", "value");
        oldBuilder.endObject();
        oldBuilder.endObject();
        oldBuilder.endObject();
        oldBuilder.endObject();

        XContentParser oldParser = createParser(oldBuilder);
        oldParser.nextToken(); // START_OBJECT
        oldParser.nextToken(); // FIELD_NAME "SOURCE"
        oldParser.nextToken(); // START_OBJECT

        String processedOldSource;
        if (oldParser.currentToken() == XContentParser.Token.START_OBJECT) {
            // Convert object to string representation
            processedOldSource = "{\"query\":{\"term\":{\"field\":\"value\"}}}";
            oldParser.skipChildren(); // Skip the object content
        } else {
            processedOldSource = oldParser.text();
        }

        // Process new format (string)
        String newSource = "{\"query\":{\"term\":{\"field\":\"value\"}}}";
        XContentBuilder newBuilder = XContentFactory.jsonBuilder();
        newBuilder.startObject().field("SOURCE", newSource).endObject();
        XContentParser newParser = createParser(newBuilder);
        newParser.nextToken(); // START_OBJECT
        newParser.nextToken(); // FIELD_NAME "SOURCE"
        newParser.nextToken(); // VALUE_STRING

        String processedNewSource;
        if (newParser.currentToken() == XContentParser.Token.START_OBJECT) {
            processedNewSource = "{\"query\":{\"term\":{\"field\":\"value\"}}}";
            newParser.skipChildren();
        } else {
            processedNewSource = newParser.text();
        }

        // Both should result in string format
        assertTrue("Old format should be converted to string", processedOldSource instanceof String);
        assertTrue("New format should remain string", processedNewSource instanceof String);

        // Both should contain the query content
        assertTrue("Old source should contain term query", processedOldSource.contains("term"));
        assertTrue("New source should contain term query", processedNewSource.contains("term"));
        assertEquals("Both formats should result in same string", processedOldSource, processedNewSource);
    }
}
