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
import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for the {@link Attribute} class, focusing on version compatibility
 * for rolling upgrades between different OpenSearch versions.
 */
public class AttributeTests extends OpenSearchTestCase {

    /**
     * Test that SOURCE attribute is correctly serialized/deserialized
     * when sending from 3.4+ node to pre-3.4 node (string to string)
     */
    public void testSourceAttributeNewToOldVersion() throws IOException {
        // Simulate 3.4+ node sending string SOURCE to pre-3.4 node
        String sourceString = "{\"size\":10,\"query\":{\"match_all\":{}}}";

        // Write with 3.4+ version (should write as string via writeGenericValue)
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("3.4.0"));
        Attribute.writeValueTo(out, sourceString);

        // Read with pre-3.4 version (should read as string since it was written as string)
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.fromString("3.3.0"));
        Object result = Attribute.readAttributeValue(in, Attribute.SOURCE);

        assertTrue("Should read as string for pre-3.4 versions", result instanceof String);
        assertEquals("String content should match", sourceString, result);
    }

    /**
     * Test that SOURCE attribute is correctly serialized/deserialized
     * when sending from pre-3.4 node to 3.4+ node (SearchSourceBuilder to string)
     */
    public void testSourceAttributeOldToNewVersion() throws IOException {
        // Simulate pre-3.4 node sending SearchSourceBuilder SOURCE to 3.4+ node
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(10);

        // Write with pre-3.4 version (should write as SearchSourceBuilder binary)
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("3.3.0"));
        Attribute.writeValueTo(out, sourceBuilder);

        // Read with 3.4+ version (should read as string)
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.fromString("3.4.0"));
        Object result = Attribute.readAttributeValue(in, Attribute.SOURCE);

        assertTrue("Should read as string for 3.4+ versions", result instanceof String);
        assertNotNull("Result should not be null", result);
    }

    /**
     * Test that SOURCE attribute works correctly within same version (3.4+)
     */
    public void testSourceAttributeSameNewVersion() throws IOException {
        String sourceString = "{\"size\":5,\"query\":{\"term\":{\"field\":\"value\"}}}";

        // Both write and read with 3.4+ version
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("3.4.0"));
        Attribute.writeValueTo(out, sourceString);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.fromString("3.4.0"));
        Object result = Attribute.readAttributeValue(in, Attribute.SOURCE);

        assertTrue("Should remain as string", result instanceof String);
        assertEquals("String content should match", sourceString, result.toString());
    }

    /**
     * Test that SOURCE attribute works correctly within same version (pre-3.4)
     */
    public void testSourceAttributeSameOldVersion() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(20);

        // Both write and read with pre-3.4 version
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("3.3.0"));
        Attribute.writeValueTo(out, sourceBuilder);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.fromString("3.3.0"));
        Object result = Attribute.readAttributeValue(in, Attribute.SOURCE);

        assertTrue("Should remain as SearchSourceBuilder", result instanceof SearchSourceBuilder);
        assertEquals("SearchSourceBuilder content should match", sourceBuilder.toString(), result.toString());
    }

    /**
     * Test attribute map serialization/deserialization across versions
     */
    public void testAttributeMapVersionCompatibility() throws IOException {
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.SEARCH_TYPE, "query_then_fetch");
        attributes.put(Attribute.TOTAL_SHARDS, 5);

        // Write with 3.4+ version
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("3.4.0"));
        out.writeMap(
            attributes,
            (stream, attribute) -> Attribute.writeTo(out, attribute),
            (stream, attributeValue) -> Attribute.writeValueTo(out, attributeValue)
        );

        // Read with pre-3.4 version
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.fromString("3.3.0"));
        Map<Attribute, Object> result = Attribute.readAttributeMap(in);

        assertEquals("Should have same number of attributes", attributes.size(), result.size());
        assertEquals("SEARCH_TYPE should match", attributes.get(Attribute.SEARCH_TYPE), result.get(Attribute.SEARCH_TYPE));
        assertEquals("TOTAL_SHARDS should match", attributes.get(Attribute.TOTAL_SHARDS), result.get(Attribute.TOTAL_SHARDS));
    }

    /**
     * Test that non-SOURCE attributes are not affected by version changes
     */
    public void testNonSourceAttributesUnaffected() throws IOException {
        // Test various attribute types across versions
        Version oldVersion = Version.fromString("3.3.0");
        Version newVersion = Version.fromString("3.4.0");

        Object[] testValues = {
            "query_then_fetch", // SEARCH_TYPE
            5, // TOTAL_SHARDS
            "node-1", // NODE_ID
            GroupingType.NONE // GROUP_BY
        };

        Attribute[] testAttributes = { Attribute.SEARCH_TYPE, Attribute.TOTAL_SHARDS, Attribute.NODE_ID, Attribute.GROUP_BY };

        for (int i = 0; i < testValues.length; i++) {
            Object value = testValues[i];
            Attribute attr = testAttributes[i];

            // Test old to new version
            BytesStreamOutput out1 = new BytesStreamOutput();
            out1.setVersion(oldVersion);
            Attribute.writeValueTo(out1, value);

            StreamInput in1 = out1.bytes().streamInput();
            in1.setVersion(newVersion);
            Object result1 = Attribute.readAttributeValue(in1, attr);
            assertEquals("Value should be unchanged old->new for " + attr, value, result1);

            // Test new to old version
            BytesStreamOutput out2 = new BytesStreamOutput();
            out2.setVersion(newVersion);
            Attribute.writeValueTo(out2, value);

            StreamInput in2 = out2.bytes().streamInput();
            in2.setVersion(oldVersion);
            Object result2 = Attribute.readAttributeValue(in2, attr);
            assertEquals("Value should be unchanged new->old for " + attr, value, result2);
        }
    }

    /**
     * Test edge case: empty SearchSourceBuilder serialization across versions
     */
    public void testEmptySearchSourceBuilderVersionCompatibility() throws IOException {
        SearchSourceBuilder emptyBuilder = new SearchSourceBuilder();

        // Write with pre-3.4 version
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("3.3.0"));
        Attribute.writeValueTo(out, emptyBuilder);

        // Read with 3.4+ version
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.fromString("3.4.0"));
        Object result = Attribute.readAttributeValue(in, Attribute.SOURCE);

        assertTrue("Should read as string for 3.4+ versions", result instanceof String);
        assertNotNull("Result should not be null", result);
    }

    /**
     * Test version boundary conditions around V_3_4_0
     */
    public void testVersionBoundaryConditions() throws IOException {
        String sourceString = "{\"query\":{\"match_all\":{}}}";

        // Test exactly at version boundary
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_3_4_0);
        Attribute.writeValueTo(out, sourceString);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_3_4_0);
        Object result = Attribute.readAttributeValue(in, Attribute.SOURCE);

        assertTrue("Should handle V_3_4_0 as string", result instanceof String);
        assertEquals("Content should match", sourceString, result);
    }

    /**
     * Test mixed attribute map with SOURCE and other attributes across versions
     */
    public void testMixedAttributeMapVersionCompatibility() throws IOException {
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.SOURCE, "{\"size\":10}");
        attributes.put(Attribute.SEARCH_TYPE, "query_then_fetch");
        attributes.put(Attribute.NODE_ID, "node-1");

        // Write with 3.4+ version
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("3.4.0"));
        out.writeMap(
            attributes,
            (stream, attribute) -> Attribute.writeTo(out, attribute),
            (stream, attributeValue) -> Attribute.writeValueTo(out, attributeValue)
        );

        // Read with pre-3.4 version
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.fromString("3.3.0"));
        Map<Attribute, Object> result = Attribute.readAttributeMap(in);

        assertEquals("Should have same number of attributes", attributes.size(), result.size());
        assertTrue("SOURCE should be readable", result.containsKey(Attribute.SOURCE));
        assertEquals("Non-SOURCE attributes should match", attributes.get(Attribute.SEARCH_TYPE), result.get(Attribute.SEARCH_TYPE));
    }

    /**
     * Comprehensive test covering the core version compatibility issue:
     * Ensures old code can parse new format and new code can parse old format
     */
    public void testCoreVersionCompatibilityScenario() throws IOException {
        // Scenario 1: Old version (SearchSourceBuilder) → New version (String)
        SearchSourceBuilder oldFormat = new SearchSourceBuilder().size(10);

        BytesStreamOutput oldOut = new BytesStreamOutput();
        oldOut.setVersion(Version.fromString("3.3.0"));
        Attribute.writeValueTo(oldOut, oldFormat);

        StreamInput newIn = oldOut.bytes().streamInput();
        newIn.setVersion(Version.fromString("3.4.0"));
        Object newResult = Attribute.readAttributeValue(newIn, Attribute.SOURCE);

        assertTrue("New code should parse old format as string", newResult instanceof String);

        // Scenario 2: New version (String) → Old version
        // When 3.4+ writes string and pre-3.4 reads it, it should get the string
        String newFormat = "{\"size\":5}";

        BytesStreamOutput newOut = new BytesStreamOutput();
        newOut.setVersion(Version.fromString("3.4.0"));
        Attribute.writeValueTo(newOut, newFormat);

        StreamInput oldIn = newOut.bytes().streamInput();
        oldIn.setVersion(Version.fromString("3.3.0"));
        Object oldResult = Attribute.readAttributeValue(oldIn, Attribute.SOURCE);

        assertTrue("Old code should parse new format as string", oldResult instanceof String);
        assertEquals("Content should be preserved", newFormat, oldResult);
    }
}
