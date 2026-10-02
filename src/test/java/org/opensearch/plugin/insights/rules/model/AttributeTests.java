/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
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
     * Test that SOURCE attribute works correctly within same version (3.5+)
     */
    public void testSourceAttributeSameNewVersion() throws IOException {
        String sourceString = "{\"size\":5,\"query\":{\"term\":{\"field\":\"value\"}}}";
        SourceString sourceStringObj = new SourceString(sourceString);

        // Both write and read with 3.5+ version
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("3.5.0"));
        Attribute.writeValueTo(out, sourceStringObj);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.fromString("3.5.0"));
        Object result = Attribute.readAttributeValue(in, Attribute.SOURCE);

        assertTrue("Should remain as SourceString", result instanceof SourceString);
        assertEquals("String content should match", sourceString, ((SourceString) result).getValue());
    }

    /**
     * Test version boundary conditions around V_3_5_0
     */
    public void testVersionBoundaryConditions() throws IOException {
        String sourceString = "{\"query\":{\"match_all\":{}}}";
        SourceString sourceStringObj = new SourceString(sourceString);

        // Test exactly at version boundary
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_3_5_0);
        Attribute.writeValueTo(out, sourceStringObj);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_3_5_0);
        Object result = Attribute.readAttributeValue(in, Attribute.SOURCE);

        assertTrue("Should handle V_3_5_0 as SourceString", result instanceof SourceString);
        assertEquals("Content should match", sourceString, ((SourceString) result).getValue());
    }

    /**
     * Test realistic scenario: 3.5+ node reading index data written by 3.3 node
     * This simulates reading old index data after a node upgrade
     */
    public void testReadingOldIndexData() throws IOException {
        // Use a simple SearchSourceBuilder that doesn't require NamedWriteableRegistry
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(100);

        // Simulate data written to index by 3.3 node
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("3.3.0"));
        Attribute.writeValueTo(out, sourceBuilder);

        // Simulate 3.5+ node reading from that index
        // The stream version reflects the original writer (3.3), not the current reader
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.fromString("3.3.0")); // This is key - version reflects the data format
        Object result = Attribute.readAttributeValue(in, Attribute.SOURCE);

        // Result should be a SourceString (converted from SearchSourceBuilder)
        assertTrue("Should read as SourceString", result instanceof SourceString);
        assertNotNull("Result should not be null", result);

        // Verify the string contains the original query information
        String resultString = ((SourceString) result).getValue();
        assertTrue("Should contain \"size\":100", resultString.contains("100"));
    }

    /**
     * Test 3.5+ node writing to old node (< 3.5)
     * SourceString should be converted to SearchSourceBuilder object
     */
    public void testNewNodeWriteToOldNode() throws IOException {
        // Use a SearchSourceBuilder directly to ensure size is preserved
        SearchSourceBuilder originalBuilder = new SearchSourceBuilder().size(10);
        String sourceString = originalBuilder.toString();
        SourceString sourceStringObj = new SourceString(sourceString);

        // 3.5+ node writing to 3.3 node
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("3.3.0"));
        Attribute.writeValueTo(out, sourceStringObj);

        // Old node reading - should get SearchSourceBuilder object
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.fromString("3.3.0"));
        Object result = Attribute.readAttributeValue(in, Attribute.SOURCE);

        assertTrue("Should convert to SourceString", result instanceof SourceString);
        String resultString = ((SourceString) result).getValue();
        assertNotNull("Result string should not be null", resultString);
        assertTrue("Should contain \"size\":10", resultString.contains("\"size\":10"));
    }

    /**
     * Test 3.5+ node writing to old node with invalid JSON
     * Should fallback to dummy SearchSourceBuilder
     */
    public void testNewNodeWriteToOldNodeInvalidJson() throws IOException {
        String invalidJson = "invalid json string";
        SourceString sourceStringObj = new SourceString(invalidJson);

        // 3.5+ node writing to 3.3 node with invalid JSON
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("3.3.0"));
        Attribute.writeValueTo(out, sourceStringObj);

        // Should not throw exception, should write dummy object
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.fromString("3.3.0"));
        Object result = Attribute.readAttributeValue(in, Attribute.SOURCE);

        assertTrue("Should handle invalid JSON gracefully", result instanceof SourceString);
        assertNotNull("Should not be null", result);
    }

    /**
     * Test for https://github.com/opensearch-project/query-insights/issues/510
     *
     * Simulates a rolling upgrade: a newer node writes an attribute map containing a key this
     * node's Attribute enum doesn't have yet (as would happen if a newer node sent USER_ROLES /
     * BACKEND_ROLES / USERNAME to a coordinator whose build predates those attributes). Reading
     * that map must not throw, and every attribute this node *does* recognize -- both before and
     * after the unknown one in the map -- must still come through with the correct value,
     * proving the stream stays byte-aligned across the skipped entry.
     */
    public void testReadAttributeMapSkipsUnknownAttributeFromNewerNode() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();

        // Manually write a 3-entry attribute map the way Attribute.writeTo/writeValueTo would,
        // with an unrecognized key in the middle -- standing in for a newer-node-only attribute
        // (e.g. "user_roles") that this test's Attribute enum build doesn't define.
        out.writeVInt(3);

        out.writeString("node_id");
        Attribute.writeValueTo(out, "node-abc");

        out.writeString("totally_unknown_future_attribute");
        // USER_ROLES/BACKEND_ROLES are written as String[] (see QueryInsightsListener), not
        // List -- Attribute#writeValueTo special-cases java.util.List through out.writeList(),
        // which is Writeable-based, not the self-describing generic codec this fix relies on.
        Attribute.writeValueTo(out, new String[] { "admin", "readall" });

        out.writeString("total_shards");
        Attribute.writeValueTo(out, 5);

        StreamInput in = out.bytes().streamInput();
        Map<Attribute, Object> result = Attribute.readAttributeMap(in);

        assertEquals("Unknown attribute should be skipped, not counted", 2, result.size());
        assertEquals("node-abc", result.get(Attribute.NODE_ID));
        assertEquals(5, result.get(Attribute.TOTAL_SHARDS));
        assertFalse(result.containsKey(null));
    }

    /**
     * Same scenario as above but with the unknown attribute as the very last entry, to make sure
     * there's no off-by-one in how the loop continues after a skip.
     */
    public void testReadAttributeMapSkipsUnknownAttributeAtEnd() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();

        out.writeVInt(2);
        out.writeString("node_id");
        Attribute.writeValueTo(out, "node-xyz");
        out.writeString("another_unknown_future_attribute");
        Attribute.writeValueTo(out, "some-value");

        StreamInput in = out.bytes().streamInput();
        Map<Attribute, Object> result = Attribute.readAttributeMap(in);

        assertEquals(1, result.size());
        assertEquals("node-xyz", result.get(Attribute.NODE_ID));
    }

    /**
     * A map made up entirely of known attributes must round-trip exactly as before -- this fix
     * must not change behavior for the common (no version skew) case.
     */
    public void testReadAttributeMapAllKnownAttributesUnaffected() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();

        out.writeVInt(2);
        out.writeString("node_id");
        Attribute.writeValueTo(out, "node-1");
        out.writeString("total_shards");
        Attribute.writeValueTo(out, 3);

        StreamInput in = out.bytes().streamInput();
        Map<Attribute, Object> result = Attribute.readAttributeMap(in);

        assertEquals(2, result.size());
        assertEquals("node-1", result.get(Attribute.NODE_ID));
        assertEquals(3, result.get(Attribute.TOTAL_SHARDS));
    }

}
