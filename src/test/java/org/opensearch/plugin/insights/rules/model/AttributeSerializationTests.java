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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

/** Tests the current writer against the attribute names and codecs understood by released readers. */
public class AttributeSerializationTests extends OpenSearchTestCase {
    // These snapshots come from the release tags, independently of Attribute's version metadata.
    private static final Set<String> V_2_16_ATTRIBUTES = Set.of(
        "search_type",
        "source",
        "total_shards",
        "indices",
        "phase_latency_map",
        "node_id",
        "task_resource_usages",
        "labels"
    );
    private static final Set<String> V_2_19_ATTRIBUTES = with(V_2_16_ATTRIBUTES, "query_group_hashcode", "group_by");
    private static final Set<String> V_3_0_ATTRIBUTES = with(V_2_19_ATTRIBUTES, "description");
    private static final Set<String> V_3_1_ATTRIBUTES = with(V_3_0_ATTRIBUTES, "top_n_query", "is_cancelled");
    private static final Set<String> V_3_3_ATTRIBUTES = with(V_3_1_ATTRIBUTES, "wlm_group_id");
    private static final Set<String> V_3_5_ATTRIBUTES = with(V_3_3_ATTRIBUTES, "username", "user_roles", "source_truncated");
    private static final Set<String> V_3_6_ATTRIBUTES = with(V_3_5_ATTRIBUTES, "backend_roles", "failed");

    public void testSerializationForReleasedReaders() throws IOException {
        assertSerializedAttributes(Version.V_2_19_0, V_2_19_ATTRIBUTES);
        assertSerializedAttributes(Version.V_2_19_2, V_2_19_ATTRIBUTES);
        assertSerializedAttributes(Version.V_2_19_3, with(V_2_19_ATTRIBUTES, "top_n_query"));
        assertSerializedAttributes(Version.V_2_19_6, with(V_2_19_ATTRIBUTES, "top_n_query"));
        // The 2.19.3 backport must not make TOP_N_QUERY appear supported by 3.0.
        assertSerializedAttributes(Version.V_3_0_0, V_3_0_ATTRIBUTES);
        assertSerializedAttributes(Version.V_3_1_0, V_3_1_ATTRIBUTES);
        assertSerializedAttributes(Version.V_3_2_0, V_3_1_ATTRIBUTES);
        assertSerializedAttributes(Version.V_3_3_0, V_3_3_ATTRIBUTES);
        assertSerializedAttributes(Version.V_3_4_0, V_3_3_ATTRIBUTES);
        assertSerializedAttributes(Version.V_3_5_0, V_3_5_ATTRIBUTES);
        assertSerializedAttributes(Version.V_3_6_0, V_3_6_ATTRIBUTES);
        assertSerializedAttributes(Version.V_3_8_0, V_3_6_ATTRIBUTES);
        assertSerializedAttributes(Version.CURRENT, V_3_6_ATTRIBUTES);
    }

    public void testAttributeMapForPre219Readers() throws IOException {
        // Pre-2.19 records have a different envelope. Check the attribute map independently.
        // 2.17/2.18 used query_hashcode, not the current query_group_hashcode name.
        Map<Attribute, Object> attributes = allAttributes();
        for (Version version : List.of(Version.V_2_16_0, Version.V_2_17_0, Version.V_2_18_0)) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                Attribute.writeAttributeMap(out, attributes);
                out.writeString("next-field");
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(version);
                    assertLegacyAttributeMap(in, V_2_16_ATTRIBUTES, attributes);
                    assertEquals("next-field", in.readString());
                    assertEquals(0, in.available());
                }
            }
        }
    }

    private void assertSerializedAttributes(Version version, Set<String> expectedNames) throws IOException {
        Map<Attribute, Object> attributes = allAttributes();
        Map<Attribute, Object> originalAttributes = new HashMap<>(attributes);
        SearchQueryRecord record = new SearchQueryRecord(123L, Map.of(MetricType.LATENCY, new Measurement(7L)), attributes, "query-id");

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            // Consecutive records and a trailing field detect incorrect map sizes or codec alignment.
            record.writeTo(out);
            record.writeTo(out);
            out.writeString("next-field");
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                assertLegacyRecord(in, expectedNames, attributes);
                assertLegacyRecord(in, expectedNames, attributes);
                assertEquals("next-field", in.readString());
                assertEquals(0, in.available());
            }
        }

        assertEquals("Writing to " + version + " must not modify the record", originalAttributes, attributes);
        // The same record can subsequently be sent to a current node with every attribute intact.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            record.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                SearchQueryRecord copy = new SearchQueryRecord(in);
                assertEquals(attributes.keySet(), copy.getAttributes().keySet());
                assertEquals(0, in.available());
            }
        }
    }

    private void assertLegacyRecord(StreamInput in, Set<String> expectedNames, Map<Attribute, Object> attributes) throws IOException {
        assertEquals(123L, in.readLong());
        assertEquals("query-id", in.readString());
        assertEquals(1, in.readVInt());
        assertEquals("latency", in.readString());
        assertEquals(7L, Measurement.readFromStream(in).getMeasurement());
        assertLegacyAttributeMap(in, expectedNames, attributes);
    }

    private void assertLegacyAttributeMap(StreamInput in, Set<String> expectedNames, Map<Attribute, Object> attributes) throws IOException {
        int size = in.readVInt();
        assertEquals("Unexpected map size for " + in.getVersion(), expectedNames.size(), size);
        Set<String> actualNames = new HashSet<>();
        for (int i = 0; i < size; i++) {
            String name = in.readString();
            // Do not call the current readAttributeMap: a patched reader would mask the original bug.
            assertTrue("Old reader " + in.getVersion() + " cannot recognize " + name, expectedNames.contains(name));
            assertTrue("Duplicate attribute " + name, actualNames.add(name));
            Object expected = attributes.get(Attribute.valueOf(name.toUpperCase(Locale.ROOT)));
            switch (name) {
                case "source":
                    if (in.getVersion().before(Version.V_3_5_0)) {
                        assertEquals(10, new SearchSourceBuilder(in).size());
                    } else {
                        assertEquals(((SourceString) expected).getValue(), in.readString());
                    }
                    break;
                case "task_resource_usages":
                    assertEquals(expected, in.readList(TaskResourceInfo::readFromStream));
                    break;
                case "group_by":
                    assertEquals(((GroupingType) expected).getValue(), in.readString());
                    break;
                default:
                    Object actual = in.readGenericValue();
                    if (expected instanceof String[]) {
                        assertArrayEquals((String[]) expected, (Object[]) actual);
                    } else {
                        assertEquals(expected, actual);
                    }
            }
        }
        assertEquals(expectedNames, actualNames);
    }

    private Map<Attribute, Object> allAttributes() {
        Map<Attribute, Object> attributes = new LinkedHashMap<>();
        // Interleave supported and unsupported entries to exercise filtering throughout the map.
        attributes.put(Attribute.USER_ROLES, new String[] { "admin", "user" });
        attributes.put(Attribute.NODE_ID, "node-1");
        attributes.put(Attribute.SOURCE_TRUNCATED, false);
        attributes.put(Attribute.SOURCE, new SourceString(new SearchSourceBuilder().size(10).toString()));
        attributes.put(Attribute.TOTAL_SHARDS, 3);
        attributes.put(Attribute.FAILED, true);
        attributes.put(
            Attribute.TASK_RESOURCE_USAGES,
            List.of(new TaskResourceInfo("search", 2L, 1L, "node-1", new TaskResourceUsage(1L, 2L)))
        );
        attributes.put(Attribute.GROUP_BY, GroupingType.SIMILARITY);
        attributes.put(Attribute.BACKEND_ROLES, new String[] { "team" });
        attributes.put(Attribute.SEARCH_TYPE, "query_then_fetch");
        attributes.put(Attribute.INDICES, new String[] { "index" });
        attributes.put(Attribute.PHASE_LATENCY_MAP, Map.of("query", 7L));
        attributes.put(Attribute.LABELS, null);
        attributes.put(Attribute.QUERY_GROUP_HASHCODE, "hash");
        attributes.put(Attribute.DESCRIPTION, "search description");
        attributes.put(Attribute.TOP_N_QUERY, Map.of("latency", true));
        attributes.put(Attribute.WLM_GROUP_ID, "group");
        attributes.put(Attribute.IS_CANCELLED, false);
        attributes.put(Attribute.USERNAME, "alice");
        assertEquals("Add a wire fixture when adding an attribute", Set.of(Attribute.values()), attributes.keySet());
        return attributes;
    }

    private static Set<String> with(Set<String> names, String... additions) {
        Set<String> result = new HashSet<>(names);
        result.addAll(List.of(additions));
        return Set.copyOf(result);
    }
}
