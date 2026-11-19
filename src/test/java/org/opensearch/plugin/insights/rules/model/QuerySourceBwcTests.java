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
import org.opensearch.test.OpenSearchTestCase;

/** The SQL/PPL attributes must not be sent to nodes that don't know them. */
public class QuerySourceBwcTests extends OpenSearchTestCase {

    private SearchQueryRecord recordWithQuerySourceAttributes() {
        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(5L));
        measurements.put(MetricType.CPU, new Measurement(10L));
        measurements.put(MetricType.MEMORY, new Measurement(20L));

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.QUERY_SOURCE, "PPL");
        attributes.put(Attribute.DERIVED_FROM, "PPL:node-1:42");
        attributes.put(Attribute.PARENT_MARKER, "PPL:node-1:42");
        attributes.put(Attribute.IS_CHILD, true);
        attributes.put(Attribute.NODE_ID, "node-1");

        return new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, "id-1");
    }

    public void testNewAttributesStrippedForOldNode() throws IOException {
        SearchQueryRecord record = recordWithQuerySourceAttributes();
        // 3.9.0 is the last release without these attributes.
        SearchQueryRecord out = roundTrip(record, Version.V_3_9_0);

        // NODE_ID is kept; the new attributes are dropped.
        assertEquals("node-1", out.getAttributes().get(Attribute.NODE_ID));
        assertNull(out.getAttributes().get(Attribute.QUERY_SOURCE));
        assertNull(out.getAttributes().get(Attribute.DERIVED_FROM));
        assertNull(out.getAttributes().get(Attribute.PARENT_MARKER));
        assertNull(out.getAttributes().get(Attribute.IS_CHILD));
    }

    public void testNewAttributesPreservedForCurrentNode() throws IOException {
        SearchQueryRecord record = recordWithQuerySourceAttributes();

        SearchQueryRecord out = roundTrip(record, Version.V_3_10_0);

        assertEquals("PPL", out.getAttributes().get(Attribute.QUERY_SOURCE));
        assertEquals("PPL:node-1:42", out.getAttributes().get(Attribute.DERIVED_FROM));
        assertEquals("PPL:node-1:42", out.getAttributes().get(Attribute.PARENT_MARKER));
        assertEquals(true, out.getAttributes().get(Attribute.IS_CHILD));
    }

    private static SearchQueryRecord roundTrip(SearchQueryRecord record, Version version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            record.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                return new SearchQueryRecord(in);
            }
        }
    }
}
