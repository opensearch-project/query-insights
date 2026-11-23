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
import java.util.List;
import java.util.Map;
import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

/** The SQL/PPL attributes must not be sent to nodes that don't know them. */
public class QuerySourceBwcTests extends OpenSearchTestCase {

    private static final ChildSearches CHILD_SEARCHES = new ChildSearches(
        1,
        List.of(new ChildSearches.Search(1L, List.of("logs"), 1, 5L, 10L, 20L, false, null))
    );

    private SearchQueryRecord recordWithQuerySourceAttributes() {
        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(5L));
        measurements.put(MetricType.CPU, new Measurement(10L));
        measurements.put(MetricType.MEMORY, new Measurement(20L));

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.QUERY_SOURCE, "PPL");
        attributes.put(Attribute.CHILD_SEARCHES, CHILD_SEARCHES);
        attributes.put(Attribute.NODE_ID, "node-1");

        return new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, "id-1");
    }

    public void testNewAttributesStrippedForOldNode() throws IOException {
        SearchQueryRecord record = recordWithQuerySourceAttributes();
        // 3.10.0 is the last release without these attributes.
        SearchQueryRecord out = roundTrip(record, Version.V_3_10_0);

        // NODE_ID is kept; the new attributes are dropped.
        assertEquals("node-1", out.getAttributes().get(Attribute.NODE_ID));
        assertNull(out.getAttributes().get(Attribute.QUERY_SOURCE));
        assertNull(out.getAttributes().get(Attribute.CHILD_SEARCHES));
    }

    public void testNewAttributesPreservedForCurrentNode() throws IOException {
        SearchQueryRecord record = recordWithQuerySourceAttributes();

        SearchQueryRecord out = roundTrip(record, Version.V_3_11_0);

        assertEquals("PPL", out.getAttributes().get(Attribute.QUERY_SOURCE));
        assertEquals(CHILD_SEARCHES, out.getAttributes().get(Attribute.CHILD_SEARCHES));
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
