/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.healthStats;

import java.io.IOException;
import java.util.Locale;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the {@link TopQueriesHealthStats} class.
 */
public class TopQueriesHealthStatsTests extends OpenSearchTestCase {
    private final int topQueriesHeapSize = 15;
    private final QueryGrouperHealthStats queryGrouperHealthStats = new QueryGrouperHealthStats(10, 5);

    public void testConstructorAndGetters() {
        TopQueriesHealthStats healthStats = new TopQueriesHealthStats(topQueriesHeapSize, queryGrouperHealthStats);
        assertEquals(topQueriesHeapSize, healthStats.getTopQueriesHeapSize());
        assertEquals(queryGrouperHealthStats, healthStats.getQueryGrouperHealthStats());
    }

    public void testSerialization() throws IOException {
        TopQueriesHealthStats healthStats = new TopQueriesHealthStats(topQueriesHeapSize, queryGrouperHealthStats);
        // Write to StreamOutput
        BytesStreamOutput out = new BytesStreamOutput();
        healthStats.writeTo(out);
        // Read from StreamInput
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        TopQueriesHealthStats deserializedHealthStats = new TopQueriesHealthStats(in);
        assertEquals(healthStats.getTopQueriesHeapSize(), deserializedHealthStats.getTopQueriesHeapSize());
        assertNotNull(deserializedHealthStats.getQueryGrouperHealthStats());
        assertEquals(
            healthStats.getQueryGrouperHealthStats().getQueryGroupCount(),
            deserializedHealthStats.getQueryGrouperHealthStats().getQueryGroupCount()
        );
        assertEquals(
            healthStats.getQueryGrouperHealthStats().getQueryGroupHeapSize(),
            deserializedHealthStats.getQueryGrouperHealthStats().getQueryGroupHeapSize()
        );
    }

    public void testToXContent() throws IOException {
        TopQueriesHealthStats healthStats = new TopQueriesHealthStats(topQueriesHeapSize, queryGrouperHealthStats);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        healthStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String expectedJson = String.format(
            Locale.ROOT,
            "{\"TopQueriesHeapSize\":%d,\"QueryGroupCount_Total\":%d,\"QueryGroupCount_MaxHeap\":%d}",
            topQueriesHeapSize,
            queryGrouperHealthStats.getQueryGroupCount(),
            queryGrouperHealthStats.getQueryGroupHeapSize()
        );
        assertEquals(expectedJson, builder.toString());
    }
}
