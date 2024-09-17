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
 * Unit tests for the {@link QueryGrouperHealthStats} class.
 */
public class QueryGrouperHealthStatsTests extends OpenSearchTestCase {
    private final int queryGroupCount = 10;
    private final int queryGroupHeapSize = 5;

    public void testConstructorAndGetters() {
        QueryGrouperHealthStats stats = new QueryGrouperHealthStats(queryGroupCount, queryGroupHeapSize);
        assertEquals(queryGroupCount, stats.getQueryGroupCount());
        assertEquals(queryGroupHeapSize, stats.getQueryGroupHeapSize());
    }

    public void testSerialization() throws IOException {
        QueryGrouperHealthStats stats = new QueryGrouperHealthStats(queryGroupCount, queryGroupHeapSize);
        // Write to StreamOutput
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        // Read from StreamInput
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        QueryGrouperHealthStats deserializedStats = new QueryGrouperHealthStats(in);
        // Assert equality
        assertEquals(stats.getQueryGroupCount(), deserializedStats.getQueryGroupCount());
        assertEquals(stats.getQueryGroupHeapSize(), deserializedStats.getQueryGroupHeapSize());
    }

    public void testToXContent() throws IOException {
        QueryGrouperHealthStats stats = new QueryGrouperHealthStats(queryGroupCount, queryGroupHeapSize);
        // Write to XContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String expectedJson = String.format(
            Locale.ROOT,
            "{\"QueryGroupCount\":%d,\"QueryGroupHeapSize\":%d}",
            queryGroupCount,
            queryGroupHeapSize
        );
        assertEquals(expectedJson, builder.toString());
    }
}
