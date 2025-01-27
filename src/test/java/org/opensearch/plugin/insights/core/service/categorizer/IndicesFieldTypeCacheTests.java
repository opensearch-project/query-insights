/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import java.util.UUID;
import java.util.stream.StreamSupport;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

public final class IndicesFieldTypeCacheTests extends OpenSearchTestCase {
    final Index index1 = new Index("index1", UUID.randomUUID().toString());
    final Index index2 = new Index("index2", UUID.randomUUID().toString());

    public IndicesFieldTypeCacheTests() {}

    public void testCache() {
        final IndicesFieldTypeCache indicesFieldTypeCache = new IndicesFieldTypeCache(Settings.EMPTY);

        // Assert cache is empty
        assertEquals(0, StreamSupport.stream(indicesFieldTypeCache.keySet().spliterator(), false).count());
        assertEquals(0, (long) indicesFieldTypeCache.getEntryCount());
        assertEquals(0, (long) indicesFieldTypeCache.getEvictionCount());
        assertEquals(0, (long) indicesFieldTypeCache.getWeight());

        // Test invalidate on empty cache
        indicesFieldTypeCache.invalidate(index1);
        indicesFieldTypeCache.invalidate(index2);

        // Insert some items in the cache
        if (indicesFieldTypeCache.getOrInitialize(index1).putIfAbsent("field1", "keyword")) {
            indicesFieldTypeCache.incrementCountAndWeight("field1", "keyword");
        }
        if (indicesFieldTypeCache.getOrInitialize(index1).putIfAbsent("field2", "boolean")) {
            indicesFieldTypeCache.incrementCountAndWeight("field2", "boolean");
        }
        if (indicesFieldTypeCache.getOrInitialize(index2).putIfAbsent("field3", "float_range")) {
            indicesFieldTypeCache.incrementCountAndWeight("field3", "float_range");
        }
        if (indicesFieldTypeCache.getOrInitialize(index2).putIfAbsent("field4", "date")) {
            indicesFieldTypeCache.incrementCountAndWeight("field4", "date");
        }

        assertEquals(2, StreamSupport.stream(indicesFieldTypeCache.keySet().spliterator(), false).count());
        assertEquals(4, (long) indicesFieldTypeCache.getEntryCount());
        assertEquals(0, (long) indicesFieldTypeCache.getEvictionCount());
        assertTrue(0 < indicesFieldTypeCache.getWeight());

        // Invalidate index1
        indicesFieldTypeCache.invalidate(index1);

        assertEquals(1, StreamSupport.stream(indicesFieldTypeCache.keySet().spliterator(), false).count());
        assertEquals(2, (long) indicesFieldTypeCache.getEntryCount());
        assertEquals(2, (long) indicesFieldTypeCache.getEvictionCount());
        assertTrue(0 < indicesFieldTypeCache.getWeight());
    }

}
