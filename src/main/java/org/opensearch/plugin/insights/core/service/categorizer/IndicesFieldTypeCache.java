/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.plugin.insights.settings.QueryCategorizationSettings;

/**
 * Cache implementation specifically for maintaining the field name type mappings
 * for indices that are part of successful search requests
 */
public class IndicesFieldTypeCache {

    private static final Logger logger = LogManager.getLogger(IndicesFieldTypeCache.class);
    private final Cache<Index, IndexFieldMap> cache;

    public IndicesFieldTypeCache(Settings settings) {
        final long sizeInBytes = QueryCategorizationSettings.SEARCH_QUERY_FIELD_TYPE_CACHE_SIZE_KEY.get(settings).getBytes();
        CacheBuilder<Index, IndexFieldMap> cacheBuilder = CacheBuilder.<Index, IndexFieldMap>builder();
        if (sizeInBytes > 0) {
            cacheBuilder.setMaximumWeight(sizeInBytes).weigher((k, v) -> RamUsageEstimator.sizeOfObject(k) + v.weight());
        }
        cache = cacheBuilder.build();
    }

    public IndexFieldMap getOrInitialize(Index index) {
        try {
            return cache.computeIfAbsent(index, k -> new IndexFieldMap());
        } catch (ExecutionException ex) {
            logger.error("Unexpected execution exception while initializing for index " + index);
        }

        // Should never return null as the ExecutionException is only thrown
        // if loader throws an exception or returns a null value, which cannot
        // be the case in this scenario
        return null;
    }

    public void invalidate(Index index) {
        cache.invalidate(index);
    }

    public Iterable<Index> keySet() {
        return cache.keys();
    }

    static class IndexFieldMap {
        private ConcurrentHashMap<String, String> fieldTypeMap;
        private CounterMetric weight;

        IndexFieldMap() {
            fieldTypeMap = new ConcurrentHashMap<>();
            weight = new CounterMetric();
        }

        public String get(String fieldName) {
            return fieldTypeMap.get(fieldName);
        }

        public void putIfAbsent(String key, String value) {
            // Increment the weight only if the key value pair added to the Map
            if (fieldTypeMap.putIfAbsent(key, value) == null) {
                weight.inc(RamUsageEstimator.sizeOf(key) + RamUsageEstimator.sizeOf(value));
            }
        }

        public long weight() {
            return weight.count();
        }
    }
}
