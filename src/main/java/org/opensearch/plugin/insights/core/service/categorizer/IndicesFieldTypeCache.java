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
    /**
     * Count of cache evictions
     */
    private final CounterMetric evictionCount;
    /**
     * Count of items in cache
     */
    private final CounterMetric entryCount;
    /**
     * Weight of cache in bytes
     */
    private final CounterMetric weight;

    public IndicesFieldTypeCache(Settings settings) {
        final long sizeInBytes = QueryCategorizationSettings.SEARCH_QUERY_FIELD_TYPE_CACHE_SIZE_KEY.get(settings).getBytes();
        CacheBuilder<Index, IndexFieldMap> cacheBuilder = CacheBuilder.<Index, IndexFieldMap>builder();
        if (sizeInBytes > 0) {
            cacheBuilder.setMaximumWeight(sizeInBytes).weigher((k, v) -> RamUsageEstimator.sizeOfObject(k) + v.weight());
        }
        cache = cacheBuilder.build();
        evictionCount = new CounterMetric();
        entryCount = new CounterMetric();
        weight = new CounterMetric();
    }

    IndexFieldMap getOrInitialize(Index index) {
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
        IndexFieldMap indexFieldMap = cache.get(index);
        if (indexFieldMap != null) {
            evictionCount.inc(indexFieldMap.fieldTypeMap.size());
            entryCount.dec(indexFieldMap.fieldTypeMap.size());
            weight.dec(indexFieldMap.weight());
        }
        cache.invalidate(index);
    }

    public Iterable<Index> keySet() {
        return cache.keys();
    }

    public void incrementCountAndWeight(String key, String value) {
        entryCount.inc();
        weight.inc(RamUsageEstimator.sizeOf(key) + RamUsageEstimator.sizeOf(value));
    }

    /**
     * Get eviction count
     */
    public Long getEvictionCount() {
        return evictionCount.count();
    }

    /**
     * Get entry count
     */
    public Long getEntryCount() {
        return entryCount.count();
    }

    /**
     * Get cache weight in bytes
     */
    public Long getWeight() {
        return weight.count();
    }

    static class IndexFieldMap {
        private final ConcurrentHashMap<String, String> fieldTypeMap;

        /**
         * Estimated memory consumption of fieldTypeMap in bytes
         */
        private final CounterMetric weight;

        IndexFieldMap() {
            fieldTypeMap = new ConcurrentHashMap<>();
            weight = new CounterMetric();
        }

        public String get(String fieldName) {
            return fieldTypeMap.get(fieldName);
        }

        /**
         * Stores key, value if absent
         *
         * @return {@code true} if key was absent, else {@code false}
         */
        public boolean putIfAbsent(String key, String value) {
            // Increment the weight only if the key value pair added to the Map
            if (fieldTypeMap.putIfAbsent(key, value) == null) {
                weight.inc(RamUsageEstimator.sizeOf(key) + RamUsageEstimator.sizeOf(value));
                return true;
            }
            return false;
        }

        public long weight() {
            return weight.count();
        }
    }
}
