package org.opensearch.plugin.insights.core.service.categorizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class IndicesFieldTypeCache {

    private static final Logger logger = LogManager.getLogger(IndicesFieldTypeCache.class);
    public static final Setting<ByteSizeValue> INDICES_FIELD_TYPE_CACHE_SIZE_KEY = Setting.memorySizeSetting(
        "search.insights.indices.fieldtype.cache.size",
        new ByteSizeValue(-1),
        Setting.Property.NodeScope
    );
    private final Cache<Index, IndexFieldMap> cache;

    public IndicesFieldTypeCache(Settings settings) {
        final long sizeInBytes = -1; //TODO: INDICES_FIELD_TYPE_CACHE_SIZE_KEY.get(settings).getBytes();
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
            // Increment the weight only if the key value got added to the Map
            if (fieldTypeMap.putIfAbsent(key, value) == null) {
                weight.inc(RamUsageEstimator.sizeOf(key) + RamUsageEstimator.sizeOf(value));
            }
        }
        public long weight() {
            return weight.count();
        }
    }
}

