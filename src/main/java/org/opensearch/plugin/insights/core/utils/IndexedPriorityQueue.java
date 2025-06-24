/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IndexedPriorityQueue<K, V> {

    private static final Logger logger = LogManager.getLogger(IndexedPriorityQueue.class);

    public static class Entry<K, V> {
        public final K key;
        public V value;

        Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    private final List<Entry<K, V>> heap;
    private final Map<K, Integer> indexMap;
    private final Comparator<? super V> comparator;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock read = lock.readLock();
    private final Lock write = lock.writeLock();

    public IndexedPriorityQueue(int initialCapacity, Comparator<? super V> comparator) {
        this.comparator = comparator;
        this.heap = new ArrayList<>(initialCapacity);
        this.indexMap = new HashMap<>(initialCapacity);
    }

    public boolean insert(K key, V value) {
        write.lock();
        try {
            if (indexMap.containsKey(key)) {
                logger.debug("Key {} already exists in queue, skipping insert", key);
                return false;
            }
            heap.add(new Entry<>(key, value));
            int idx = heap.size() - 1;
            indexMap.put(key, idx);
            siftUp(idx);
            logger.debug("Successfully inserted key {} at index {}", key, idx);
            return true;
        } finally {
            write.unlock();
        }
    }

    public boolean remove(K key) {
        write.lock();
        try {
            Integer idx = indexMap.remove(key);
            if (idx == null) {
                logger.debug("Key {} not found in queue, nothing to remove", key);
                return false;
            }

            int lastIdx = heap.size() - 1;
            if (idx != lastIdx) {
                Entry<K, V> lastItem = heap.get(lastIdx);
                heap.set(idx, lastItem);
                indexMap.put(lastItem.key, idx);

                boolean needSiftUp = false;
                boolean needSiftDown = false;

                if (idx > 0) {
                    int parentIdx = (idx - 1) >>> 1;
                    if (comparator.compare(lastItem.value, heap.get(parentIdx).value) < 0) {
                        needSiftUp = true;
                    }
                }

                if (!needSiftUp && idx < heap.size() >>> 1) {
                    int left = (idx << 1) + 1;
                    if (comparator.compare(lastItem.value, heap.get(left).value) > 0) {
                        needSiftDown = true;
                    } else if (left + 1 < heap.size() && comparator.compare(lastItem.value, heap.get(left + 1).value) > 0) {
                        needSiftDown = true;
                    }
                }

                if (needSiftUp) {
                    siftUp(idx);
                } else if (needSiftDown) {
                    siftDown(idx);
                }
            }
            heap.remove(lastIdx);
            logger.debug("Successfully removed key {} from index {}", key, idx);
            return true;
        } finally {
            write.unlock();
        }
    }

    public V poll() {
        Entry<K, V> e = pollEntry();
        return e == null ? null : e.value;
    }

    public Entry<K, V> pollEntry() {
        write.lock();
        try {
            if (heap.isEmpty()) {
                return null;
            }
            Entry<K, V> head = heap.get(0);
            remove(head.key);
            return head;
        } finally {
            write.unlock();
        }
    }

    public Entry<K, V> peek() {
        read.lock();
        try {
            return heap.isEmpty() ? null : heap.get(0);
        } finally {
            read.unlock();
        }
    }

    public int size() {
        read.lock();
        try {
            return heap.size();
        } finally {
            read.unlock();
        }
    }

    public List<V> getAllValues() {
        read.lock();
        try {
            List<V> values = new ArrayList<>();
            for (Entry<K, V> entry : heap) {
                values.add(entry.value);
            }
            return values;
        } finally {
            read.unlock();
        }
    }

    public void clear() {
        write.lock();
        try {
            heap.clear();
            indexMap.clear();
        } finally {
            write.unlock();
        }
    }

    private void siftUp(int idx) {
        Entry<K, V> item = heap.get(idx);
        while (idx > 0) {
            int parentIdx = (idx - 1) >>> 1;
            Entry<K, V> parent = heap.get(parentIdx);
            if (comparator.compare(item.value, parent.value) >= 0) break;
            heap.set(idx, parent);
            indexMap.put(parent.key, idx);
            idx = parentIdx;
        }
        heap.set(idx, item);
        indexMap.put(item.key, idx);
    }

    private void siftDown(int idx) {
        int half = heap.size() >>> 1;
        Entry<K, V> item = heap.get(idx);
        while (idx < half) {
            int left = (idx << 1) + 1;
            int right = left + 1;
            int smallest = left;

            if (right < heap.size() && comparator.compare(heap.get(right).value, heap.get(left).value) < 0) {
                smallest = right;
            }

            Entry<K, V> smallestItem = heap.get(smallest);
            if (comparator.compare(item.value, smallestItem.value) <= 0) break;

            heap.set(idx, smallestItem);
            indexMap.put(smallestItem.key, idx);
            idx = smallest;
        }
        heap.set(idx, item);
        indexMap.put(item.key, idx);
    }

    public boolean isEmpty() {
        read.lock();
        try {
            return heap.isEmpty();
        } finally {
            read.unlock();
        }
    }
}
