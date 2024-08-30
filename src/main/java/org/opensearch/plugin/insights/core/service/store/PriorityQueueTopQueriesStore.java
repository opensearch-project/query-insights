/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.service.store;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Implementation of {@link TopQueriesStore} that uses a {@link PriorityBlockingQueue}
 * to manage Top N search query records or groups. This implementation is thread-safe.
 *
 * @param <T> The type of records that this store will manage.
 */
public class PriorityQueueTopQueriesStore<T> implements TopQueriesStore<T> {

    private final PriorityBlockingQueue<T> topQueriesStore;

    /**
     * Constructs a new {@code PriorityQueueTopQueriesStore} with the specified capacity.
     *
     * @param capacity the initial capacity of the store
     * @param comparator comparator to use for the priority queue store
     */
    public PriorityQueueTopQueriesStore(int capacity, Comparator<T> comparator) {
        this.topQueriesStore = new PriorityBlockingQueue<>(capacity, comparator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(T queryRecord) {
        return topQueriesStore.add(queryRecord);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T poll() {
        return topQueriesStore.poll();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T peek() {
        return topQueriesStore.peek();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return topQueriesStore.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return topQueriesStore.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addAll(List<T> records) {
        return topQueriesStore.addAll(records);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        topQueriesStore.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<T> getSnapshot() {
        return new ArrayList<>(topQueriesStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(T queryRecord) {
        return topQueriesStore.remove(queryRecord);
    }
}
