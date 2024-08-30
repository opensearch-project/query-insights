/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.service.store;

import java.util.List;

/**
 * Interface for managing a store of top search query records.
 * Implementations of this interface should provide thread-safe operations
 * to manage and access the top search queries.
 *
 * @param <T> The type of records that this store will manage.
 */
public interface TopQueriesStore<T> {

    /**
     * Adds a search query record to the store.
     *
     * @param queryRecord the search query record to be added
     * @return {@code true} if the record was successfully added, otherwise {@code false}
     */
    boolean add(T queryRecord);

    /**
     * Retrieves and removes the top search query record from the store,
     * or returns {@code null} if the store is empty.
     *
     * @return the top search query record, or {@code null} if the store is empty
     */
    T poll();

    /**
     * Retrieves, but does not remove, the top search query record from the store,
     * or returns {@code null} if the store is empty.
     *
     * @return the top search query record, or {@code null} if the store is empty
     */
    T peek();

    /**
     * Returns the number of search query records currently in the store.
     *
     * @return the size of the store
     */
    int size();

    /**
     * Returns {@code true} if the store is empty, otherwise {@code false}.
     *
     * @return {@code true} if the store is empty, otherwise {@code false}
     */
    boolean isEmpty();

    /**
     * Adds a list of query records to the store.
     *
     * @param records the list of query records to add
     * @return {@code true} if all records were added successfully, otherwise {@code false}
     */
    boolean addAll(List<T> records);

    /**
     * Clears all the records from the store.
     */
    void clear();

    /**
     * Returns a snapshot of the current elements in the store as a list.
     *
     * @return a list containing all the elements currently in the store
     */
    List<T> getSnapshot();

    /**
     * Removes the specified record from the store, if it exists.
     *
     * @param queryRecord the record to remove
     * @return {@code true} if the record was present and removed, otherwise {@code false}
     */
    boolean remove(T queryRecord);
}
