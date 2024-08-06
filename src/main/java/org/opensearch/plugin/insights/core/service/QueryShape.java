/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.List;
import java.util.Objects;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;

public class QueryShape {
    QueryBuilder query;
    AggregatorFactories.Builder aggregations;
    List<SortBuilder<?>> sorts;

    public QueryShape(SearchSourceBuilder source) {
        query = source.query();
        aggregations = source.aggregations();
        sorts = source.sorts();
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, aggregations, sorts);
    }
}
