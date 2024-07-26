/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import java.util.Map;
import org.apache.lucene.search.BooleanClause;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.plugin.insights.rules.model.MetricType;

/**
 * Class to visit the query builder tree and also track the level information.
 * Increments the counters related to Search Query type.
 */
final class SearchQueryCategorizingVisitor implements QueryBuilderVisitor {
    private final int level;
    private final SearchQueryCounters searchQueryCounters;
    private final Map<MetricType, Number> measurements;

    public SearchQueryCategorizingVisitor(SearchQueryCounters searchQueryCounters, Map<MetricType, Number> measurements) {
        this(searchQueryCounters, 0, measurements);
    }

    private SearchQueryCategorizingVisitor(SearchQueryCounters counters, int level, Map<MetricType, Number> measurements) {
        this.searchQueryCounters = counters;
        this.level = level;
        this.measurements = measurements;
    }

    public void accept(QueryBuilder qb) {
        searchQueryCounters.incrementCounter(qb, level, measurements);
    }

    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return new SearchQueryCategorizingVisitor(searchQueryCounters, level + 1, measurements);
    }
}
