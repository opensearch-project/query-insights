/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * Class to categorize the search queries based on the type and increment the relevant counters.
 * Class also logs the query shape.
 */
public final class SearchQueryCategorizer {

    private static final Logger logger = LogManager.getLogger(SearchQueryCategorizer.class);

    /**
     * Contains all the search query counters
     */
    private final SearchQueryCounters searchQueryCounters;

    final SearchQueryAggregationCategorizer searchQueryAggregationCategorizer;
    private static SearchQueryCategorizer instance;
    private static org.opensearch.core.xcontent.NamedXContentRegistry namedXContentRegistry;

    /**
     * Constructor for SearchQueryCategorizor
     * @param metricsRegistry opentelemetry metrics registry
     * @param xContentRegistry NamedXContentRegistry for parsing
     */
    private SearchQueryCategorizer(MetricsRegistry metricsRegistry, org.opensearch.core.xcontent.NamedXContentRegistry xContentRegistry) {
        searchQueryCounters = new SearchQueryCounters(metricsRegistry);
        searchQueryAggregationCategorizer = new SearchQueryAggregationCategorizer(searchQueryCounters);
        namedXContentRegistry = xContentRegistry;
    }

    /**
     * Get singleton instance of SearchQueryCategorizer
     * @param metricsRegistry metric registry
     * @return singleton instance
     */
    public static SearchQueryCategorizer getInstance(MetricsRegistry metricsRegistry) {
        return getInstance(metricsRegistry, org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY);
    }

    /**
     * Get singleton instance of SearchQueryCategorizer
     * @param metricsRegistry metric registry
     * @param xContentRegistry NamedXContentRegistry for parsing
     * @return singleton instance
     */
    public static SearchQueryCategorizer getInstance(
        MetricsRegistry metricsRegistry,
        org.opensearch.core.xcontent.NamedXContentRegistry xContentRegistry
    ) {
        if (instance == null) {
            synchronized (SearchQueryCategorizer.class) {
                if (instance == null) {
                    instance = new SearchQueryCategorizer(metricsRegistry, xContentRegistry);
                }
            }
        }
        return instance;
    }

    /**
     * Consume records and increment categorization counters and histograms for the records including latency, cpu and memory.
     * @param records records to consume
     */
    public void consumeRecords(List<SearchQueryRecord> records) {
        for (SearchQueryRecord record : records) {
            categorize(record);
        }
    }

    /**
     * Increment categorizations counters for the given search query record and
     * also increment latency, cpu and memory related histograms.
     * @param record search query record
     */
    public void categorize(SearchQueryRecord record) {
        Object sourceObj = record.getAttributes().get(Attribute.SOURCE);
        if (sourceObj == null) {
            return;
        }

        SearchSourceBuilder source;
        if (sourceObj instanceof String) {
            // Parse string back to SearchSourceBuilder for categorization
            String sourceString = (String) sourceObj;

            // Check if source was previously truncated - skip parsing if so
            Boolean isTruncated = (Boolean) record.getAttributes().get(Attribute.SOURCE_TRUNCATED);
            if (Boolean.TRUE.equals(isTruncated)) {
                logger.debug("Skipping categorization for truncated source string");
                return;
            }

            try {
                source = SearchSourceBuilder.fromXContent(
                    org.opensearch.common.xcontent.json.JsonXContent.jsonXContent.createParser(
                        namedXContentRegistry,
                        org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        sourceString
                    )
                );
            } catch (Exception e) {
                logger.warn("Failed to parse SOURCE string for categorization: {} - Source: {}", e.getMessage(), sourceString);
                return;
            }
        } else {
            source = (SearchSourceBuilder) sourceObj;
        }

        Map<MetricType, Measurement> measurements = record.getMeasurements();

        incrementQueryTypeCounters(source.query(), measurements);
        incrementQueryAggregationCounters(source.aggregations(), measurements);
        incrementQuerySortCounters(source.sorts(), measurements);
    }

    private void incrementQuerySortCounters(List<SortBuilder<?>> sorts, Map<MetricType, Measurement> measurements) {
        if (sorts != null && sorts.size() > 0) {
            for (SortBuilder<?> sortBuilder : sorts) {
                String sortOrder = sortBuilder.order().toString();
                searchQueryCounters.incrementSortCounter(1, Tags.create().addTag("sort_order", sortOrder), measurements);
            }
        }
    }

    private void incrementQueryAggregationCounters(AggregatorFactories.Builder aggregations, Map<MetricType, Measurement> measurements) {
        if (aggregations == null) {
            return;
        }

        searchQueryAggregationCategorizer.incrementSearchQueryAggregationCounters(aggregations.getAggregatorFactories(), measurements);
    }

    private void incrementQueryTypeCounters(QueryBuilder topLevelQueryBuilder, Map<MetricType, Measurement> measurements) {
        if (topLevelQueryBuilder == null) {
            return;
        }
        QueryBuilderVisitor searchQueryVisitor = new SearchQueryCategorizingVisitor(searchQueryCounters, measurements);
        topLevelQueryBuilder.visit(searchQueryVisitor);
    }

    /**
     * Get search query counters
     * @return search query counters
     */
    public SearchQueryCounters getSearchQueryCounters() {
        return this.searchQueryCounters;
    }

    /**
     * Reset the search query categorizer and its counters
     */
    public void reset() {
        synchronized (SearchQueryCategorizer.class) {
            instance = null;
            namedXContentRegistry = null;
        }
    }
}
