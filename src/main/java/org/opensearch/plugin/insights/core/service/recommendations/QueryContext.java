/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.recommendations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanClause;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Context object that provides query information and metadata for recommendation rule evaluation.
 *
 */
public class QueryContext {
    private static final Logger log = LogManager.getLogger(QueryContext.class);
    public static final String KEYWORD_FIELD_TYPE = "keyword";
    public static final String KEYWORD_SUBFIELD_SUFFIX = "." + KEYWORD_FIELD_TYPE;

    private final SearchQueryRecord record;
    private final ClusterService clusterService;
    private final QueryShapeGenerator queryShapeGenerator;

    /**
     * Constructor for QueryContext
     * @param record the search query record (must have a non-null SearchSourceBuilder)
     * @param clusterService the cluster service for metadata access
     * @param queryShapeGenerator the query shape generator for cached field type lookups
     */
    public QueryContext(SearchQueryRecord record, ClusterService clusterService, QueryShapeGenerator queryShapeGenerator) {
        this.record = Objects.requireNonNull(record, "record must not be null");
        this.clusterService = clusterService;
        this.queryShapeGenerator = queryShapeGenerator;
    }

    /**
     * Get the search query record
     * @return the record
     */
    public SearchQueryRecord getRecord() {
        return record;
    }

    /**
     * Get the search source builder from the underlying record
     * @return the search source builder, or null if not available
     */
    public SearchSourceBuilder getSearchSourceBuilder() {
        return record.getSearchSourceBuilder();
    }

    /**
     * Get the cluster service
     * @return the cluster service
     */
    public ClusterService getClusterService() {
        return clusterService;
    }

    /**
     * Get the indices being queried
     * @return the list of indices, or empty list if not available
     */
    @SuppressWarnings("unchecked")
    public List<String> getIndices() {
        Object indicesObj = record.getAttributes().get(Attribute.INDICES);

        // Handle List type - deserialized records
        if (indicesObj instanceof List) {
            return (List<String>) indicesObj;
        }
        // In-memory records
        if (indicesObj instanceof String[]) {
            List<String> indices = Arrays.asList((String[]) indicesObj);
            return indices;
        }
        return new ArrayList<>();
    }

    /**
     * Extract all query builders of a given type from the query tree.
     * Uses OpenSearch's {@link QueryBuilderVisitor} to recursively walk all compound query types
     * (bool, constant_score, dis_max, nested, boosting, function_score, etc.).
     *
     * @param queryType the class of query builder to extract (e.g. TermQueryBuilder.class)
     * @param <T> the query builder type
     * @return list of matching query builders
     */
    public <T extends QueryBuilder> List<T> extractQueries(Class<T> queryType) {
        List<T> results = new ArrayList<>();
        SearchSourceBuilder ssb = getSearchSourceBuilder();
        if (ssb != null && ssb.query() != null) {
            ssb.query().visit(new QueryCollectorVisitor<>(queryType, results));
        }
        return results;
    }

    /**
     * Get the field type for a given field from the index mapping
     * @param fieldName the field name
     * @return the field type, or null if not found
     */
    @SuppressWarnings("unchecked")
    public String getFieldType(String fieldName) {
        if (clusterService == null || queryShapeGenerator == null) {
            return null;
        }

        List<String> indices = getIndices();
        if (indices.isEmpty()) {
            return null;
        }

        try {
            String indexName = indices.get(0);
            IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
            if (indexMetadata == null) {
                return null;
            }
            MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata == null) {
                return null;
            }
            Map<String, Object> sourceAsMap = mappingMetadata.sourceAsMap();
            Object propertiesObj = sourceAsMap.get("properties");
            if (!(propertiesObj instanceof Map)) {
                return null;
            }
            return queryShapeGenerator.getFieldType(fieldName, (Map<String, Object>) propertiesObj, indexMetadata.getIndex());
        } catch (Exception e) {
            log.warn("Error getting field type for field: {}", fieldName, e);
        }

        return null;
    }

    /**
     * A {@link QueryBuilderVisitor} that collects all query nodes matching a given type.
     * Delegates child traversal back to itself so it walks the entire tree.
     */
    private static class QueryCollectorVisitor<T extends QueryBuilder> implements QueryBuilderVisitor {
        private final Class<T> queryType;
        private final List<T> results;

        QueryCollectorVisitor(Class<T> queryType, List<T> results) {
            this.queryType = queryType;
            this.results = results;
        }

        @Override
        public void accept(QueryBuilder qb) {
            if (queryType.isInstance(qb)) {
                results.add(queryType.cast(qb));
            }
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
            return this;
        }
    }
}
