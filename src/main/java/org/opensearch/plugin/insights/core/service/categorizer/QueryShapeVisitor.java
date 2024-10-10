/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.ONE_SPACE_INDENT;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.search.BooleanClause;
import org.opensearch.common.SetOnce;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;

/**
 * Class to traverse the QueryBuilder tree and capture the query shape
 */
public final class QueryShapeVisitor implements QueryBuilderVisitor {
    private final SetOnce<String> queryType = new SetOnce<>();
    private final SetOnce<String> fieldData = new SetOnce<>();
    private final Map<BooleanClause.Occur, List<QueryShapeVisitor>> childVisitors = new EnumMap<>(BooleanClause.Occur.class);
    private final QueryShapeGenerator queryShapeGenerator;
    private final Set<Index> successfulSearchShardIndices;
    private final Boolean showFieldName;
    private final Boolean showFieldType;

    @Override
    public void accept(QueryBuilder queryBuilder) {
        queryType.set(queryBuilder.getName());
        fieldData.set(queryShapeGenerator.buildFieldDataString(queryBuilder, successfulSearchShardIndices, showFieldName, showFieldType));
    }

    @Override
    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        // Should get called once per Occur value
        if (childVisitors.containsKey(occur)) {
            throw new IllegalStateException("child visitor already called for " + occur);
        }
        final List<QueryShapeVisitor> childVisitorList = new ArrayList<>();
        QueryBuilderVisitor childVisitorWrapper = new QueryBuilderVisitor() {
            QueryShapeVisitor currentChild;

            @Override
            public void accept(QueryBuilder qb) {
                currentChild = new QueryShapeVisitor(queryShapeGenerator, successfulSearchShardIndices, showFieldName, showFieldType);
                childVisitorList.add(currentChild);
                currentChild.accept(qb);
            }

            @Override
            public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
                return currentChild.getChildVisitor(occur);
            }
        };
        childVisitors.put(occur, childVisitorList);
        return childVisitorWrapper;
    }

    /**
     * Convert query builder tree to json
     * @return json query builder tree as a string
     */
    public String toJson() {
        StringBuilder outputBuilder = new StringBuilder("{\"type\":\"").append(queryType.get()).append("\"");
        for (Map.Entry<BooleanClause.Occur, List<QueryShapeVisitor>> entry : childVisitors.entrySet()) {
            outputBuilder.append(",\"").append(entry.getKey().name().toLowerCase(Locale.ROOT)).append("\"[");
            boolean first = true;
            for (QueryShapeVisitor child : entry.getValue()) {
                if (!first) {
                    outputBuilder.append(",");
                }
                outputBuilder.append(child.toJson());
                first = false;
            }
            outputBuilder.append("]");
        }
        outputBuilder.append("}");
        return outputBuilder.toString();
    }

    /**
     * Pretty print the query builder tree
     *
     * @param indent        indent size
     * @param showFieldName    whether to print field name
     * @param showFieldType
     * @return Query builder tree as a pretty string
     */
    public String prettyPrintTree(String indent, Boolean showFieldName, Boolean showFieldType) {
        StringBuilder outputBuilder = new StringBuilder(indent).append(queryType.get());
        if (showFieldName || showFieldType) {
            outputBuilder.append(fieldData.get());
        }
        outputBuilder.append("\n");
        for (Map.Entry<BooleanClause.Occur, List<QueryShapeVisitor>> entry : childVisitors.entrySet()) {
            outputBuilder.append(indent)
                .append(ONE_SPACE_INDENT.repeat(2))
                .append(entry.getKey().name().toLowerCase(Locale.ROOT))
                .append(":\n");
            for (QueryShapeVisitor child : entry.getValue()) {
                outputBuilder.append(child.prettyPrintTree(indent + ONE_SPACE_INDENT.repeat(4), showFieldName, showFieldType));
            }
        }
        return outputBuilder.toString();
    }

    /**
     * Default constructor
     */
    public QueryShapeVisitor(
        QueryShapeGenerator queryShapeGenerator,
        Set<Index> successfulSearchShardIndices,
        Boolean showFieldName,
        Boolean showFieldType
    ) {
        this.queryShapeGenerator = queryShapeGenerator;
        this.successfulSearchShardIndices = successfulSearchShardIndices;
        this.showFieldName = showFieldName;
        this.showFieldType = showFieldType;
    }
}
