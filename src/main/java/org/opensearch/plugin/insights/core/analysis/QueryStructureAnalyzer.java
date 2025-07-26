/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.analysis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Analyzes the structure of search queries to extract features for ML models.
 */
public class QueryStructureAnalyzer {
    private static final Logger log = LogManager.getLogger(QueryStructureAnalyzer.class);

    /**
     * Extracts structural features from a search request.
     *
     * @param searchRequest The search request to analyze
     * @return Map of feature names to values
     */
    public static Map<String, Object> extractQueryFeatures(SearchRequest searchRequest) {
        Map<String, Object> features = new HashMap<>();
        
        try {
            SearchSourceBuilder source = searchRequest.source();
            if (source == null) {
                return features;
            }
            
            // Get query as string for simple analysis
            String queryString = source.toString();
            
            // Extract basic query structure features
            features.put("query_depth", estimateQueryDepth(queryString));
            features.put("boolean_clause_count", countOccurrences(queryString, "bool"));
            features.put("has_wildcards", queryString.contains("wildcard") || 
                                         (queryString.contains("query_string") && 
                                          (queryString.contains("*") || queryString.contains("?"))));
            features.put("has_fuzzy_matching", queryString.contains("fuzzy") || 
                                              queryString.contains("fuzziness"));
            features.put("has_function_score", queryString.contains("function_score"));
            features.put("has_script_fields", queryString.contains("script_fields") || 
                                             queryString.contains("script"));
            
            // Extract query types
            Set<String> queryTypes = extractQueryTypes(queryString);
            features.put("query_type_count", queryTypes.size());
            
            // Count specific clause types
            features.put("match_clause_count", countOccurrences(queryString, "match"));
            features.put("term_clause_count", countOccurrences(queryString, "term"));
            features.put("range_clause_count", countOccurrences(queryString, "range"));
            features.put("exists_clause_count", countOccurrences(queryString, "exists"));
            features.put("prefix_clause_count", countOccurrences(queryString, "prefix"));
            features.put("wildcard_clause_count", countOccurrences(queryString, "wildcard"));
            features.put("regexp_clause_count", countOccurrences(queryString, "regexp"));
            features.put("fuzzy_clause_count", countOccurrences(queryString, "fuzzy"));
            
            // Extract boolean query details
            features.put("must_clause_count", countOccurrences(queryString, "must"));
            features.put("should_clause_count", countOccurrences(queryString, "should"));
            features.put("must_not_clause_count", countOccurrences(queryString, "must_not"));
            features.put("filter_clause_count", countOccurrences(queryString, "filter"));
            
            // Extract aggregation features if present
            if (source.aggregations() != null && !source.aggregations().getAggregatorFactories().isEmpty()) {
                int aggCount = source.aggregations().getAggregatorFactories().size();
                features.put("aggregation_count", aggCount);
                features.put("aggregation_complexity", estimateAggregationComplexity(source.toString()));
            } else {
                features.put("aggregation_count", 0);
                features.put("aggregation_complexity", 0);
            }
            
            // Extract sort features if present
            if (source.sorts() != null) {
                int sortCount = source.sorts().size();
                features.put("sort_field_count", sortCount);
                features.put("sort_complexity", sortCount);
            } else {
                features.put("sort_field_count", 0);
                features.put("sort_complexity", 0);
            }
            
            // Extract pagination features
            features.put("from", source.from());
            features.put("size", source.size());
            Integer trackTotalHits = source.trackTotalHitsUpTo();
            features.put("track_total_hits", trackTotalHits != null && trackTotalHits > 0);
            
            // Calculate overall query complexity score
            int queryComplexity = calculateQueryComplexityScore(features);
            features.put("query_complexity_score", queryComplexity);
            
            // Add query size in bytes
            features.put("query_size_bytes", queryString.length());
            
            // Add field complexity estimation
            int fieldComplexity = estimateFieldComplexity(queryString);
            features.put("field_complexity_score", fieldComplexity);
            
            // Extract highlighting features
            features.put("has_highlighting", source.highlighter() != null);
            
            // Extract suggest features
            features.put("has_suggest", source.suggest() != null);
            
        } catch (Exception e) {
            log.error("Error extracting query features", e);
        }
        
        return features;
    }
    
    /**
     * Estimates the query depth based on the number of nested braces.
     */
    private static int estimateQueryDepth(String queryString) {
        int maxDepth = 0;
        int currentDepth = 0;
        
        for (char c : queryString.toCharArray()) {
            if (c == '{') {
                currentDepth++;
                maxDepth = Math.max(maxDepth, currentDepth);
            } else if (c == '}') {
                currentDepth = Math.max(0, currentDepth - 1);
            }
        }
        
        return maxDepth;
    }
    
    /**
     * Counts occurrences of a substring in a string.
     */
    private static int countOccurrences(String text, String substring) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(substring, index)) != -1) {
            count++;
            index += substring.length();
        }
        return count;
    }
    
    /**
     * Extracts query types from the query string.
     */
    private static Set<String> extractQueryTypes(String queryString) {
        Set<String> queryTypes = new HashSet<>();
        
        // Common query types to check for
        String[] types = {
            "match", "match_all", "match_phrase", "term", "terms", "range", 
            "exists", "prefix", "wildcard", "regexp", "fuzzy", "bool", 
            "dis_max", "function_score", "geo_distance", "geo_bounding_box", 
            "geo_polygon", "more_like_this", "script", "nested", "has_child", 
            "has_parent", "parent_id", "query_string", "simple_query_string"
        };
        
        for (String type : types) {
            if (queryString.contains("\"" + type + "\"") || 
                queryString.contains("'" + type + "'") ||
                queryString.contains("{" + type + ":")) {
                queryTypes.add(type);
            }
        }
        
        return queryTypes;
    }
    
    /**
     * Estimates aggregation complexity based on the aggregation string.
     */
    private static int estimateAggregationComplexity(String aggString) {
        // Simple estimation based on string length and nesting
        int complexity = aggString.length() / 100; // Length factor
        complexity += countOccurrences(aggString, "aggs"); // Nesting factor
        complexity += countOccurrences(aggString, "aggregations"); // Nesting factor
        
        // Add points for complex aggregation types
        if (aggString.contains("terms")) complexity += 1;
        if (aggString.contains("histogram")) complexity += 2;
        if (aggString.contains("date_histogram")) complexity += 2;
        if (aggString.contains("range")) complexity += 2;
        if (aggString.contains("filters")) complexity += 3;
        if (aggString.contains("significant_terms")) complexity += 4;
        if (aggString.contains("percentiles")) complexity += 3;
        if (aggString.contains("cardinality")) complexity += 2;
        if (aggString.contains("geo")) complexity += 3;
        if (aggString.contains("script")) complexity += 5;
        
        return complexity;
    }
    
    /**
     * Calculates an overall complexity score for the query based on various features.
     */
    private static int calculateQueryComplexityScore(Map<String, Object> features) {
        int score = 0;
        
        // Add points for query depth
        score += ((Number) features.getOrDefault("query_depth", 0)).intValue() * 5;
        
        // Add points for boolean clauses
        score += ((Number) features.getOrDefault("boolean_clause_count", 0)).intValue() * 2;
        
        // Add points for aggregation complexity
        score += ((Number) features.getOrDefault("aggregation_complexity", 0)).intValue() * 3;
        
        // Add points for sort complexity
        score += ((Number) features.getOrDefault("sort_complexity", 0)).intValue() * 2;
        
        // Add points for wildcards (can be expensive)
        if ((Boolean) features.getOrDefault("has_wildcards", false)) {
            score += 5;
        }
        
        // Add points for fuzzy matching (can be expensive)
        if ((Boolean) features.getOrDefault("has_fuzzy_matching", false)) {
            score += 5;
        }
        
        // Add points for function score (can be expensive)
        if ((Boolean) features.getOrDefault("has_function_score", false)) {
            score += 8;
        }
        
        // Add points for regexp clauses (can be very expensive)
        score += ((Number) features.getOrDefault("regexp_clause_count", 0)).intValue() * 10;
        
        return score;
    }
    
    /**
     * Estimates field complexity based on field types in the query.
     */
    private static int estimateFieldComplexity(String queryString) {
        int complexity = 0;
        
        // Text fields are more expensive than keyword fields
        if (queryString.contains(".text")) complexity += 3;
        if (queryString.contains(".keyword")) complexity += 1;
        
        // Nested fields are more expensive
        if (queryString.contains("nested")) complexity += 5;
        
        // Geo fields are expensive
        if (queryString.contains("geo_point") || queryString.contains("geo_shape")) complexity += 4;
        
        // Check for common expensive fields
        if (queryString.contains("name")) complexity += 2; // Often text field
        if (queryString.contains("description")) complexity += 3; // Often long text
        if (queryString.contains("location")) complexity += 3; // Often geo
        
        return complexity;
    }
}