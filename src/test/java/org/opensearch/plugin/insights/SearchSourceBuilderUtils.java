/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.bucket.terms.SignificantTextAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;

public class SearchSourceBuilderUtils {

    public static SearchSourceBuilder createDefaultSearchSourceBuilder() {
        /*
         * {
         *   "size": 0,
         *   "query": {
         *     "bool": {
         *       "must": [
         *         {
         *           "term": {
         *             "field1": "value2"
         *           }
         *         }
         *       ],
         *       "filter": [
         *         {
         *           "match": {
         *             "field2": "php"
         *           }
         *         },
         *         {
         *           "range": {
         *             "field4": {}
         *           }
         *         }
         *       ],
         *       "should": [
         *         {
         *           "regexp": {
         *             "field3": "text"
         *           }
         *         }
         *       ]
         *     }
         *   },
         *   "aggs": {
         *     "agg1": {
         *       "terms": {
         *         "field": "type"
         *       },
         *       "meta": {
         *         "user_value_type_hint": "string"
         *       },
         *       "aggs": {
         *         "pipeline-agg1": {
         *           "derivative": {
         *             "buckets_path": "bucket1"
         *           }
         *         },
         *         "child-agg3": {
         *           "terms": {
         *             "field": "key.sub3"
         *           },
         *           "meta": {
         *             "user_value_type_hint": "string"
         *           }
         *         }
         *       }
         *     },
         *     "agg2": {
         *       "terms": {
         *         "field": "model"
         *       },
         *       "meta": {
         *         "user_value_type_hint": "string"
         *       }
         *     },
         *     "agg3": {
         *       "terms": {
         *         "field": "key"
         *       },
         *       "meta": {
         *         "user_value_type_hint": "string"
         *       },
         *       "aggs": {
         *         "pipeline-agg2": {
         *           "max_bucket": {
         *             "buckets_path": "bucket2"
         *           }
         *         },
         *         "child-agg1": {
         *           "terms": {
         *             "field": "key.sub1"
         *           },
         *           "meta": {
         *             "user_value_type_hint": "string"
         *           }
         *         },
         *         "child-agg2": {
         *           "terms": {
         *             "field": "key.sub2"
         *           },
         *           "meta": {
         *             "user_value_type_hint": "string"
         *           }
         *         }
         *       }
         *     },
         *     "top_hits": {
         *       "top_hits": {
         *         "stored_fields": ["_none_"]
         *       }
         *     },
         *     "sig_text": {
         *       "significant_text": {
         *         "field": "agg4",
         *         "filter_duplicate_text": true
         *       }
         *     },
         *     "pipeline-agg4": {
         *       "max_bucket": {
         *         "buckets_path": "bucket4"
         *       }
         *     },
         *     "pipeline-agg3": {
         *       "derivative": {
         *         "buckets_path": "bucket3"
         *       }
         *     },
         *     "pipeline-agg5": {
         *       "avg_bucket": {
         *         "buckets_path": "bucket5"
         *       }
         *     }
         *   },
         *   "sort": [
         *     {
         *       "color": {
         *         "order": "desc"
         *       }
         *     },
         *     {
         *       "vendor": {
         *         "order": "desc"
         *       }
         *     },
         *     {
         *       "price": {
         *         "order": "asc"
         *       }
         *     },
         *     {
         *       "album": {
         *         "order": "asc"
         *       }
         *     }
         *   ]
         * }
         */
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        // build query
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("field1", "value2");
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("field2", "php");
        RegexpQueryBuilder regexpQueryBuilder = new RegexpQueryBuilder("field3", "text");
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("field4");
        sourceBuilder.query(
            new BoolQueryBuilder().must(termQueryBuilder).filter(matchQueryBuilder).should(regexpQueryBuilder).filter(rangeQueryBuilder)
        );
        // build aggregation
        sourceBuilder.aggregation(
            new TermsAggregationBuilder("agg1").userValueTypeHint(ValueType.STRING)
                .field("type")
                .subAggregation(new DerivativePipelineAggregationBuilder("pipeline-agg1", "bucket1"))
                .subAggregation(new TermsAggregationBuilder("child-agg3").userValueTypeHint(ValueType.STRING).field("key.sub3"))
        );
        sourceBuilder.aggregation(new TermsAggregationBuilder("agg2").userValueTypeHint(ValueType.STRING).field("model"));
        sourceBuilder.aggregation(
            new TermsAggregationBuilder("agg3").userValueTypeHint(ValueType.STRING)
                .field("key")
                .subAggregation(new MaxBucketPipelineAggregationBuilder("pipeline-agg2", "bucket2"))
                .subAggregation(new TermsAggregationBuilder("child-agg1").userValueTypeHint(ValueType.STRING).field("key.sub1"))
                .subAggregation(new TermsAggregationBuilder("child-agg2").userValueTypeHint(ValueType.STRING).field("key.sub2"))
        );
        sourceBuilder.aggregation(new TopHitsAggregationBuilder("top_hits").storedField("_none_"));
        sourceBuilder.aggregation(new SignificantTextAggregationBuilder("sig_text", "agg4").filterDuplicateText(true));
        sourceBuilder.aggregation(new MaxBucketPipelineAggregationBuilder("pipeline-agg4", "bucket4"));
        sourceBuilder.aggregation(new DerivativePipelineAggregationBuilder("pipeline-agg3", "bucket3"));
        sourceBuilder.aggregation(new AvgBucketPipelineAggregationBuilder("pipeline-agg5", "bucket5"));
        // build sort
        sourceBuilder.sort("color", SortOrder.DESC);
        sourceBuilder.sort("vendor", SortOrder.DESC);
        sourceBuilder.sort("price", SortOrder.ASC);
        sourceBuilder.sort("album", SortOrder.ASC);

        return sourceBuilder;
    }

    public static SearchSourceBuilder createQuerySearchSourceBuilder() {
        /*
         * {
         *   "size": 0,
         *   "query": {
         *     "bool": {
         *       "must": [
         *         {
         *           "term": {
         *             "field1": "html"
         *           }
         *         }
         *       ],
         *       "filter": [
         *         {
         *           "match": {
         *             "field2": "php"
         *           }
         *         },
         *         {
         *           "range": {
         *             "field4": {
         *               "from": "2023-01-01T00:00:00Z",
         *               "to": "2023-12-31T23:59:59Z",
         *               "format": "strict_date_optional_time"
         *             }
         *           }
         *         }
         *       ],
         *       "should": [
         *         {
         *           "regexp": {
         *             "field3": "text"
         *           }
         *         }
         *       ]
         *     }
         *   }
         * }
         */
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("field1", "html");
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("field2", "php");
        RegexpQueryBuilder regexpQueryBuilder = new RegexpQueryBuilder("field3", "text");
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("field4").from("2023-01-01T00:00:00Z")
            .to("2023-12-31T23:59:59Z")
            .format("strict_date_optional_time");
        sourceBuilder.query(
            new BoolQueryBuilder().must(termQueryBuilder).filter(matchQueryBuilder).should(regexpQueryBuilder).filter(rangeQueryBuilder)
        );
        return sourceBuilder;
    }

    public static SearchSourceBuilder createAggregationSearchSourceBuilder() {
        /*
         * {
         *   "aggs": {
         *     "agg1": {
         *       "terms": {
         *         "field": "type"
         *       },
         *       "meta": {
         *         "user_value_type_hint": "string"
         *       },
         *       "aggs": {
         *         "pipeline-agg1": {
         *           "derivative": {
         *             "buckets_path": "bucket1"
         *           }
         *         },
         *         "child-agg3": {
         *           "terms": {
         *             "field": "key.sub3"
         *           },
         *           "meta": {
         *             "user_value_type_hint": "string"
         *           }
         *         }
         *       }
         *     },
         *     "agg2": {
         *       "terms": {
         *         "field": "model"
         *       },
         *       "meta": {
         *         "user_value_type_hint": "string"
         *       }
         *     },
         *     "agg3": {
         *       "terms": {
         *         "field": "key"
         *       },
         *       "meta": {
         *         "user_value_type_hint": "string"
         *       },
         *       "aggs": {
         *         "pipeline-agg2": {
         *           "max_bucket": {
         *             "buckets_path": "bucket2"
         *           }
         *         },
         *         "child-agg1": {
         *           "terms": {
         *             "field": "key.sub1"
         *           },
         *           "meta": {
         *             "user_value_type_hint": "string"
         *           }
         *         },
         *         "child-agg2": {
         *           "terms": {
         *             "field": "key.sub2"
         *           },
         *           "meta": {
         *             "user_value_type_hint": "string"
         *           }
         *         }
         *       }
         *     },
         *     "top_hits": {
         *       "top_hits": {
         *         "stored_fields": ["_none_"]
         *       }
         *     },
         *     "sig_text": {
         *       "significant_text": {
         *         "field": "agg4",
         *         "filter_duplicate_text": true
         *       }
         *     },
         *     "pipeline-agg4": {
         *       "max_bucket": {
         *         "buckets_path": "bucket4"
         *       }
         *     },
         *     "pipeline-agg3": {
         *       "derivative": {
         *         "buckets_path": "bucket3"
         *       }
         *     },
         *     "pipeline-agg5": {
         *       "avg_bucket": {
         *         "buckets_path": "bucket5"
         *       }
         *     }
         *   }
         * }
         */
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(
            new TermsAggregationBuilder("agg1").userValueTypeHint(ValueType.STRING)
                .field("type")
                .subAggregation(new DerivativePipelineAggregationBuilder("pipeline-agg1", "bucket1"))
                .subAggregation(new TermsAggregationBuilder("child-agg3").userValueTypeHint(ValueType.STRING).field("key.sub3"))
        );
        sourceBuilder.aggregation(new TermsAggregationBuilder("agg2").userValueTypeHint(ValueType.STRING).field("model"));
        sourceBuilder.aggregation(
            new TermsAggregationBuilder("agg3").userValueTypeHint(ValueType.STRING)
                .field("key")
                .subAggregation(new MaxBucketPipelineAggregationBuilder("pipeline-agg2", "bucket2"))
                .subAggregation(new TermsAggregationBuilder("child-agg1").userValueTypeHint(ValueType.STRING).field("key.sub1"))
                .subAggregation(new TermsAggregationBuilder("child-agg2").userValueTypeHint(ValueType.STRING).field("key.sub2"))
        );
        sourceBuilder.aggregation(new TopHitsAggregationBuilder("top_hits").storedField("_none_"));
        sourceBuilder.aggregation(new SignificantTextAggregationBuilder("sig_text", "agg4").filterDuplicateText(true));
        sourceBuilder.aggregation(new MaxBucketPipelineAggregationBuilder("pipeline-agg4", "bucket4"));
        sourceBuilder.aggregation(new DerivativePipelineAggregationBuilder("pipeline-agg3", "bucket3"));
        sourceBuilder.aggregation(new AvgBucketPipelineAggregationBuilder("pipeline-agg5", "bucket5"));
        return sourceBuilder;
    }

    public static SearchSourceBuilder createSortSearchSourceBuilder() {
        /*
         * {
         *   "sort": [
         *     {
         *       "color": {
         *         "order": "desc"
         *       }
         *     },
         *     {
         *       "vendor": {
         *         "order": "desc"
         *       }
         *     },
         *     {
         *       "price": {
         *         "order": "asc"
         *       }
         *     },
         *     {
         *       "album": {
         *         "order": "asc"
         *       }
         *     }
         *   ]
         * }
         */
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.sort("color", SortOrder.DESC);
        sourceBuilder.sort("vendor", SortOrder.DESC);
        sourceBuilder.sort("price", SortOrder.ASC);
        sourceBuilder.sort("album", SortOrder.ASC);
        return sourceBuilder;
    }
}
