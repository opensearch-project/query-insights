/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.plugin.insights.QueryInsightsTestUtils.generateQueryInsightRecords;
import static org.opensearch.plugin.insights.core.service.categorizer.SearchQueryAggregationCategorizer.AGGREGATION_TYPE_TAG;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.plugin.insights.core.service.categorizer.SearchQueryCategorizer;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;

public final class SearchQueryCategorizerTests extends OpenSearchTestCase {

    private static final String MULTI_TERMS_AGGREGATION = "multi_terms";

    private MetricsRegistry metricsRegistry;

    private SearchQueryCategorizer searchQueryCategorizer;

    private Map<String, Histogram> histogramMap = new HashMap<>();

    @Before
    public void setup() {
        SearchQueryCategorizer.getInstance(mock(MetricsRegistry.class)).reset();
        metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );

        when(metricsRegistry.createHistogram(any(String.class), any(String.class), any(String.class))).thenAnswer(new Answer<Histogram>() {
            @Override
            public Histogram answer(InvocationOnMock invocation) throws Throwable {
                // Extract arguments to identify which histogram is being created
                String name = invocation.getArgument(0);
                // Create a mock histogram
                Histogram histogram = mock(Histogram.class);
                // Store histogram in map for lookup
                histogramMap.put(name, histogram);
                return histogram;
            }
        });
        searchQueryCategorizer = SearchQueryCategorizer.getInstance(metricsRegistry);
    }

    @After
    public void cleanup() {
        searchQueryCategorizer.reset();
    }

    public void testAggregationsQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(
            new MultiTermsAggregationBuilder("agg1").terms(
                Arrays.asList(
                    new MultiTermsValuesSourceConfig.Builder().setFieldName("username").build(),
                    new MultiTermsValuesSourceConfig.Builder().setFieldName("rating").build()
                )
            )
        );
        sourceBuilder.size(0);

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 1);

        verify(searchQueryCategorizer.getSearchQueryCounters().getAggCounter()).add(eq(1.0d), any(Tags.class));

        ArgumentCaptor<Double> valueCaptor = ArgumentCaptor.forClass(Double.class);
        ArgumentCaptor<Tags> tagsCaptor = ArgumentCaptor.forClass(Tags.class);

        verify(searchQueryCategorizer.getSearchQueryCounters().getAggCounter()).add(valueCaptor.capture(), tagsCaptor.capture());

        double actualValue = valueCaptor.getValue();
        String actualTag = (String) tagsCaptor.getValue().getTagsMap().get(AGGREGATION_TYPE_TAG);

        assertEquals(1.0d, actualValue, 0.0001);
        assertEquals(MULTI_TERMS_AGGREGATION, actualTag);
    }

    public void testBoolQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(new BoolQueryBuilder().must(new MatchQueryBuilder("searchText", "fox")));

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 2);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("bool")).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("match")).add(eq(1.0d), any(Tags.class));
    }

    public void testFunctionScoreQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(new FunctionScoreQueryBuilder(QueryBuilders.prefixQuery("text", "bro")));

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 1);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("function_score")).add(
            eq(1.0d),
            any(Tags.class)
        );
    }

    public void testMatchQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "php"));

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 1);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("match")).add(eq(1.0d), any(Tags.class));
    }

    public void testMatchPhraseQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(QueryBuilders.matchPhraseQuery("tags", "php"));

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 1);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("match_phrase")).add(eq(1.0d), any(Tags.class));
    }

    public void testMultiMatchQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(new MultiMatchQueryBuilder("foo bar", "myField"));

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 1);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("multi_match")).add(eq(1.0d), any(Tags.class));
    }

    public void testOtherQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        BoostingQueryBuilder queryBuilder = new BoostingQueryBuilder(
            new TermQueryBuilder("unmapped_field", "foo"),
            new MatchNoneQueryBuilder()
        );
        sourceBuilder.query(queryBuilder);

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 3);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("boosting")).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("match_none")).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("term")).add(eq(1.0d), any(Tags.class));
    }

    public void testQueryStringQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder("foo:*");
        sourceBuilder.query(queryBuilder);

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 1);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("query_string")).add(eq(1.0d), any(Tags.class));
    }

    public void testRangeQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder("date");
        rangeQuery.gte("1970-01-01");
        rangeQuery.lt("1982-01-01");
        sourceBuilder.query(rangeQuery);

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 1);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("range")).add(eq(1.0d), any(Tags.class));
    }

    public void testRegexQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new RegexpQueryBuilder("field", "text"));

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 1);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("regexp")).add(eq(1.0d), any(Tags.class));
    }

    public void testSortQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "ruby"));
        sourceBuilder.sort("creationDate", SortOrder.DESC);
        sourceBuilder.sort(new ScoreSortBuilder());

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 3);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("match")).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.getSearchQueryCounters().getSortCounter(), times(2)).add(eq(1.0d), any(Tags.class));
    }

    public void testTermQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(QueryBuilders.termQuery("field", "value2"));

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 1);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("term")).add(eq(1.0d), any(Tags.class));
    }

    public void testWildcardQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(new WildcardQueryBuilder("field", "text"));

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 1);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("wildcard")).add(eq(1.0d), any(Tags.class));
    }

    public void testComplexQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("field", "value2");
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("tags", "php");
        RegexpQueryBuilder regexpQueryBuilder = new RegexpQueryBuilder("field", "text");
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().must(termQueryBuilder)
            .filter(matchQueryBuilder)
            .should(regexpQueryBuilder);
        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.aggregation(new RangeAggregationBuilder("agg1").field("num"));

        SearchQueryRecord record = generateQueryInsightRecords(1, sourceBuilder).get(0);
        searchQueryCategorizer.categorize(record);

        verifyMeasurementHistogramsIncremented(record, 5);

        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("term")).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("match")).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("regexp")).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.getSearchQueryCounters().getCounterByQueryBuilderName("bool")).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.getSearchQueryCounters().getAggCounter()).add(eq(1.0d), any(Tags.class));
    }

    private void verifyMeasurementHistogramsIncremented(SearchQueryRecord record, int times) {
        Double expectedLatency = record.getMeasurement(MetricType.LATENCY).doubleValue();
        Double expectedCpu = record.getMeasurement(MetricType.CPU).doubleValue();
        Double expectedMemory = record.getMeasurement(MetricType.MEMORY).doubleValue();

        Histogram queryTypeLatencyHistogram = histogramMap.get("search.query.type.latency.histogram");
        Histogram queryTypeCpuHistogram = histogramMap.get("search.query.type.cpu.histogram");
        Histogram queryTypeMemoryHistogram = histogramMap.get("search.query.type.memory.histogram");

        verify(queryTypeLatencyHistogram, times(times)).record(eq(expectedLatency), any(Tags.class));
        verify(queryTypeCpuHistogram, times(times)).record(eq(expectedCpu), any(Tags.class));
        verify(queryTypeMemoryHistogram, times(times)).record(eq(expectedMemory), any(Tags.class));
    }
}
