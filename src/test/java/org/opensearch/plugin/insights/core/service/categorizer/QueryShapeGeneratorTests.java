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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.index.query.QueryBuilders.regexpQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.index.query.QueryBuilders.termsQuery;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.plugin.insights.SearchSourceBuilderUtils;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

public final class QueryShapeGeneratorTests extends OpenSearchTestCase {
    final Set<Index> successfulSearchShardIndices = Set.of(new Index("index1", UUID.randomUUID().toString()));
    final QueryShapeGenerator queryShapeGenerator;

    private ClusterService mockClusterService;
    private ClusterState mockClusterState;
    private Metadata mockMetaData;

    public QueryShapeGeneratorTests() {
        CompressorRegistry.defaultCompressor();
        this.mockClusterService = mock(ClusterService.class);
        this.mockClusterState = mock(ClusterState.class);
        this.mockMetaData = mock(Metadata.class);
        this.queryShapeGenerator = new QueryShapeGenerator(mockClusterService);

        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetaData);
    }

    public void setUpMockMappings(String indexName, Map<String, Object> mappingProperties) throws IOException {
        MappingMetadata mockMappingMetadata = mock(MappingMetadata.class);

        when(mockMappingMetadata.getSourceAsMap()).thenReturn(mappingProperties);

        final Map<String, MappingMetadata> indexMappingMap = Map.of(indexName, mockMappingMetadata);
        when(mockMetaData.findMappings(any(), any())).thenReturn(indexMappingMap);
    }

    public void testBasicSearchWithFieldNameAndType() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of(
                "properties",
                Map.of(
                    "field1",
                    Map.of("type", "keyword"),
                    "field2",
                    Map.of("type", "text"),
                    "field3",
                    Map.of("type", "text"),
                    "field4",
                    Map.of("type", "long")
                )
            )
        );

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder();

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [field1, keyword]\n"
            + "  filter:\n"
            + "    match [field2, text]\n"
            + "    range [field4, long]\n"
            + "  should:\n"
            + "    regexp [field3, text]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    // If field type is not found we leave the field type blank
    public void testFieldTypeNotFound() throws IOException {
        setUpMockMappings("index1", Map.of("properties", Map.of("field1", Map.of("type", "keyword"))));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(
                boolQuery().must(termQuery("field1", "value1"))
                    .filter(matchQuery("field2", "value2"))
                    .filter(rangeQuery("field4").gte(10).lte(20))
                    .should(regexpQuery("field3", ".*pattern.*"))
            );

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);

        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [field1, keyword]\n"
            + "  filter:\n"
            + "    match [field2]\n"
            + "    range [field4]\n"
            + "  should:\n"
            + "    regexp [field3]\n";

        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testEmptyMappings() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of(
                "properties",
                Map.of() // No fields defined
            )
        );

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(
                boolQuery().must(termQuery("field1", "value1"))
                    .filter(matchQuery("field2", "value2"))
                    .filter(rangeQuery("field4").gte(10).lte(20))
                    .should(regexpQuery("field3", ".*pattern.*"))
            );

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);

        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [field1]\n"
            + "  filter:\n"
            + "    match [field2]\n"
            + "    range [field4]\n"
            + "  should:\n"
            + "    regexp [field3]\n";

        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    // Field type should be inferred from both the mappings
    public void testMultipleIndexMappings() throws IOException {
        setUpMockMappings("index2", Map.of("properties", Map.of("field1", Map.of("type", "keyword"))));

        setUpMockMappings("index1", Map.of("properties", Map.of("field2", Map.of("type", "text"), "field4", Map.of("type", "long"))));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder();

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [field1]\n"
            + "  filter:\n"
            + "    match [field2, text]\n"
            + "    range [field4, long]\n"
            + "  should:\n"
            + "    regexp [field3]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testDifferentFieldTypes() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of(
                "properties",
                Map.of(
                    "field1",
                    Map.of("type", "keyword"),
                    "field2",
                    Map.of("type", "text"),
                    "field3",
                    Map.of("type", "integer"),
                    "field4",
                    Map.of("type", "boolean")
                )
            )
        );

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder();

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [field1, keyword]\n"
            + "  filter:\n"
            + "    match [field2, text]\n"
            + "    range [field4, boolean]\n"
            + "  should:\n"
            + "    regexp [field3, integer]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testFieldWithNestedProperties() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of(
                "properties",
                Map.of(
                    "nestedField",
                    Map.of(
                        "type",
                        "nested",
                        "properties",
                        Map.of("subField1", Map.of("type", "keyword"), "subField2", Map.of("type", "text"))
                    )
                )
            )
        );

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(boolQuery().must(termQuery("nestedField.subField1", "value1")).filter(matchQuery("nestedField.subField2", "value2")));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [nestedField.subField1, keyword]\n"
            + "  filter:\n"
            + "    match [nestedField.subField2, text]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testFieldWithArrayType() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of(
                "properties",
                Map.of("field1", Map.of("type", "keyword"), "field2", Map.of("type", "text"), "field3", Map.of("type", "keyword"))
            )
        );

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(
                boolQuery().must(termQuery("field1", "value1"))
                    .filter(matchQuery("field2", "value2"))
                    .should(termsQuery("field3", "value3a", "value3b"))
            );

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [field1, keyword]\n"
            + "  filter:\n"
            + "    match [field2, text]\n"
            + "  should:\n"
            + "    terms [field3, keyword]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testFieldWithDateType() throws IOException {
        setUpMockMappings("index1", Map.of("properties", Map.of("dateField", Map.of("type", "date"))));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(boolQuery().filter(rangeQuery("dateField").gte("2024-01-01").lte("2024-12-31")));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n" + "  filter:\n" + "    range [dateField, date]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testFieldWithGeoPointType() throws IOException {
        setUpMockMappings("index1", Map.of("properties", Map.of("location", Map.of("type", "geo_point"))));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(boolQuery().filter(geoDistanceQuery("location").point(40.73, -74.1).distance(200, DistanceUnit.KILOMETERS)));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n" + "  filter:\n" + "    geo_distance [location, geo_point]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testFieldWithBinaryType() throws IOException {
        setUpMockMappings("index1", Map.of("properties", Map.of("binaryField", Map.of("type", "binary"))));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(boolQuery().must(termQuery("binaryField", "base64EncodedString")));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n" + "  must:\n" + "    term [binaryField, binary]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testFieldWithMixedTypes() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of(
                "properties",
                Map.of(
                    "mixedField",
                    Map.of(
                        "type",
                        "object",
                        "properties",
                        Map.of("subField1", Map.of("type", "keyword"), "subField2", Map.of("type", "text"))
                    )
                )
            )
        );

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(boolQuery().must(termQuery("mixedField.subField1", "value1")).filter(matchQuery("mixedField.subField2", "value2")));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [mixedField.subField1, keyword]\n"
            + "  filter:\n"
            + "    match [mixedField.subField2, text]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testFieldWithInvalidQueries() throws IOException {
        setUpMockMappings("index1", Map.of("properties", Map.of("field1", Map.of("type", "keyword"))));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(
                boolQuery().must(termQuery("invalidField", "value1")) // Invalid field
            );

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n" + "  must:\n" + "    term [invalidField]\n"; // No type info, just the invalid field
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testFieldWithDeeplyNestedStructure() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of(
                "properties",
                Map.of(
                    "level1",
                    Map.of(
                        "type",
                        "nested",
                        "properties",
                        Map.of("level2", Map.of("type", "nested", "properties", Map.of("level3", Map.of("type", "keyword"))))
                    )
                )
            )
        );

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(boolQuery().must(termQuery("level1.level2.level3", "value1")));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n" + "  must:\n" + "    term [level1.level2.level3, keyword]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    // We are not parsing fields for scripts
    public void testFieldWithScriptedQuery() throws IOException {
        setUpMockMappings("index1", Map.of("properties", Map.of("scriptedField", Map.of("type", "long"))));

        Script script = new Script(
            ScriptType.INLINE,
            "p.params.threshold < doc['scriptedField'].value",
            "mockscript",
            Map.of("threshold", 100)
        );

        ScriptQueryBuilder scriptedQuery = new ScriptQueryBuilder(script);

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder().query(scriptedQuery);

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "script []\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testDynamicTemplateMappingWithTypeInference() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of(
                "dynamic_templates",
                List.of(
                    Map.of("fields", Map.of("mapping", Map.of("type", "short"), "match_mapping_type", "string", "path_match", "status*"))
                ),
                "properties",
                Map.of("status_code", Map.of("type", "short"), "other_field", Map.of("type", "text"))
            )
        );

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(boolQuery().must(termQuery("status_code", "200")).filter(matchQuery("other_field", "value")));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);

        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [status_code, short]\n"
            + "  filter:\n"
            + "    match [other_field, text]\n";

        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testFieldWithIpAddressType() throws IOException {
        setUpMockMappings("index1", Map.of("properties", Map.of("ip_address", Map.of("type", "ip", "ignore_malformed", true))));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(boolQuery().must(termQuery("ip_address", "192.168.1.1")).filter(termQuery("ip_address", "invalid_ip")));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [ip_address, ip]\n"
            + "  filter:\n"
            + "    term [ip_address, ip]\n";

        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    // Nested query not working as expected
    public void testNestedQueryType() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of(
                "properties",
                Map.of(
                    "patients",
                    Map.of(
                        "type",
                        "nested",
                        "properties",
                        Map.of("name", Map.of("type", "text"), "age", Map.of("type", "integer"), "smoker", Map.of("type", "boolean"))
                    )
                )
            )
        );

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(
                nestedQuery(
                    "patients",
                    boolQuery().should(termQuery("patients.smoker", true)).should(rangeQuery("patients.age").gte(75)),
                    ScoreMode.Avg
                )
            );

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);

        String expectedShowFieldsTrue = "nested []\n" + "  must:\n" + "    bool []\n";

        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testFlatObjectQueryType() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of(
                "properties",
                Map.of(
                    "issue",
                    Map.of(
                        "type",
                        "flat_object",
                        "properties",
                        Map.of(
                            "number",
                            Map.of("type", "keyword"),
                            "labels",
                            Map.of(
                                "properties",
                                Map.of(
                                    "version",
                                    Map.of("type", "keyword"),
                                    "category",
                                    Map.of("properties", Map.of("type", Map.of("type", "keyword"), "level", Map.of("type", "keyword")))
                                )
                            )
                        )
                    )
                )
            )
        );

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(matchQuery("issue.labels.category.level", "bug"));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);

        String expectedShowFieldsTrue = "match [issue.labels.category.level, keyword]\n";

        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testQueryTypeWithSorting() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of(
                "properties",
                Map.of("age", Map.of("type", "integer"), "name", Map.of("type", "keyword"), "score", Map.of("type", "float"))
            )
        );

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(matchQuery("name", "John"))
            .sort(new FieldSortBuilder("age").order(SortOrder.ASC))
            .sort(new FieldSortBuilder("score").order(SortOrder.DESC));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);

        String expectedShowFieldsTrue = "match [name, keyword]\n" + "sort:\n" + "  asc [age, integer]\n" + "  desc [score, float]\n";

        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testQueryTypeWithAggregations() throws IOException {
        setUpMockMappings("index1", Map.of("properties", Map.of("price", Map.of("type", "double"), "category", Map.of("type", "keyword"))));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(matchAllQuery())
            .aggregation(AggregationBuilders.terms("categories").field("category"))
            .aggregation(AggregationBuilders.avg("average_price").field("price"));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);

        String expectedShowFieldsTrue = "match_all []\n" + "aggregation:\n" + "  avg [price, double]\n" + "  terms [category, keyword]\n";

        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    // No field name and type being parsed for pipeline aggregations
    public void testQueryTypeWithPipelineAggregation() throws IOException {
        setUpMockMappings("index1", Map.of("properties", Map.of("sales", Map.of("type", "double"), "timestamp", Map.of("type", "date"))));

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(matchAllQuery())
            .aggregation(
                AggregationBuilders.dateHistogram("sales_over_time")
                    .field("timestamp")
                    .calendarInterval(DateHistogramInterval.MONTH)
                    .subAggregation(AggregationBuilders.sum("total_sales").field("sales"))
            )
            .aggregation(PipelineAggregatorBuilders.derivative("sales_derivative", "total_sales"));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);

        String expectedShowFieldsTrue = "match_all []\n"
            + "aggregation:\n"
            + "  date_histogram [timestamp, date]\n"
            + "    aggregation:\n"
            + "      sum [sales, double]\n"
            + "  pipeline aggregation:\n"
            + "    derivative\n";

        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    // Should cache empty value when we do not find a field type to avoid doing the search again
    public void testFieldTypeCachingForNonExistentField() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of(
                "properties",
                Map.of("age", Map.of("type", "integer"), "name", Map.of("type", "keyword"), "score", Map.of("type", "float"))
            )
        );

        QueryShapeGenerator queryShapeGeneratorSpy = spy(queryShapeGenerator);

        String nonExistentField = "nonExistentField";
        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(matchQuery(nonExistentField, "value"));

        queryShapeGeneratorSpy.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);

        verify(queryShapeGeneratorSpy, atLeastOnce()).getFieldTypeFromCache(eq(nonExistentField), any(Index.class));

        queryShapeGeneratorSpy.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);

        verify(queryShapeGeneratorSpy, atLeastOnce()).getFieldTypeFromCache(eq(nonExistentField), any(Index.class));
    }

    public void testMultifieldQueryCombined() throws IOException {
        setUpMockMappings(
            "index1",
            Map.of("properties", Map.of("title", Map.of("type", "text", "fields", Map.of("raw", Map.of("type", "keyword")))))
        );

        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder()
            .query(boolQuery().must(matchQuery("title", "eg")).should(termQuery("title.raw", "e_g")));

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, true, successfulSearchShardIndices);

        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    match [title, text]\n"
            + "  should:\n"
            + "    term [title.raw, keyword]\n";

        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);
    }

    public void testComplexSearch() {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createDefaultSearchSourceBuilder();

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, false, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [field1]\n"
            + "  filter:\n"
            + "    match [field2]\n"
            + "    range [field4]\n"
            + "  should:\n"
            + "    regexp [field3]\n"
            + "aggregation:\n"
            + "  significant_text []\n"
            + "  terms [key]\n"
            + "    aggregation:\n"
            + "      terms [key.sub1]\n"
            + "      terms [key.sub2]\n"
            + "      pipeline aggregation:\n"
            + "        max_bucket\n"
            + "  terms [model]\n"
            + "  terms [type]\n"
            + "    aggregation:\n"
            + "      terms [key.sub3]\n"
            + "      pipeline aggregation:\n"
            + "        derivative\n"
            + "  top_hits []\n"
            + "  pipeline aggregation:\n"
            + "    avg_bucket\n"
            + "    derivative\n"
            + "    max_bucket\n"
            + "sort:\n"
            + "  desc [color]\n"
            + "  desc [vendor]\n"
            + "  asc [price]\n"
            + "  asc [album]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = queryShapeGenerator.buildShape(sourceBuilder, false, false, successfulSearchShardIndices);
        String expectedShowFieldsFalse = "bool\n"
            + "  must:\n"
            + "    term\n"
            + "  filter:\n"
            + "    match\n"
            + "    range\n"
            + "  should:\n"
            + "    regexp\n"
            + "aggregation:\n"
            + "  significant_text\n"
            + "  terms\n"
            + "  terms\n"
            + "    aggregation:\n"
            + "      terms\n"
            + "      pipeline aggregation:\n"
            + "        derivative\n"
            + "  terms\n"
            + "    aggregation:\n"
            + "      terms\n"
            + "      terms\n"
            + "      pipeline aggregation:\n"
            + "        max_bucket\n"
            + "  top_hits\n"
            + "  pipeline aggregation:\n"
            + "    avg_bucket\n"
            + "    derivative\n"
            + "    max_bucket\n"
            + "sort:\n"
            + "  desc\n"
            + "  desc\n"
            + "  asc\n"
            + "  asc\n";
        assertEquals(expectedShowFieldsFalse, shapeShowFieldsFalse);
    }

    public void testAggregationShape() {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createAggregationSearchSourceBuilder();

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, false, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "aggregation:\n"
            + "  significant_text []\n"
            + "  terms [key]\n"
            + "    aggregation:\n"
            + "      terms [key.sub1]\n"
            + "      terms [key.sub2]\n"
            + "      pipeline aggregation:\n"
            + "        max_bucket\n"
            + "  terms [model]\n"
            + "  terms [type]\n"
            + "    aggregation:\n"
            + "      terms [key.sub3]\n"
            + "      pipeline aggregation:\n"
            + "        derivative\n"
            + "  top_hits []\n"
            + "  pipeline aggregation:\n"
            + "    avg_bucket\n"
            + "    derivative\n"
            + "    max_bucket\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = queryShapeGenerator.buildShape(sourceBuilder, false, false, successfulSearchShardIndices);
        String expectedShowFieldsFalse = "aggregation:\n"
            + "  significant_text\n"
            + "  terms\n"
            + "  terms\n"
            + "    aggregation:\n"
            + "      terms\n"
            + "      pipeline aggregation:\n"
            + "        derivative\n"
            + "  terms\n"
            + "    aggregation:\n"
            + "      terms\n"
            + "      terms\n"
            + "      pipeline aggregation:\n"
            + "        max_bucket\n"
            + "  top_hits\n"
            + "  pipeline aggregation:\n"
            + "    avg_bucket\n"
            + "    derivative\n"
            + "    max_bucket\n";
        assertEquals(expectedShowFieldsFalse, shapeShowFieldsFalse);
    }

    public void testSortShape() {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createSortSearchSourceBuilder();

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, false, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "sort:\n" + "  desc [color]\n" + "  desc [vendor]\n" + "  asc [price]\n" + "  asc [album]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = queryShapeGenerator.buildShape(sourceBuilder, false, false, successfulSearchShardIndices);
        String expectedShowFieldsFalse = "sort:\n" + "  desc\n" + "  desc\n" + "  asc\n" + "  asc\n";
        assertEquals(expectedShowFieldsFalse, shapeShowFieldsFalse);
    }

    public void testHashCode() {
        // Create test source builders
        SearchSourceBuilder defaultSourceBuilder = SearchSourceBuilderUtils.createDefaultSearchSourceBuilder();
        SearchSourceBuilder querySourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder();

        // showFields true
        MurmurHash3.Hash128 defaultHashTrue = queryShapeGenerator.getShapeHashCode(
            defaultSourceBuilder,
            true,
            false,
            successfulSearchShardIndices
        );
        MurmurHash3.Hash128 queryHashTrue = queryShapeGenerator.getShapeHashCode(
            querySourceBuilder,
            true,
            false,
            successfulSearchShardIndices
        );
        assertEquals(
            defaultHashTrue,
            queryShapeGenerator.getShapeHashCode(defaultSourceBuilder, true, false, successfulSearchShardIndices)
        );
        assertEquals(queryHashTrue, queryShapeGenerator.getShapeHashCode(querySourceBuilder, true, false, successfulSearchShardIndices));
        assertNotEquals(defaultHashTrue, queryHashTrue);

        // showFields false
        MurmurHash3.Hash128 defaultHashFalse = queryShapeGenerator.getShapeHashCode(
            defaultSourceBuilder,
            false,
            false,
            successfulSearchShardIndices
        );
        MurmurHash3.Hash128 queryHashFalse = queryShapeGenerator.getShapeHashCode(
            querySourceBuilder,
            false,
            false,
            successfulSearchShardIndices
        );
        assertEquals(
            defaultHashFalse,
            queryShapeGenerator.getShapeHashCode(defaultSourceBuilder, false, false, successfulSearchShardIndices)
        );
        assertEquals(queryHashFalse, queryShapeGenerator.getShapeHashCode(querySourceBuilder, false, false, successfulSearchShardIndices));
        assertNotEquals(defaultHashFalse, queryHashFalse);

        // Compare field data on vs off
        assertNotEquals(defaultHashTrue, defaultHashFalse);
        assertNotEquals(queryHashTrue, queryHashFalse);
    }
}
