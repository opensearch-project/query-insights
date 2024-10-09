/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugin.insights.SearchSourceBuilderUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

public final class QueryShapeGeneratorTests extends OpenSearchTestCase {
    final Set<Index> successfulSearchShardIndices = Set.of(
        new Index("index1", UUID.randomUUID().toString()),
        new Index("index2", UUID.randomUUID().toString())
    );
    final QueryShapeGenerator queryShapeGenerator;

    public QueryShapeGeneratorTests() {
        final ClusterService mockClusterService = mock(ClusterService.class);
        final ClusterState mockClusterState = mock(ClusterState.class);
        final Metadata mockMetaData = mock(Metadata.class);
//        final MappingMetadata mockMappingMetadata = mock(MappingMetadata.class);
        final MappingMetadata mappingMetadata = new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, Collections.emptyMap());
        final Map<String, MappingMetadata> map = Map.of("index1", mappingMetadata);
        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetaData);
        Map<String, Object> finalMap = Map.of(
            "properties", Map.of(
                "age", Map.of("type", "long"),
                "name", Map.of(
                    "type", "text",
                    "fields", Map.of(
                        "keyword_field_name", Map.of(
                            "type", "keyword",
                            "ignore_above", 256
                        )
                    )
                )
            )
        );
//        when(mockMappingMetadata.getSourceAsMap()).thenReturn(finalMap);
        try {
            when(mockMetaData.findMappings(any(), any())).thenReturn(map);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.queryShapeGenerator = new QueryShapeGenerator(mockClusterService);
    }

    public void testComplexSearch() {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createDefaultSearchSourceBuilder();

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, successfulSearchShardIndices);
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

        String shapeShowFieldsFalse = queryShapeGenerator.buildShape(sourceBuilder, false, successfulSearchShardIndices);
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

    public void testQueryShape() {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder();

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [field1]\n"
            + "  filter:\n"
            + "    match [field2]\n"
            + "    range [field4]\n"
            + "  should:\n"
            + "    regexp [field3]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = queryShapeGenerator.buildShape(sourceBuilder, false, successfulSearchShardIndices);
        String expectedShowFieldsFalse = "bool\n"
            + "  must:\n"
            + "    term\n"
            + "  filter:\n"
            + "    match\n"
            + "    range\n"
            + "  should:\n"
            + "    regexp\n";
        assertEquals(expectedShowFieldsFalse, shapeShowFieldsFalse);
    }

    public void testAggregationShape() {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createAggregationSearchSourceBuilder();

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, successfulSearchShardIndices);
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

        String shapeShowFieldsFalse = queryShapeGenerator.buildShape(sourceBuilder, false, successfulSearchShardIndices);
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

        String shapeShowFieldsTrue = queryShapeGenerator.buildShape(sourceBuilder, true, successfulSearchShardIndices);
        String expectedShowFieldsTrue = "sort:\n" + "  desc [color]\n" + "  desc [vendor]\n" + "  asc [price]\n" + "  asc [album]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = queryShapeGenerator.buildShape(sourceBuilder, false, successfulSearchShardIndices);
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
            successfulSearchShardIndices
        );
        MurmurHash3.Hash128 queryHashTrue = queryShapeGenerator.getShapeHashCode(querySourceBuilder, true, successfulSearchShardIndices);
        assertEquals(defaultHashTrue, queryShapeGenerator.getShapeHashCode(defaultSourceBuilder, true, successfulSearchShardIndices));
        assertEquals(queryHashTrue, queryShapeGenerator.getShapeHashCode(querySourceBuilder, true, successfulSearchShardIndices));
        assertNotEquals(defaultHashTrue, queryHashTrue);

        // showFields false
        MurmurHash3.Hash128 defaultHashFalse = queryShapeGenerator.getShapeHashCode(
            defaultSourceBuilder,
            false,
            successfulSearchShardIndices
        );
        MurmurHash3.Hash128 queryHashFalse = queryShapeGenerator.getShapeHashCode(querySourceBuilder, false, successfulSearchShardIndices);
        assertEquals(defaultHashFalse, queryShapeGenerator.getShapeHashCode(defaultSourceBuilder, false, successfulSearchShardIndices));
        assertEquals(queryHashFalse, queryShapeGenerator.getShapeHashCode(querySourceBuilder, false, successfulSearchShardIndices));
        assertNotEquals(defaultHashFalse, queryHashFalse);

        // Compare field data on vs off
        assertNotEquals(defaultHashTrue, defaultHashFalse);
        assertNotEquals(queryHashTrue, queryHashFalse);
    }
}
