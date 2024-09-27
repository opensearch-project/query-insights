/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.findFieldType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.plugin.insights.SearchSourceBuilderUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

public final class QueryShapeGeneratorTests extends OpenSearchTestCase {
    static Set<String> SEARCH_SHARD_INDICES = Set.of("index1", "index2", "index3");
    static Metadata mockMetadata;

    /**
     * Helper function to return flat index map.
     *
     * <pre>
     * {
     *   properties = {
     *     field1 = { type = long },
     *     field2 = { type = text },
     *     field3 = { type = boolean }
     *   }
     * }
     * </pre>
     */
    private static Map<String, Object> getFlatIndexMapping() {
        return new HashMap<>() {
            {
                put("properties", new HashMap<>() {
                    {
                        put("field1", new HashMap<>() {
                            {
                                put("type", "long");
                            }
                        });
                        put("field2", new HashMap<>() {
                            {
                                put("type", "text");
                            }
                        });
                        put("field3", new HashMap<>() {
                            {
                                put("type", "boolean");
                            }
                        });
                    }
                });
            }
        };
    }

    /**
     * Helper function to return nested index map.
     *
     * <pre>
     * {
     *   properties = {
     *     field1 = { type = long, fields = {
     *       field2 = { type = text, fields = {
     *         field3 = { type = keyword, ignore_above = 256 }
     *     } } }
     *   }
     * }
     * </pre>
     */
    private static Map<String, Object> getNestedIndexMapping() {
        return new HashMap<>() {
            {
                put("properties", new HashMap<>() {
                    {
                        put("field1", new HashMap<>() {
                            {
                                put("type", "long");
                                put("fields", new HashMap<>() {
                                    {
                                        put("field2", new HashMap<>() {
                                            {
                                                put("type", "text");
                                                put("fields", new HashMap<>() {
                                                    {
                                                        put("field3", new HashMap<>() {
                                                            {
                                                                put("type", "boolean");
                                                                put("ignore_above", 256);
                                                            }
                                                        });
                                                    }
                                                });
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            }
        };
    }

    public void testComplexSearch() {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createDefaultSearchSourceBuilder();

        String shapeShowFieldsTrue = QueryShapeGenerator.buildShape(sourceBuilder, true, mockMetadata, SEARCH_SHARD_INDICES);
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

        String shapeShowFieldsFalse = QueryShapeGenerator.buildShape(sourceBuilder, false, mockMetadata, SEARCH_SHARD_INDICES);
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

        String shapeShowFieldsTrue = QueryShapeGenerator.buildShape(sourceBuilder, true, mockMetadata, SEARCH_SHARD_INDICES);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [field1]\n"
            + "  filter:\n"
            + "    match [field2]\n"
            + "    range [field4]\n"
            + "  should:\n"
            + "    regexp [field3]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = QueryShapeGenerator.buildShape(sourceBuilder, false, mockMetadata, SEARCH_SHARD_INDICES);
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

        String shapeShowFieldsTrue = QueryShapeGenerator.buildShape(sourceBuilder, true, mockMetadata, SEARCH_SHARD_INDICES);
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

        String shapeShowFieldsFalse = QueryShapeGenerator.buildShape(sourceBuilder, false, mockMetadata, SEARCH_SHARD_INDICES);
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

        String shapeShowFieldsTrue = QueryShapeGenerator.buildShape(sourceBuilder, true, mockMetadata, SEARCH_SHARD_INDICES);
        String expectedShowFieldsTrue = "sort:\n" + "  desc [color]\n" + "  desc [vendor]\n" + "  asc [price]\n" + "  asc [album]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = QueryShapeGenerator.buildShape(sourceBuilder, false, mockMetadata, SEARCH_SHARD_INDICES);
        String expectedShowFieldsFalse = "sort:\n" + "  desc\n" + "  desc\n" + "  asc\n" + "  asc\n";
        assertEquals(expectedShowFieldsFalse, shapeShowFieldsFalse);
    }

    public void testHashCode() {
        // Create test source builders
        SearchSourceBuilder defaultSourceBuilder = SearchSourceBuilderUtils.createDefaultSearchSourceBuilder();
        SearchSourceBuilder querySourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder();

        // showFields true
        MurmurHash3.Hash128 defaultHashTrue = QueryShapeGenerator.getShapeHashCode(
            defaultSourceBuilder,
            true,
            mockMetadata,
            SEARCH_SHARD_INDICES
        );
        MurmurHash3.Hash128 queryHashTrue = QueryShapeGenerator.getShapeHashCode(
            querySourceBuilder,
            true,
            mockMetadata,
            SEARCH_SHARD_INDICES
        );
        assertEquals(defaultHashTrue, QueryShapeGenerator.getShapeHashCode(defaultSourceBuilder, true, mockMetadata, SEARCH_SHARD_INDICES));
        assertEquals(queryHashTrue, QueryShapeGenerator.getShapeHashCode(querySourceBuilder, true, mockMetadata, SEARCH_SHARD_INDICES));
        assertNotEquals(defaultHashTrue, queryHashTrue);

        // showFields false
        MurmurHash3.Hash128 defaultHashFalse = QueryShapeGenerator.getShapeHashCode(
            defaultSourceBuilder,
            false,
            mockMetadata,
            SEARCH_SHARD_INDICES
        );
        MurmurHash3.Hash128 queryHashFalse = QueryShapeGenerator.getShapeHashCode(
            querySourceBuilder,
            false,
            mockMetadata,
            SEARCH_SHARD_INDICES
        );
        assertEquals(
            defaultHashFalse,
            QueryShapeGenerator.getShapeHashCode(defaultSourceBuilder, false, mockMetadata, SEARCH_SHARD_INDICES)
        );
        assertEquals(queryHashFalse, QueryShapeGenerator.getShapeHashCode(querySourceBuilder, false, mockMetadata, SEARCH_SHARD_INDICES));
        assertNotEquals(defaultHashFalse, queryHashFalse);

        // Compare field data on vs off
        assertNotEquals(defaultHashTrue, defaultHashFalse);
        assertNotEquals(queryHashTrue, queryHashFalse);
    }

    public void testFindFieldType() {
        Map<String, Object> properties = (Map<String, Object>) getFlatIndexMapping().get("properties");
        String fieldType1 = findFieldType(properties, "field1");
        String fieldType2 = findFieldType(properties, "field2");
        String fieldType3 = findFieldType(properties, "field3");
        assertEquals("long", fieldType1);
        assertEquals("text", fieldType2);
        assertEquals("boolean", fieldType3);

        properties = (Map<String, Object>) getNestedIndexMapping().get("properties");
        fieldType1 = findFieldType(properties, "field1");
        assertEquals("long", fieldType1);
    }
}
