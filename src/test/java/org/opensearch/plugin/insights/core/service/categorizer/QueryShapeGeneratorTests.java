/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.categorizer;

import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.plugin.insights.SearchSourceBuilderUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

public final class QueryShapeGeneratorTests extends OpenSearchTestCase {

    public void testComplexSearch() {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createDefaultSearchSourceBuilder();

        String shapeShowFieldsTrue = QueryShapeGenerator.buildShape(sourceBuilder, true);
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
            + "  asc [album]\n"
            + "  asc [price]\n"
            + "  desc [color]\n"
            + "  desc [vendor]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = QueryShapeGenerator.buildShape(sourceBuilder, false);
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
            + "  asc\n"
            + "  asc\n"
            + "  desc\n"
            + "  desc\n";
        assertEquals(expectedShowFieldsFalse, shapeShowFieldsFalse);
    }

    public void testQueryShape() {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder();

        String shapeShowFieldsTrue = QueryShapeGenerator.buildShape(sourceBuilder, true);
        String expectedShowFieldsTrue = "bool []\n"
            + "  must:\n"
            + "    term [field1]\n"
            + "  filter:\n"
            + "    match [field2]\n"
            + "    range [field4]\n"
            + "  should:\n"
            + "    regexp [field3]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = QueryShapeGenerator.buildShape(sourceBuilder, false);
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

        String shapeShowFieldsTrue = QueryShapeGenerator.buildShape(sourceBuilder, true);
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

        String shapeShowFieldsFalse = QueryShapeGenerator.buildShape(sourceBuilder, false);
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

        String shapeShowFieldsTrue = QueryShapeGenerator.buildShape(sourceBuilder, true);
        String expectedShowFieldsTrue = "sort:\n" + "  asc [album]\n" + "  asc [price]\n" + "  desc [color]\n" + "  desc [vendor]\n";
        assertEquals(expectedShowFieldsTrue, shapeShowFieldsTrue);

        String shapeShowFieldsFalse = QueryShapeGenerator.buildShape(sourceBuilder, false);
        String expectedShowFieldsFalse = "sort:\n" + "  asc\n" + "  asc\n" + "  desc\n" + "  desc\n";
        assertEquals(expectedShowFieldsFalse, shapeShowFieldsFalse);
    }

    public void testHashCode() {
        // Create test source builders
        SearchSourceBuilder defaultSourceBuilder = SearchSourceBuilderUtils.createDefaultSearchSourceBuilder();
        SearchSourceBuilder querySourceBuilder = SearchSourceBuilderUtils.createQuerySearchSourceBuilder();

        // showFields true
        MurmurHash3.Hash128 defaultHashTrue = QueryShapeGenerator.getShapeHashCode(defaultSourceBuilder, true);
        MurmurHash3.Hash128 queryHashTrue = QueryShapeGenerator.getShapeHashCode(querySourceBuilder, true);
        assertEquals(defaultHashTrue, QueryShapeGenerator.getShapeHashCode(defaultSourceBuilder, true));
        assertEquals(queryHashTrue, QueryShapeGenerator.getShapeHashCode(querySourceBuilder, true));
        assertNotEquals(defaultHashTrue, queryHashTrue);

        // showFields false
        MurmurHash3.Hash128 defaultHashFalse = QueryShapeGenerator.getShapeHashCode(defaultSourceBuilder, false);
        MurmurHash3.Hash128 queryHashFalse = QueryShapeGenerator.getShapeHashCode(querySourceBuilder, false);
        assertEquals(defaultHashFalse, QueryShapeGenerator.getShapeHashCode(defaultSourceBuilder, false));
        assertEquals(queryHashFalse, QueryShapeGenerator.getShapeHashCode(querySourceBuilder, false));
        assertNotEquals(defaultHashFalse, queryHashFalse);

        // Compare field data on vs off
        assertNotEquals(defaultHashTrue, defaultHashFalse);
        assertNotEquals(queryHashTrue, queryHashFalse);
    }
}
