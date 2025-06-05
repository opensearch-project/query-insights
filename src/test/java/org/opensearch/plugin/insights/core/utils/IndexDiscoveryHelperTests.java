/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.utils;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_TOP_N_QUERIES_INDEX_PATTERN;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link IndexDiscoveryHelper}.
 */
public class IndexDiscoveryHelperTests extends OpenSearchTestCase {

    private final DateTimeFormatter indexPattern = DateTimeFormatter.ofPattern(DEFAULT_TOP_N_QUERIES_INDEX_PATTERN, Locale.ROOT);

    public void testBuildLocalIndexName() {
        ZonedDateTime testDate = ZonedDateTime.of(2023, 12, 25, 10, 30, 0, 0, ZoneOffset.UTC);

        String indexName = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, testDate);

        assertNotNull(indexName);
        assertTrue(indexName.startsWith("top_queries-2023.12.25-"));
        assertTrue(indexName.length() > "top_queries-2023.12.25-".length());

        // The hash should be consistent for the same date
        String indexName2 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, testDate);
        assertEquals(indexName, indexName2);
    }

    public void testBuildLocalIndexNameConsistency() {
        ZonedDateTime testDate1 = ZonedDateTime.of(2023, 12, 25, 10, 30, 0, 0, ZoneOffset.UTC);
        ZonedDateTime testDate2 = ZonedDateTime.of(2023, 12, 25, 15, 45, 30, 500, ZoneOffset.UTC);

        String indexName1 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, testDate1);
        String indexName2 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, testDate2);

        // Same date should produce same index name regardless of time
        assertEquals(indexName1, indexName2);
    }

    public void testBuildLocalIndexNameDifferentDates() {
        ZonedDateTime testDate1 = ZonedDateTime.of(2023, 12, 25, 10, 30, 0, 0, ZoneOffset.UTC);
        ZonedDateTime testDate2 = ZonedDateTime.of(2023, 12, 26, 10, 30, 0, 0, ZoneOffset.UTC);

        String indexName1 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, testDate1);
        String indexName2 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, testDate2);

        // Different dates should produce different index names
        assertNotEquals(indexName1, indexName2);
        assertTrue(indexName1.startsWith("top_queries-2023.12.25-"));
        assertTrue(indexName2.startsWith("top_queries-2023.12.26-"));
    }

    public void testBuildLocalIndexNameWithDifferentPatterns() {
        ZonedDateTime testDate = ZonedDateTime.of(2023, 12, 25, 10, 30, 0, 0, ZoneOffset.UTC);
        // Test with the default pattern and some alternative patterns that might be used
        DateTimeFormatter defaultPattern = DateTimeFormatter.ofPattern(DEFAULT_TOP_N_QUERIES_INDEX_PATTERN, Locale.ROOT);
        DateTimeFormatter alternativePattern1 = DateTimeFormatter.ofPattern("'top_queries-'yyyy-MM-dd", Locale.ROOT);
        DateTimeFormatter alternativePattern2 = DateTimeFormatter.ofPattern("'insights-'yyyy.MM.dd", Locale.ROOT);

        String indexName1 = IndexDiscoveryHelper.buildLocalIndexName(defaultPattern, testDate);
        String indexName2 = IndexDiscoveryHelper.buildLocalIndexName(alternativePattern1, testDate);
        String indexName3 = IndexDiscoveryHelper.buildLocalIndexName(alternativePattern2, testDate);

        assertTrue(indexName1.startsWith("top_queries-2023.12.25-"));
        assertTrue(indexName2.startsWith("top_queries-2023-12-25-"));
        assertTrue(indexName3.startsWith("insights-2023.12.25-"));

        // All should have the same hash suffix since it's the same date
        String hash1 = indexName1.substring(indexName1.lastIndexOf("-") + 1);
        String hash2 = indexName2.substring(indexName2.lastIndexOf("-") + 1);
        String hash3 = indexName3.substring(indexName3.lastIndexOf("-") + 1);

        assertEquals(hash1, hash2);
        assertEquals(hash2, hash3);
    }

    public void testBuildLocalIndexNameWithDifferentTimezones() {
        // Test that different timezones on the same UTC date produce the same index name
        ZonedDateTime utcDate = ZonedDateTime.of(2023, 12, 25, 12, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime estDate = utcDate.withZoneSameInstant(ZoneOffset.ofHours(-5));
        ZonedDateTime pstDate = utcDate.withZoneSameInstant(ZoneOffset.ofHours(-8));

        String indexName1 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, utcDate);
        String indexName2 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, estDate);
        String indexName3 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, pstDate);

        // All should produce the same index name since they represent the same instant
        assertEquals(indexName1, indexName2);
        assertEquals(indexName2, indexName3);
    }

    public void testBuildLocalIndexNameWithEdgeCases() {
        // Test leap year
        ZonedDateTime leapYearDate = ZonedDateTime.of(2024, 2, 29, 10, 30, 0, 0, ZoneOffset.UTC);
        String leapYearIndex = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, leapYearDate);
        assertTrue(leapYearIndex.startsWith("top_queries-2024.02.29-"));

        // Test year boundary
        ZonedDateTime newYearDate = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        String newYearIndex = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, newYearDate);
        assertTrue(newYearIndex.startsWith("top_queries-2024.01.01-"));

        // Test end of year
        ZonedDateTime endYearDate = ZonedDateTime.of(2023, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC);
        String endYearIndex = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, endYearDate);
        assertTrue(endYearIndex.startsWith("top_queries-2023.12.31-"));
    }

    public void testCreateClusterStateRequestWithDefaultOptions() {
        ClusterStateRequest request = IndexDiscoveryHelper.createClusterStateRequest(IndicesOptions.lenientExpandOpen());

        assertNotNull(request);
        assertNotNull(request.indices());
        assertEquals(1, request.indices().length);
        assertTrue(request.metadata());
        assertTrue(request.local());
        assertNotNull(request.indicesOptions());
    }

    public void testCreateClusterStateRequestWithStrictOptions() {
        ClusterStateRequest request = IndexDiscoveryHelper.createClusterStateRequest(IndicesOptions.strictExpand());

        assertNotNull(request);
        assertNotNull(request.indices());
        assertEquals(1, request.indices().length);
        assertTrue(request.metadata());
        assertTrue(request.local());
        assertNotNull(request.indicesOptions());
        assertEquals(IndicesOptions.strictExpand(), request.indicesOptions());
    }

    public void testBuildLocalIndexNameHashConsistency() {
        ZonedDateTime testDate = ZonedDateTime.of(2023, 6, 15, 14, 30, 0, 0, ZoneOffset.UTC);

        // Generate multiple index names for the same date
        String indexName1 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, testDate);
        String indexName2 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, testDate);
        String indexName3 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, testDate);

        // All should be identical
        assertEquals(indexName1, indexName2);
        assertEquals(indexName2, indexName3);

        // Verify hash is 5 digits
        String hash = indexName1.substring(indexName1.lastIndexOf("-") + 1);
        assertEquals(5, hash.length());
        assertTrue(hash.matches("\\d{5}"));
    }

    public void testDiscoverIndicesInDateRangeIncludesLastDay() {
        // Test that the date range iteration includes the last day even when end date has time components
        ZonedDateTime start = ZonedDateTime.of(2023, 6, 15, 0, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime end = ZonedDateTime.of(2023, 6, 17, 15, 30, 45, 0, ZoneOffset.UTC); // End date with time

        // Create expected index names for the 3 days
        String expectedIndex1 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, start);
        String expectedIndex2 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, start.plusDays(1));
        String expectedIndex3 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, start.plusDays(2));

        Set<String> existingIndices = Set.of(expectedIndex1, expectedIndex2, expectedIndex3);
        List<String> result = IndexDiscoveryHelper.findIndicesInDateRange(existingIndices, indexPattern, start, end);

        // Should include all 3 days: 2023-06-15, 2023-06-16, 2023-06-17
        assertEquals(3, result.size());
        assertTrue(result.contains(expectedIndex1));
        assertTrue(result.contains(expectedIndex2));
        assertTrue(result.contains(expectedIndex3));
    }

    public void testDiscoverIndicesInDateRangeSingleDay() {
        ZonedDateTime start = ZonedDateTime.of(2023, 6, 15, 10, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime end = ZonedDateTime.of(2023, 6, 15, 23, 59, 59, 0, ZoneOffset.UTC);

        String expectedIndex = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, start);
        Set<String> existingIndices = Set.of(expectedIndex);
        List<String> result = IndexDiscoveryHelper.findIndicesInDateRange(existingIndices, indexPattern, start, end);

        // Should include exactly 1 day
        assertEquals(1, result.size());
        assertTrue(result.contains(expectedIndex));
    }

    public void testDiscoverIndicesInDateRangeWithMissingIndices() {
        // Test range where some indices don't exist
        ZonedDateTime start = ZonedDateTime.of(2023, 6, 15, 0, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime end = ZonedDateTime.of(2023, 6, 17, 12, 0, 0, 0, ZoneOffset.UTC);

        String expectedIndex1 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, start);
        String expectedIndex3 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, start.plusDays(2));

        // Only indices for day 1 and day 3 exist, day 2 is missing
        Set<String> existingIndices = Set.of(expectedIndex1, expectedIndex3);

        List<String> result = IndexDiscoveryHelper.findIndicesInDateRange(existingIndices, indexPattern, start, end);

        // Should include only the 2 existing indices
        assertEquals(2, result.size());
        assertTrue(result.contains(expectedIndex1));
        assertTrue(result.contains(expectedIndex3));
    }

    public void testDiscoverIndicesInDateRangeWithTimeComponentsInEndDate() {
        ZonedDateTime start = ZonedDateTime.of(2023, 6, 15, 0, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime end = ZonedDateTime.of(2023, 6, 17, 23, 59, 59, 999_000_000, ZoneOffset.UTC); // End of day

        // Create expected index names for all 3 days
        String expectedIndex1 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, start);
        String expectedIndex2 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, start.plusDays(1));
        String expectedIndex3 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, start.plusDays(2));

        // All indices exist
        Set<String> existingIndices = Set.of(expectedIndex1, expectedIndex2, expectedIndex3);

        List<String> result = IndexDiscoveryHelper.findIndicesInDateRange(existingIndices, indexPattern, start, end);

        // the last day is NOT missed even when end date has time components
        assertEquals(3, result.size());
        assertTrue(result.contains(expectedIndex1));
        assertTrue(result.contains(expectedIndex2));
        assertTrue(result.contains(expectedIndex3));
    }

    public void testDiscoverIndicesInDateRangeWithStartAndEndAtDifferentTimes() {
        ZonedDateTime start = ZonedDateTime.of(2023, 6, 15, 14, 30, 45, 0, ZoneOffset.UTC); // Afternoon
        ZonedDateTime end = ZonedDateTime.of(2023, 6, 16, 8, 15, 30, 0, ZoneOffset.UTC); // Morning next day

        String expectedIndex1 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, start);
        String expectedIndex2 = IndexDiscoveryHelper.buildLocalIndexName(indexPattern, start.plusDays(1));

        Set<String> existingIndices = Set.of(expectedIndex1, expectedIndex2);

        List<String> result = IndexDiscoveryHelper.findIndicesInDateRange(existingIndices, indexPattern, start, end);

        assertEquals(2, result.size());
        assertTrue(result.contains(expectedIndex1));
        assertTrue(result.contains(expectedIndex2));
    }
}
