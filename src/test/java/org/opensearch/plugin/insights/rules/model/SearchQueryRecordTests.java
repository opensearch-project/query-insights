/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Granular tests for the {@link SearchQueryRecord} class.
 */
public class SearchQueryRecordTests extends OpenSearchTestCase {

    /**
     * Check that if the serialization, deserialization and equals functions are working as expected
     */
    public void testSerializationAndEquals() throws Exception {
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        List<SearchQueryRecord> copiedRecords = new ArrayList<>();
        for (SearchQueryRecord record : records) {
            copiedRecords.add(roundTripRecord(record));
        }
        assertTrue(QueryInsightsTestUtils.checkRecordsEquals(records, copiedRecords));

    }

    public void testAllMetricTypes() {
        Set<MetricType> allMetrics = MetricType.allMetricTypes();
        Set<MetricType> expected = new HashSet<>(Arrays.asList(MetricType.LATENCY, MetricType.CPU, MetricType.MEMORY));
        assertEquals(expected, allMetrics);
    }

    public void testCompare() {
        SearchQueryRecord record1 = QueryInsightsTestUtils.createFixedSearchQueryRecord();
        SearchQueryRecord record2 = QueryInsightsTestUtils.createFixedSearchQueryRecord();
        assertEquals(0, SearchQueryRecord.compare(record1, record2, MetricType.LATENCY));
    }

    public void testEqual() {
        SearchQueryRecord record1 = QueryInsightsTestUtils.createFixedSearchQueryRecord();
        SearchQueryRecord record2 = QueryInsightsTestUtils.createFixedSearchQueryRecord();
        assertEquals(record1, record2);
    }

    public void testFromXContent() {
        SearchQueryRecord record = QueryInsightsTestUtils.createFixedSearchQueryRecord();
        try (XContentParser recordParser = createParser(JsonXContent.jsonXContent, record.toString())) {
            SearchQueryRecord parsedRecord = SearchQueryRecord.fromXContent(recordParser);
            QueryInsightsTestUtils.checkRecordsEquals(List.of(record), List.of(parsedRecord));
        } catch (Exception e) {
            fail("Test should not throw exceptions when parsing search query record");
        }
    }

    /**
     * Serialize and deserialize a SearchQueryRecord.
     * @param record A SearchQueryRecord to serialize.
     * @return The deserialized, "round-tripped" record.
     */
    private static SearchQueryRecord roundTripRecord(SearchQueryRecord record) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            record.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new SearchQueryRecord(in);
            }
        }
    }
}
