/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.metrics;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the {@link OperationalMetricsTests} class.
 */
public class OperationalMetricsTests extends OpenSearchTestCase {
    public void testEnumValues() {
        // Verify each enum value has the correct description
        assertEquals(
            "Number of errors when parsing with LocalIndexReader",
            OperationalMetric.LOCAL_INDEX_READER_PARSING_EXCEPTIONS.getDescription()
        );
        assertEquals(
            "Number of failures when ingesting Query Insights data to local indices",
            OperationalMetric.LOCAL_INDEX_EXPORTER_BULK_FAILURES.getDescription()
        );
    }

    public void testToString() {
        // Test the toString method
        assertEquals("LOCAL_INDEX_READER_PARSING_EXCEPTIONS", OperationalMetric.LOCAL_INDEX_READER_PARSING_EXCEPTIONS.toString());
        assertEquals("LOCAL_INDEX_EXPORTER_BULK_FAILURES", OperationalMetric.LOCAL_INDEX_EXPORTER_BULK_FAILURES.toString());
        assertEquals("LOCAL_INDEX_EXPORTER_DELETE_FAILURES", OperationalMetric.LOCAL_INDEX_EXPORTER_DELETE_FAILURES.toString());
        assertEquals("LOCAL_INDEX_EXPORTER_EXCEPTIONS", OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS.toString());
        assertEquals("INVALID_EXPORTER_TYPE_FAILURES", OperationalMetric.INVALID_EXPORTER_TYPE_FAILURES.toString());
        assertEquals("DATA_INGEST_EXCEPTIONS", OperationalMetric.DATA_INGEST_EXCEPTIONS.toString());
        assertEquals("QUERY_CATEGORIZE_EXCEPTIONS", OperationalMetric.QUERY_CATEGORIZE_EXCEPTIONS.toString());
        assertEquals("EXPORTER_FAIL_TO_CLOSE_EXCEPTION", OperationalMetric.EXPORTER_FAIL_TO_CLOSE_EXCEPTION.toString());
        assertEquals("TOP_N_QUERIES_USAGE_COUNT", OperationalMetric.TOP_N_QUERIES_USAGE_COUNT.toString());
    }
}
