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
        assertEquals(
            "LOCAL_INDEX_READER_PARSING_EXCEPTIONS (Number of errors when parsing with LocalIndexReader)",
            OperationalMetric.LOCAL_INDEX_READER_PARSING_EXCEPTIONS.toString()
        );
    }
}
