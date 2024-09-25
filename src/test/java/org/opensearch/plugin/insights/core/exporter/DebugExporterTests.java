/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.core.Logger;
import org.junit.Before;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the {@link DebugExporterTests} class.
 */
public class DebugExporterTests extends OpenSearchTestCase {
    private DebugExporter debugExporter;
    private Logger mockLogger = mock(Logger.class);

    @Before
    public void setup() {
        debugExporter = new DebugExporter(mockLogger);
    }

    public void testGetInstance() {
        DebugExporter instance1 = DebugExporter.getInstance();
        DebugExporter instance2 = DebugExporter.getInstance();
        assertEquals(instance1, instance2);
    }

    public void testExport() {;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(1);
        debugExporter.export(records);
        // Verify that the logger received the expected debug message
        verify(mockLogger).debug(String.format(Locale.ROOT, "QUERY_INSIGHTS_RECORDS: %s", records));
    }

    public void testClose() {
        debugExporter.close();
        // Verify that the logger received the expected debug message
        verify(mockLogger).debug("Closing the DebugExporter..");
    }
}
