/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.reader;

import static org.mockito.Mockito.mock;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_TOP_N_QUERIES_INDEX_PATTERN;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.EXPORT_INDEX;

import org.joda.time.format.DateTimeFormat;
import org.junit.Before;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Granular tests for the {@link QueryInsightsReaderFactoryTests} class.
 */
public class QueryInsightsReaderFactoryTests extends OpenSearchTestCase {
    private final String format = DEFAULT_TOP_N_QUERIES_INDEX_PATTERN;

    private final Client client = mock(Client.class);
    private final NamedXContentRegistry namedXContentRegistry = mock(NamedXContentRegistry.class);
    private QueryInsightsReaderFactory queryInsightsReaderFactory;

    @Before
    public void setup() {
        queryInsightsReaderFactory = new QueryInsightsReaderFactory(client);
    }

    public void testValidateConfigWhenResetReader() {
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        try {
            queryInsightsReaderFactory.validateReaderConfig(settings);
        } catch (Exception e) {
            fail("No exception should be thrown when setting is null");
        }
    }

    public void testInvalidReaderTypeConfig() {
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.put(EXPORT_INDEX, "some_invalid_type").build();
        assertThrows(IllegalArgumentException.class, () -> { queryInsightsReaderFactory.validateReaderConfig(settings); });
    }

    public void testCreateAndCloseReader() {
        QueryInsightsReader reader1 = queryInsightsReaderFactory.createReader(format, namedXContentRegistry);
        assertTrue(reader1 instanceof LocalIndexReader);
        try {
            queryInsightsReaderFactory.closeReader(reader1);
            queryInsightsReaderFactory.closeAllReaders();
        } catch (Exception e) {
            fail("No exception should be thrown when closing reader");
        }
    }

    public void testUpdateReader() {
        LocalIndexReader reader = new LocalIndexReader(client, DateTimeFormat.forPattern(format), namedXContentRegistry);
        queryInsightsReaderFactory.updateReader(reader, "yyyy-MM-dd-HH");
        assertEquals(DateTimeFormat.forPattern("yyyy-MM-dd-HH"), reader.getIndexPattern());
    }

}
