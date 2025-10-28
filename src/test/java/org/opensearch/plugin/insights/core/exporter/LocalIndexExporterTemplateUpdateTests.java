/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.mockito.Mockito.mock;

import java.time.format.DateTimeFormatter;
import java.util.Locale;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

/**
 * Tests for LocalIndexExporter template update functionality
 */
public class LocalIndexExporterTemplateUpdateTests extends OpenSearchTestCase {

    private static final String TEST_MAPPING = "{\"properties\":{\"source\":{\"type\":\"keyword\",\"index\":false}}}";

    private LocalIndexExporter createExporter(String mapping) {
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        return new LocalIndexExporter(
            client,
            clusterService,
            DateTimeFormatter.ofPattern("'top_queries-'yyyy.MM.dd", Locale.ROOT),
            mapping,
            "test-exporter"
        );
    }

    public void testReadIndexMappings() throws Exception {
        LocalIndexExporter exporter = createExporter(TEST_MAPPING);
        String result = exporter.readIndexMappings();
        assertEquals("Should return the provided mapping", TEST_MAPPING, result);
    }

    public void testReadIndexMappingsEmpty() throws Exception {
        LocalIndexExporter emptyExporter = createExporter("");
        String result = emptyExporter.readIndexMappings();
        assertEquals("Should return empty object for empty mapping", "{}", result);
    }

    public void testTemplatePriorityGetterSetter() {
        LocalIndexExporter exporter = createExporter(TEST_MAPPING);
        long newPriority = 500L;
        exporter.setTemplatePriority(newPriority);
        assertEquals("Template priority should be updated", newPriority, exporter.getTemplatePriority());
    }

    public void testExporterId() {
        LocalIndexExporter exporter = createExporter(TEST_MAPPING);
        assertEquals("Should return correct exporter ID", "test-exporter", exporter.getId());
    }

    public void testIndexPatternGetterSetter() {
        LocalIndexExporter exporter = createExporter(TEST_MAPPING);
        DateTimeFormatter newPattern = DateTimeFormatter.ofPattern("'custom-'yyyy-MM-dd", Locale.ROOT);
        exporter.setIndexPattern(newPattern);
        assertEquals("Index pattern should be updated", newPattern, exporter.getIndexPattern());
    }

    public void testSourceFieldInNewTemplate() throws Exception {
        LocalIndexExporter exporter = createExporter(TEST_MAPPING);
        String mapping = exporter.readIndexMappings();
        assertTrue("Template should contain source field", mapping.contains("source"));
        assertTrue("Source field should be keyword type", mapping.contains("\"type\":\"keyword\""));
        assertTrue("Source field should not be indexed", mapping.contains("\"index\":false"));
    }

    public void testSourceFieldInUpdatedTemplate() throws Exception {
        // Test template without SOURCE field (simulating old template)
        String oldMapping = "{\"properties\":{\"timestamp\":{\"type\":\"long\"}}}";
        LocalIndexExporter oldExporter = createExporter(oldMapping);

        String oldResult = oldExporter.readIndexMappings();
        assertFalse("Old template should not contain source field", oldResult.contains("source"));

        // Test new template with SOURCE field
        LocalIndexExporter newExporter = createExporter(TEST_MAPPING);
        String newResult = newExporter.readIndexMappings();
        assertTrue("New template should contain source field", newResult.contains("source"));
        assertTrue(
            "Source should be properly configured for string storage",
            newResult.contains("\"source\":{\"type\":\"keyword\",\"index\":false}")
        );
    }
}
