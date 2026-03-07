/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.recommendations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link QueryContext}
 */
public class QueryContextTests extends OpenSearchTestCase {

    // --- getIndices tests ---

    public void testGetIndicesFromStringArray() {
        SearchQueryRecord record = createRecordWithIndices(new String[] { "index1", "index2" });
        QueryContext ctx = new QueryContext(record, null, null);

        List<String> indices = ctx.getIndices();
        assertEquals(2, indices.size());
        assertEquals("index1", indices.get(0));
        assertEquals("index2", indices.get(1));
    }

    public void testGetIndicesFromList() {
        SearchQueryRecord record = createRecordWithIndices(Arrays.asList("idx-a", "idx-b", "idx-c"));
        QueryContext ctx = new QueryContext(record, null, null);

        List<String> indices = ctx.getIndices();
        assertEquals(3, indices.size());
        assertEquals("idx-a", indices.get(0));
        assertEquals("idx-c", indices.get(2));
    }

    public void testGetIndicesFromObjectArray() {
        SearchQueryRecord record = createRecordWithIndices(new Object[] { "obj-index1", "obj-index2" });
        QueryContext ctx = new QueryContext(record, null, null);

        List<String> indices = ctx.getIndices();
        assertEquals(2, indices.size());
        assertEquals("obj-index1", indices.get(0));
        assertEquals("obj-index2", indices.get(1));
    }

    public void testGetIndicesReturnsEmptyWhenNull() {
        Map<Attribute, Object> attributes = new HashMap<>();
        SearchQueryRecord record = new SearchQueryRecord(System.currentTimeMillis(), new HashMap<>(), attributes, "test-id");
        QueryContext ctx = new QueryContext(record, null, null);

        List<String> indices = ctx.getIndices();
        assertTrue(indices.isEmpty());
    }

    public void testGetIndicesReturnsEmptyForUnexpectedType() {
        SearchQueryRecord record = createRecordWithIndices("not-an-array-or-list");
        QueryContext ctx = new QueryContext(record, null, null);

        List<String> indices = ctx.getIndices();
        assertTrue(indices.isEmpty());
    }

    // --- getFieldType / getFieldTypeFromProperties tests ---

    public void testGetFieldTypeSimpleField() {
        ClusterService clusterService = mockClusterWithMapping(
            "test-index",
            Map.of("properties", Map.of("status", Map.of("type", "keyword"), "title", Map.of("type", "text")))
        );
        QueryContext ctx = createContextWithIndex(clusterService, "test-index");

        assertEquals("keyword", ctx.getFieldType("status"));
        assertEquals("text", ctx.getFieldType("title"));
    }

    public void testGetFieldTypeNestedField() {
        Map<String, Object> addressMapping = new HashMap<>();
        addressMapping.put("properties", Map.of("city", Map.of("type", "keyword"), "zip", Map.of("type", "integer")));

        ClusterService clusterService = mockClusterWithMapping("test-index", Map.of("properties", Map.of("address", addressMapping)));
        QueryContext ctx = createContextWithIndex(clusterService, "test-index");

        assertEquals("keyword", ctx.getFieldType("address.city"));
        assertEquals("integer", ctx.getFieldType("address.zip"));
    }

    public void testGetFieldTypeMultifield() {
        Map<String, Object> titleMapping = new HashMap<>();
        titleMapping.put("type", "text");
        titleMapping.put("fields", Map.of("keyword", Map.of("type", "keyword"), "raw", Map.of("type", "keyword")));

        ClusterService clusterService = mockClusterWithMapping("test-index", Map.of("properties", Map.of("title", titleMapping)));
        QueryContext ctx = createContextWithIndex(clusterService, "test-index");

        assertEquals("text", ctx.getFieldType("title"));
        assertEquals("keyword", ctx.getFieldType("title.keyword"));
        assertEquals("keyword", ctx.getFieldType("title.raw"));
    }

    public void testGetFieldTypeReturnsNullForMissingField() {
        ClusterService clusterService = mockClusterWithMapping(
            "test-index",
            Map.of("properties", Map.of("status", Map.of("type", "keyword")))
        );
        QueryContext ctx = createContextWithIndex(clusterService, "test-index");

        assertNull(ctx.getFieldType("nonexistent"));
    }

    public void testGetFieldTypeReturnsNullWhenNoClusterService() {
        SearchQueryRecord record = createRecordWithIndices(Arrays.asList("test-index"));
        QueryContext ctx = new QueryContext(record, null, null);

        assertNull(ctx.getFieldType("status"));
    }

    public void testGetFieldTypeReturnsNullWhenNoIndices() {
        ClusterService clusterService = mockClusterWithMapping(
            "test-index",
            Map.of("properties", Map.of("status", Map.of("type", "keyword")))
        );
        Map<Attribute, Object> attributes = new HashMap<>();
        SearchQueryRecord record = new SearchQueryRecord(System.currentTimeMillis(), new HashMap<>(), attributes, "test-id");
        QueryContext ctx = new QueryContext(record, clusterService, null);

        assertNull(ctx.getFieldType("status"));
    }

    public void testGetFieldTypeReturnsNullForUnknownIndex() {
        ClusterService clusterService = mockClusterWithMapping(
            "existing-index",
            Map.of("properties", Map.of("status", Map.of("type", "keyword")))
        );
        QueryContext ctx = createContextWithIndex(clusterService, "nonexistent-index");

        assertNull(ctx.getFieldType("status"));
    }

    public void testGetFieldTypeReturnsNullWhenNoMapping() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index("test-index")).thenReturn(indexMetadata);
        when(indexMetadata.mapping()).thenReturn(null);

        QueryContext ctx = createContextWithIndex(clusterService, "test-index");
        assertNull(ctx.getFieldType("status"));
    }

    public void testGetFieldTypeReturnsNullWhenNoProperties() {
        ClusterService clusterService = mockClusterWithMapping("test-index", Map.of("no_properties_key", "value"));
        QueryContext ctx = createContextWithIndex(clusterService, "test-index");

        assertNull(ctx.getFieldType("status"));
    }

    public void testGetFieldTypeDeepNestedWithMultifield() {
        Map<String, Object> nameMapping = new HashMap<>();
        nameMapping.put("type", "text");
        nameMapping.put("fields", Map.of("keyword", Map.of("type", "keyword")));

        Map<String, Object> authorMapping = new HashMap<>();
        authorMapping.put("properties", Map.of("name", nameMapping));

        ClusterService clusterService = mockClusterWithMapping("test-index", Map.of("properties", Map.of("author", authorMapping)));
        QueryContext ctx = createContextWithIndex(clusterService, "test-index");

        assertEquals("text", ctx.getFieldType("author.name"));
        assertEquals("keyword", ctx.getFieldType("author.name.keyword"));
    }

    // --- extractQueries tests ---

    public void testExtractQueriesFindsTermQueries() {
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.query(new TermQueryBuilder("status", "active"));

        SearchQueryRecord record = createRecordWithSource(ssb);
        QueryContext ctx = new QueryContext(record, null, null);

        List<TermQueryBuilder> termQueries = ctx.extractQueries(TermQueryBuilder.class);
        assertEquals(1, termQueries.size());
        assertEquals("status", termQueries.get(0).fieldName());
    }

    public void testExtractQueriesReturnsEmptyWhenNoQuery() {
        SearchSourceBuilder ssb = new SearchSourceBuilder();

        SearchQueryRecord record = createRecordWithSource(ssb);
        QueryContext ctx = new QueryContext(record, null, null);

        List<TermQueryBuilder> termQueries = ctx.extractQueries(TermQueryBuilder.class);
        assertTrue(termQueries.isEmpty());
    }

    public void testExtractQueriesReturnsEmptyWhenNullSource() {
        SearchQueryRecord record = new SearchQueryRecord(System.currentTimeMillis(), new HashMap<>(), new HashMap<>(), "test-id");
        QueryContext ctx = new QueryContext(record, null, null);

        List<TermQueryBuilder> termQueries = ctx.extractQueries(TermQueryBuilder.class);
        assertTrue(termQueries.isEmpty());
    }

    // --- constructor validation ---

    public void testConstructorRejectsNullRecord() {
        expectThrows(NullPointerException.class, () -> new QueryContext(null, null, null));
    }

    // --- helpers ---

    private SearchQueryRecord createRecordWithIndices(Object indices) {
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.INDICES, indices);
        return new SearchQueryRecord(System.currentTimeMillis(), new HashMap<>(), attributes, "test-id");
    }

    private SearchQueryRecord createRecordWithSource(SearchSourceBuilder ssb) {
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.INDICES, new String[] { "test-index" });
        return new SearchQueryRecord(System.currentTimeMillis(), new HashMap<>(), attributes, ssb, null, "test-id");
    }

    private QueryContext createContextWithIndex(ClusterService clusterService, String indexName) {
        SearchQueryRecord record = createRecordWithIndices(new ArrayList<>(List.of(indexName)));
        return new QueryContext(record, clusterService, null);
    }

    private ClusterService mockClusterWithMapping(String indexName, Map<String, Object> sourceAsMap) {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(indexName)).thenReturn(indexMetadata);
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(mappingMetadata.sourceAsMap()).thenReturn(sourceAsMap);

        return clusterService;
    }
}
