/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.reader;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.core.utils.ExporterReaderUtils;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;

/**
 * Granular tests for the {@link LocalIndexReaderTests} class.
 */
public class LocalIndexReaderTests extends OpenSearchTestCase {
    private final DateTimeFormatter format = DateTimeFormatter.ofPattern("YYYY.MM.dd", Locale.ROOT);
    private final Client client = mock(Client.class);
    private final NamedXContentRegistry namedXContentRegistry = mock(NamedXContentRegistry.class);
    private LocalIndexReader localIndexReader;

    @Before
    public void setup() {
        localIndexReader = new LocalIndexReader(client, format, namedXContentRegistry, "id");
    }

    @SuppressWarnings("unchecked")
    public void testReadRecords() throws InterruptedException {
        ActionFuture<SearchResponse> responseActionFuture = mock(ActionFuture.class);
        Map<String, Object> sourceMap = new HashMap<>();
        long timestamp = ZonedDateTime.now(ZoneOffset.UTC).toInstant().toEpochMilli();
        sourceMap.put(SearchQueryRecord.TIMESTAMP, timestamp);
        sourceMap.put(SearchQueryRecord.INDICES, Collections.singletonList("my-index-0"));
        sourceMap.put(Attribute.SOURCE.toString(), Collections.emptyMap());
        sourceMap.put(Attribute.LABELS.toString(), Map.of("label1", "value1"));
        sourceMap.put(Attribute.TASK_RESOURCE_USAGES.toString(), Collections.emptyList());
        sourceMap.put(Attribute.PHASE_LATENCY_MAP.toString(), Collections.singletonMap("fetch", 100L));
        Map<String, Object> measurementsMap = new HashMap<>();
        measurementsMap.put(String.valueOf(MetricType.CPU), Map.of("number", 10000, "count", 1, "aggregationType", "NONE"));
        measurementsMap.put(String.valueOf(MetricType.MEMORY), Map.of("number", 20000, "count", 1, "aggregationType", "NONE"));
        measurementsMap.put(String.valueOf(MetricType.LATENCY), Map.of("number", 3, "count", 1, "aggregationType", "NONE"));
        measurementsMap.put(String.valueOf(MetricType.FAILURE), Map.of("number", 0, "count", 1, "aggregationType", "NONE"));
        sourceMap.put(SearchQueryRecord.MEASUREMENTS, measurementsMap);

        BytesReference sourceRef;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(sourceMap);
            sourceRef = BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new RuntimeException("Failed to build XContent", e);
        }

        SearchHit hit = new SearchHit(
            1,
            "id1",
            Collections.singletonMap("_source", new DocumentField("_source", List.of(sourceMap))),
            new HashMap<>()
        );
        hit.sourceRef(sourceRef);
        SearchHits searchHits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(responseActionFuture.actionGet()).thenReturn(searchResponse);

        String time = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);
        String id = "example-hashcode";

        // Stub admin client and cluster state to return test index
        AdminClient adminClient = mock(AdminClient.class);
        ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        final ZonedDateTime startZdt = ZonedDateTime.parse(time);
        final String expectedIndexName = startZdt.format(format)
            + "-"
            + ExporterReaderUtils.generateLocalIndexDateHash(startZdt.toLocalDate());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<ClusterStateResponse> listener = invocation.getArgument(1);
            ClusterStateResponse mockStateResponse = mock(ClusterStateResponse.class);
            ClusterState mockState = mock(ClusterState.class);
            Metadata metadata = mock(Metadata.class);
            Map<String, IndexMetadata> indicesMap = Collections.singletonMap(expectedIndexName, mock(IndexMetadata.class));
            when(metadata.indices()).thenReturn(indicesMap);
            when(mockState.metadata()).thenReturn(metadata);
            when(mockStateResponse.getState()).thenReturn(mockState);
            listener.onResponse(mockStateResponse);
            return null;
        }).when(clusterAdminClient).state(any(ClusterStateRequest.class), any(ActionListener.class));
        // Stub async search to invoke listener immediately
        doAnswer(invocation -> {
            SearchRequest invokedSearchRequest = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            SearchSourceBuilder sourceBuilder = invokedSearchRequest.source();
            boolean verbose = true;
            if (sourceBuilder != null && sourceBuilder.fetchSource() != null) {
                // See LocalIndexReader.java. This logic is similar to how we exclude fields when verbose == false
                String[] excludes = sourceBuilder.fetchSource().excludes();
                if (excludes != null && excludes.length > 0) {
                    verbose = false;
                }
            }
            if (!verbose) {
                Map<String, Object> filteredSourceMap = new HashMap<>(sourceMap);
                for (Attribute attr : SearchQueryRecord.VERBOSE_ONLY_FIELDS) {
                    filteredSourceMap.remove(attr.toString());
                }

                BytesReference filteredSourceRef;
                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    builder.map(filteredSourceMap);
                    filteredSourceRef = BytesReference.bytes(builder);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to build filtered XContent in mock", e);
                }

                SearchHit filteredHit = new SearchHit(hit.docId(), hit.getId(), null, null);
                filteredHit.sourceRef(filteredSourceRef);
                SearchHits filteredSearchHits = new SearchHits(
                    new SearchHit[] { filteredHit },
                    new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                    1.0f
                );
                SearchResponse filteredSearchResponse = mock(SearchResponse.class);
                when(filteredSearchResponse.getHits()).thenReturn(filteredSearchHits);

                listener.onResponse(filteredSearchResponse);
            } else {
                listener.onResponse(searchResponse);
            }
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        // Test verbose = true
        AtomicReference<List<SearchQueryRecord>> recordsRef1 = new AtomicReference<>();
        CountDownLatch latch1 = new CountDownLatch(1);
        localIndexReader.read(time, time, id, true, MetricType.LATENCY, ActionListener.wrap(records -> {
            recordsRef1.set(records);
            latch1.countDown();
        }, e -> fail("No exception should be thrown when reading query insights data")));
        assertTrue("Listener timed out for verbose=true", latch1.await(1, TimeUnit.SECONDS));
        List<SearchQueryRecord> records1 = recordsRef1.get();
        assertNotNull(records1);
        assertEquals(1, records1.size());
        assertEquals(timestamp, records1.getFirst().getTimestamp());
        assertEquals(10000, records1.getFirst().getMeasurement(MetricType.CPU));
        assertEquals(20000, records1.getFirst().getMeasurement(MetricType.MEMORY));
        assertEquals(3, records1.getFirst().getMeasurement(MetricType.LATENCY));
        assertEquals(0, records1.getFirst().getMeasurement(MetricType.FAILURE));
        Object[] indices = (Object[]) records1.getFirst().getAttributes().get(Attribute.INDICES);
        assertNotNull(indices);
        assertEquals(1, indices.length);
        assertEquals("my-index-0", indices[0]);
        // Verify verbose-only fields are present for verbose=true
        assertTrue(records1.getFirst().getAttributes().containsKey(Attribute.SOURCE));
        assertNotNull(records1.getFirst().getAttributes().get(Attribute.SOURCE));
        assertTrue(records1.getFirst().getAttributes().containsKey(Attribute.TASK_RESOURCE_USAGES));
        assertNotNull(records1.getFirst().getAttributes().get(Attribute.TASK_RESOURCE_USAGES));
        assertTrue(records1.getFirst().getAttributes().containsKey(Attribute.PHASE_LATENCY_MAP));
        assertNotNull(records1.getFirst().getAttributes().get(Attribute.PHASE_LATENCY_MAP));

        // Test verbose = false
        AtomicReference<List<SearchQueryRecord>> recordsRef2 = new AtomicReference<>();
        CountDownLatch latch2 = new CountDownLatch(1);
        localIndexReader.read(time, time, id, false, MetricType.LATENCY, ActionListener.wrap(records -> {
            recordsRef2.set(records);
            latch2.countDown();
        }, e -> fail("No exception should be thrown when reading query insights data")));
        assertTrue("Listener timed out for verbose=false", latch2.await(1, TimeUnit.SECONDS));
        List<SearchQueryRecord> records2 = recordsRef2.get();
        assertNotNull(records2);
        assertEquals(1, records2.size());
        SearchQueryRecord record2 = records2.getFirst();
        // Verbose only fields not exist
        assertNotNull(record2.getAttributes());
        assertFalse(record2.getAttributes().containsKey(Attribute.TASK_RESOURCE_USAGES));
        assertFalse(record2.getAttributes().containsKey(Attribute.SOURCE));
        assertFalse(record2.getAttributes().containsKey(Attribute.PHASE_LATENCY_MAP));
        // Check that other fields are still present
        assertEquals(timestamp, record2.getTimestamp());
        assertEquals(10000, record2.getMeasurement(MetricType.CPU));
        assertEquals(20000, record2.getMeasurement(MetricType.MEMORY));
        assertEquals(3, record2.getMeasurement(MetricType.LATENCY));
        assertEquals(0, record2.getMeasurement(MetricType.FAILURE));
        Object[] indicesNonVerbose = (Object[]) record2.getAttributes().get(Attribute.INDICES);
        assertNotNull(indicesNonVerbose);
        assertEquals(1, indicesNonVerbose.length);
        assertEquals("my-index-0", indicesNonVerbose[0]);
    }

    public void testClose() {
        try {
            localIndexReader.close();
        } catch (Exception e) {
            fail("No exception should be thrown when closing local index reader");
        }
    }

    public void testGetAndSetIndexPattern() {
        final DateTimeFormatter newFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd", Locale.ROOT);
        localIndexReader.setIndexPattern(newFormatter);
        assert (localIndexReader.getIndexPattern() == newFormatter);
    }
}
