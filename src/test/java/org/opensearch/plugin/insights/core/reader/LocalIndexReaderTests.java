/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.reader;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Granular tests for the {@link LocalIndexReaderTests} class.
 */
public class LocalIndexReaderTests extends OpenSearchTestCase {
    private final DateTimeFormatter format = DateTimeFormat.forPattern("YYYY.MM.dd");
    private final Client client = mock(Client.class);
    private final NamedXContentRegistry namedXContentRegistry = mock(NamedXContentRegistry.class);
    private LocalIndexReader localIndexReader;

    @Before
    public void setup() {
        localIndexReader = new LocalIndexReader(client, format, namedXContentRegistry);
    }

    @SuppressWarnings("unchecked")
    public void testReadRecords() {
        ActionFuture<SearchResponse> responseActionFuture = mock(ActionFuture.class);
        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("timestamp", DateTime.now(DateTimeZone.UTC).getMillis());
        sourceMap.put("indices", Collections.singletonList("my-index-0"));
        sourceMap.put("source", Map.of());
        sourceMap.put("labels", Map.of());
        sourceMap.put("cpu", 10000);
        sourceMap.put("memory", 20000);
        sourceMap.put("latency", 3);

        BytesReference sourceRef;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(sourceMap); // Writes the map to XContentBuilder
            sourceRef = BytesReference.bytes(builder); // Converts XContentBuilder to BytesReference
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
        when(client.search(any(SearchRequest.class))).thenReturn(responseActionFuture);
        String time = DateTime.now(DateTimeZone.UTC).toString();
        List<SearchQueryRecord> records = List.of();
        try {
            records = localIndexReader.read(time, time);
        } catch (Exception e) {
            fail("No exception should be thrown when reading query insights data");
        }
        assertNotNull(records);
        assertEquals(1, records.size());
    }

    public void testClose() {
        try {
            localIndexReader.close();
        } catch (Exception e) {
            fail("No exception should be thrown when closing local index reader");
        }
    }

    public void testGetAndSetIndexPattern() {
        DateTimeFormatter newFormatter = mock(DateTimeFormatter.class);
        localIndexReader.setIndexPattern(newFormatter);
        assert (localIndexReader.getIndexPattern() == newFormatter);
    }
}
