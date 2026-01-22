/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for finished queries functionality in {@link LiveQueriesResponse}.
 */
public class LiveQueriesResponseFinishedTests extends OpenSearchTestCase {

    private SearchQueryRecord createFinishedQuery(String id, long latency) {
        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(latency));
        measurements.put(MetricType.CPU, new Measurement(randomLongBetween(100, 1000)));
        measurements.put(MetricType.MEMORY, new Measurement(randomLongBetween(1024, 10240)));

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.SEARCH_TYPE, "query_then_fetch");
        attributes.put(Attribute.TOTAL_SHARDS, 1);

        return new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, id);
    }

    public void testToXContentWithFinishedQueries() throws IOException {
        // Create live queries
        SearchQueryRecord liveQuery = createFinishedQuery("live1", 500L);
        List<SearchQueryRecord> liveQueries = List.of(liveQuery);

        // Create finished queries
        SearchQueryRecord finishedQuery1 = createFinishedQuery("finished1", 100L);
        SearchQueryRecord finishedQuery2 = createFinishedQuery("finished2", 200L);
        List<SearchQueryRecord> finishedQueries = List.of(finishedQuery1, finishedQuery2);

        LiveQueriesResponse response = new LiveQueriesResponse(liveQueries, finishedQueries, true);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        // Verify live queries section
        assertTrue(parsed.containsKey("live_queries"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> liveQueriesList = (List<Map<String, Object>>) parsed.get("live_queries");
        assertEquals(1, liveQueriesList.size());
        assertEquals("live1", liveQueriesList.get(0).get("id"));

        // Verify finished queries section
        assertTrue(parsed.containsKey("finished_queries"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> finishedQueriesList = (List<Map<String, Object>>) parsed.get("finished_queries");
        assertEquals(2, finishedQueriesList.size());
        assertEquals("finished1", finishedQueriesList.get(0).get("id"));
        assertEquals("finished2", finishedQueriesList.get(1).get("id"));
    }

    public void testToXContentWithOnlyLiveQueries() throws IOException {
        SearchQueryRecord liveQuery = createFinishedQuery("live1", 500L);
        List<SearchQueryRecord> liveQueries = List.of(liveQuery);

        LiveQueriesResponse response = new LiveQueriesResponse(liveQueries);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        assertTrue(parsed.containsKey("live_queries"));
        assertFalse(parsed.containsKey("finished_queries"));
    }

    public void testToXContentWithEmptyFinishedQueries() throws IOException {
        SearchQueryRecord liveQuery = createFinishedQuery("live1", 500L);
        List<SearchQueryRecord> liveQueries = List.of(liveQuery);
        List<SearchQueryRecord> finishedQueries = Collections.emptyList();

        LiveQueriesResponse response = new LiveQueriesResponse(liveQueries, finishedQueries, true);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        assertTrue(parsed.containsKey("live_queries"));
        assertTrue(parsed.containsKey("finished_queries"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> finishedQueriesList = (List<Map<String, Object>>) parsed.get("finished_queries");
        assertTrue(finishedQueriesList.isEmpty());
    }

    public void testSerializationWithFinishedQueries() throws IOException {
        List<SearchQueryRecord> liveQueries = List.of(createFinishedQuery("live1", 500L));
        List<SearchQueryRecord> finishedQueries = List.of(createFinishedQuery("finished1", 100L), createFinishedQuery("finished2", 200L));

        LiveQueriesResponse originalResponse = new LiveQueriesResponse(liveQueries, finishedQueries, true);

        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesResponse deserializedResponse = new LiveQueriesResponse(in);

        assertEquals(originalResponse.getLiveQueries().size(), deserializedResponse.getLiveQueries().size());
        assertEquals(originalResponse.getLiveQueries(), deserializedResponse.getLiveQueries());
    }

    public void testConstructorWithFinishedQueries() {
        List<SearchQueryRecord> liveQueries = List.of(createFinishedQuery("live1", 500L));
        List<SearchQueryRecord> finishedQueries = List.of(createFinishedQuery("finished1", 100L));

        LiveQueriesResponse response = new LiveQueriesResponse(liveQueries, finishedQueries, true);

        assertEquals(1, response.getLiveQueries().size());
        assertEquals("live1", response.getLiveQueries().get(0).getId());
    }

    public void testConstructorWithoutFinishedQueries() {
        List<SearchQueryRecord> liveQueries = List.of(createFinishedQuery("live1", 500L));

        LiveQueriesResponse response = new LiveQueriesResponse(liveQueries);

        assertEquals(1, response.getLiveQueries().size());
        assertEquals("live1", response.getLiveQueries().get(0).getId());
    }
}
