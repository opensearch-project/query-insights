/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.LiveQueryRecord;
import org.opensearch.test.OpenSearchTestCase;

public class LiveQueriesResponseFinishedTests extends OpenSearchTestCase {

    private LiveQueryRecord createRecord(String id, long latency) {
        return new LiveQueryRecord(id, "completed", System.currentTimeMillis(), null, latency, 200L, 300L, null, new ArrayList<>());
    }

    public void testToXContentWithFinishedQueries() throws IOException {
        List<LiveQueryRecord> liveQueries = List.of(createRecord("live1", 500L));
        List<LiveQueryRecord> finishedQueries = List.of(createRecord("finished1", 100L), createRecord("finished2", 200L));

        LiveQueriesResponse response = new LiveQueriesResponse(liveQueries, finishedQueries, true);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        assertTrue(parsed.containsKey("live_queries"));
        assertTrue(parsed.containsKey("finished_queries"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> liveList = (List<Map<String, Object>>) parsed.get("live_queries");
        assertEquals(1, liveList.size());
        assertEquals("live1", liveList.get(0).get("id"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> finishedList = (List<Map<String, Object>>) parsed.get("finished_queries");
        assertEquals(2, finishedList.size());
    }

    public void testToXContentWithOnlyLiveQueries() throws IOException {
        List<LiveQueryRecord> liveQueries = List.of(createRecord("live1", 500L));
        LiveQueriesResponse response = new LiveQueriesResponse(liveQueries);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        assertTrue(parsed.containsKey("live_queries"));
        assertFalse(parsed.containsKey("finished_queries"));
    }

    public void testToXContentWithEmptyFinishedQueries() throws IOException {
        List<LiveQueryRecord> liveQueries = List.of(createRecord("live1", 500L));
        LiveQueriesResponse response = new LiveQueriesResponse(liveQueries, Collections.emptyList(), true);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        assertTrue(parsed.containsKey("finished_queries"));
        @SuppressWarnings("unchecked")
        List<?> finishedList = (List<?>) parsed.get("finished_queries");
        assertTrue(finishedList.isEmpty());
    }

    public void testSerializationWithFinishedQueries() throws IOException {
        List<LiveQueryRecord> liveQueries = List.of(createRecord("live1", 500L));
        List<LiveQueryRecord> finishedQueries = List.of(createRecord("finished1", 100L));

        LiveQueriesResponse original = new LiveQueriesResponse(liveQueries, finishedQueries, true);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        LiveQueriesResponse deserialized = new LiveQueriesResponse(in);

        assertEquals(original.getLiveQueries().size(), deserialized.getLiveQueries().size());
    }

    public void testConstructorWithFinishedQueries() {
        List<LiveQueryRecord> liveQueries = List.of(createRecord("live1", 500L));
        List<LiveQueryRecord> finishedQueries = List.of(createRecord("finished1", 100L));

        LiveQueriesResponse response = new LiveQueriesResponse(liveQueries, finishedQueries, true);
        assertEquals(1, response.getLiveQueries().size());
        assertEquals("live1", response.getLiveQueries().get(0).getQueryId());
    }
}
