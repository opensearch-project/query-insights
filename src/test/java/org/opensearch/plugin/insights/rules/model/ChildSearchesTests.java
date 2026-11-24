/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

/** The searches a SQL/PPL query ran, as its record keeps them. */
public class ChildSearchesTests extends OpenSearchTestCase {

    private static ChildSearches searches() {
        return new ChildSearches(
            250,
            List.of(
                new ChildSearches.Search(1_700_000_000_000L, List.of("logs", "metrics"), 3, 12L, 4_000_000L, 2_048L, false, "analytics"),
                new ChildSearches.Search(1_700_000_000_100L, List.of("logs"), 1, 7L, 1_000_000L, 512L, true, null)
            )
        );
    }

    public void testTheyTravelBetweenNodes() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            searches().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(searches(), new ChildSearches(in));
            }
        }
    }

    public void testTheyReadBackFromTheRecordsDocument() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        searches().toXContent(builder, ToXContent.EMPTY_PARAMS);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder).utf8ToString())) {
            parser.nextToken();
            assertEquals(searches(), ChildSearches.fromXContent(parser));
        }
    }

    public void testTheyReadBackFromTheJsonStringADocumentStores() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("child_searches", searches().toJson()).endObject();
        String document = BytesReference.bytes(builder).utf8ToString();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, document)) {
            parser.nextToken(); // the document
            parser.nextToken(); // its field name
            parser.nextToken(); // the string
            assertEquals(XContentParser.Token.VALUE_STRING, parser.currentToken());
            assertEquals(searches(), ChildSearches.fromXContent(parser));
        }
    }

    public void testTheDocumentNamesEachField() {
        String json = Strings.toString(MediaTypeRegistry.JSON, searches());

        assertEquals(
            "{\"count\":250,\"searches\":["
                + "{\"timestamp\":1700000000000,\"indices\":[\"logs\",\"metrics\"],\"total_shards\":3,\"latency_millis\":12,"
                + "\"cpu_nanos\":4000000,\"memory_bytes\":2048,\"failed\":false,\"wlm_group_id\":\"analytics\"},"
                + "{\"timestamp\":1700000000100,\"indices\":[\"logs\"],\"total_shards\":1,\"latency_millis\":7,"
                + "\"cpu_nanos\":1000000,\"memory_bytes\":512,\"failed\":true}]}",
            json
        );
    }

    public void testFieldsItDoesntKnowAreSkipped() throws IOException {
        String json = "{\"count\":1,\"later\":{\"a\":[1,2]},\"searches\":[{\"timestamp\":5,\"later\":[\"x\"],\"failed\":true}]}";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            ChildSearches read = ChildSearches.fromXContent(parser);

            assertEquals(new ChildSearches(1, List.of(new ChildSearches.Search(5L, List.of(), 0, 0L, 0L, 0L, true, null))), read);
        }
    }

    public void testASearchTakesWhatTheDetailViewShowsFromItsRecord() {
        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(12L));
        measurements.put(MetricType.CPU, new Measurement(4_000_000L));
        measurements.put(MetricType.MEMORY, new Measurement(2_048L));
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.INDICES, new String[] { "logs", "metrics" });
        attributes.put(Attribute.TOTAL_SHARDS, 3);
        attributes.put(Attribute.FAILED, false);
        attributes.put(Attribute.WLM_GROUP_ID, "analytics");
        attributes.put(Attribute.SEARCH_TYPE, "query_then_fetch");
        SearchQueryRecord record = new SearchQueryRecord(1_700_000_000_000L, measurements, attributes, "search-1");

        assertEquals(searches().getSearches().get(0), ChildSearches.Search.of(record));
    }

    public void testASearchWithoutOptionalFieldsHasDefaults() {
        SearchQueryRecord record = new SearchQueryRecord(1L, new HashMap<>(), new HashMap<>(), "search-1");

        assertEquals(new ChildSearches.Search(1L, List.of(), 0, 0L, 0L, 0L, false, null), ChildSearches.Search.of(record));
    }
}
