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
import java.util.Map;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

/** A search's source is written as a JSON object; a SQL/PPL query's text as a string. */
public class FinishedQueryRecordTests extends OpenSearchTestCase {

    public void testPplQueryTextIsWrittenAsAString() throws IOException {
        String text = "source=logs | where status = 500 | stats count() by host";
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.QUERY_SOURCE, "PPL");
        attributes.put(Attribute.SOURCE, new SourceString(text));
        attributes.put(Attribute.SOURCE_TRUNCATED, false);

        Map<String, Object> json = toMap(finished(attributes, "node-1:42"));

        assertEquals(text, json.get("source"));
        assertEquals("node-1:42", json.get("id"));
    }

    public void testSearchSourceIsWrittenAsAnObject() throws IOException {
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.SOURCE, new SourceString("{\"size\":5}"));
        attributes.put(Attribute.SOURCE_TRUNCATED, false);

        assertEquals(Map.of("size", 5), toMap(finished(attributes, "node-1:7")).get("source"));
    }

    private static FinishedQueryRecord finished(Map<Attribute, Object> attributes, String liveQueryId) {
        SearchQueryRecord record = new SearchQueryRecord(1L, new HashMap<>(), attributes, "top-n-id");
        return new FinishedQueryRecord(record, record.getId(), "completed", liveQueryId);
    }

    private static Map<String, Object> toMap(FinishedQueryRecord record) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        record.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, builder.toString(), false);
    }
}
