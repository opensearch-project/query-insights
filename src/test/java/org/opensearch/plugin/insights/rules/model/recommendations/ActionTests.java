/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.recommendations;

import java.io.IOException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link Action}
 */
public class ActionTests extends OpenSearchTestCase {

    public void testBuilder() {
        Action action = Action.builder()
            .name("use_keyword_subfield")
            .hint("Use title.keyword instead of title")
            .documentationUrl("https://opensearch.org/docs/latest/query-dsl/term/term/")
            .suggestedQuery("{\"query\":{\"term\":{\"title.keyword\":\"OpenSearch\"}}}")
            .build();

        assertEquals("use_keyword_subfield", action.getName());
        assertEquals("Use title.keyword instead of title", action.getHint());
        assertEquals("https://opensearch.org/docs/latest/query-dsl/term/term/", action.getDocumentationUrl());
        assertEquals("{\"query\":{\"term\":{\"title.keyword\":\"OpenSearch\"}}}", action.getSuggestedQuery());
    }

    public void testBuilderWithNulls() {
        Action action = Action.builder().name("test_action").build();

        assertEquals("test_action", action.getName());
        assertNull(action.getHint());
        assertNull(action.getDocumentationUrl());
        assertNull(action.getSuggestedQuery());
    }

    public void testSerialization() throws IOException {
        Action original = createTestAction();
        Action deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getHint(), deserialized.getHint());
        assertEquals(original.getDocumentationUrl(), deserialized.getDocumentationUrl());
        assertEquals(original.getSuggestedQuery(), deserialized.getSuggestedQuery());
    }

    public void testSerializationWithNulls() throws IOException {
        Action original = Action.builder().build();
        Action deserialized = roundTrip(original);

        assertEquals(original, deserialized);
        assertNull(deserialized.getName());
        assertNull(deserialized.getHint());
        assertNull(deserialized.getDocumentationUrl());
        assertNull(deserialized.getSuggestedQuery());
    }

    public void testEquals() {
        Action action1 = createTestAction();
        Action action2 = createTestAction();

        assertEquals(action1, action2);
        assertEquals(action1, action1);
        assertNotEquals(action1, null);
        assertNotEquals(action1, "not an action");
    }

    public void testNotEquals() {
        Action action1 = createTestAction();

        Action differentName = Action.builder()
            .name("different")
            .hint(action1.getHint())
            .documentationUrl(action1.getDocumentationUrl())
            .suggestedQuery(action1.getSuggestedQuery())
            .build();
        assertNotEquals(action1, differentName);

        Action differentHint = Action.builder()
            .name(action1.getName())
            .hint("different hint")
            .documentationUrl(action1.getDocumentationUrl())
            .suggestedQuery(action1.getSuggestedQuery())
            .build();
        assertNotEquals(action1, differentHint);

        Action differentUrl = Action.builder()
            .name(action1.getName())
            .hint(action1.getHint())
            .documentationUrl("https://different.url")
            .suggestedQuery(action1.getSuggestedQuery())
            .build();
        assertNotEquals(action1, differentUrl);

        Action differentQuery = Action.builder()
            .name(action1.getName())
            .hint(action1.getHint())
            .documentationUrl(action1.getDocumentationUrl())
            .suggestedQuery("{\"query\":{\"match_all\":{}}}")
            .build();
        assertNotEquals(action1, differentQuery);
    }

    public void testHashCode() {
        Action action1 = createTestAction();
        Action action2 = createTestAction();

        assertEquals(action1.hashCode(), action2.hashCode());
    }

    public void testToXContent() throws IOException {
        Action action = createTestAction();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        action.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"name\":\"use_keyword_subfield\""));
        assertTrue(json.contains("\"hint\":\"Use title.keyword instead of title\""));
        assertTrue(json.contains("\"documentation_url\":\"https://opensearch.org/docs/latest/query-dsl/term/term/\""));
        assertTrue(json.contains("\"suggested_query\""));
    }

    public void testToXContentWithNulls() throws IOException {
        Action action = Action.builder().build();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        action.toXContent(builder, null);
        String json = builder.toString();

        assertEquals("{}", json);
    }

    private Action createTestAction() {
        return Action.builder()
            .name("use_keyword_subfield")
            .hint("Use title.keyword instead of title")
            .documentationUrl("https://opensearch.org/docs/latest/query-dsl/term/term/")
            .suggestedQuery("{\"query\":{\"term\":{\"title.keyword\":\"OpenSearch\"}}}")
            .build();
    }

    private Action roundTrip(Action action) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            action.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new Action(in);
            }
        }
    }
}
