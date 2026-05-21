/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.Collections;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.test.OpenSearchTestCase;

public class TaskDetailsTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        TaskInfo taskInfo = new TaskInfo(
            new TaskId("node1", 1L),
            "type",
            "action",
            "desc",
            null,
            100L,
            200L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            null
        );
        TaskDetails details = new TaskDetails(taskInfo, "running");

        BytesStreamOutput out = new BytesStreamOutput();
        details.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        TaskDetails deserialized = new TaskDetails(in);

        assertEquals(details.getStatus(), deserialized.getStatus());
        assertEquals(details.getTaskInfo().getTaskId(), deserialized.getTaskInfo().getTaskId());
    }

    public void testToXContent() throws IOException {
        TaskInfo taskInfo = new TaskInfo(
            new TaskId("node1", 1L),
            "type",
            "action",
            "desc",
            null,
            100L,
            200L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            null
        );
        TaskDetails details = new TaskDetails(taskInfo, "running");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        details.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"task_id\":\"node1:1\""));
        assertTrue(json.contains("\"node_id\":\"node1\""));
        assertTrue(json.contains("\"action\":\"action\""));
        assertTrue(json.contains("\"status\":\"running\""));
        assertTrue(json.contains("\"description\":\"desc\""));
    }
}
