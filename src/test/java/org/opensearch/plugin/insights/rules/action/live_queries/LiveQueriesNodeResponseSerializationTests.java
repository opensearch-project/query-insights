/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import static org.mockito.Mockito.mock;

import java.util.List;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.plugin.insights.rules.model.TaskRecord;
import org.opensearch.test.OpenSearchTestCase;

public class LiveQueriesNodeResponseSerializationTests extends OpenSearchTestCase {

    public void testGetTasks() {
        DiscoveryNode node = mock(DiscoveryNode.class);
        List<TaskRecord> tasks = List.of();

        LiveQueriesNodeResponse response = new LiveQueriesNodeResponse(node, tasks);

        assertSame(tasks, response.getTasks());
    }
}
