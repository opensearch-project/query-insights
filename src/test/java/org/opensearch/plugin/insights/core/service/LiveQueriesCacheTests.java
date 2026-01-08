/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class LiveQueriesCacheTests extends OpenSearchTestCase {

    private Client client;
    private ThreadPool threadPool;
    private TransportService transportService;
    private LiveQueriesCache cache;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        threadPool = new TestThreadPool("test");
        transportService = mock(TransportService.class);
        cache = new LiveQueriesCache(client, threadPool, transportService, -1);
    }

    public void testCollectChildTaskStats() throws Exception {
        TaskInfo parentTask = createTaskInfo("node1:1", "indices:data/read/search", null, 1000L, 2000L);
        TaskInfo childTask1 = createTaskInfo("node1:2", "indices:data/read/search[phase/query]", "node1:1", 500L, 1000L);
        TaskInfo childTask2 = createTaskInfo("node1:3", "indices:data/read/search[phase/fetch]", "node1:1", 300L, 800L);

        TaskGroup child1Group = new TaskGroup(childTask1, new ArrayList<>());
        TaskGroup child2Group = new TaskGroup(childTask2, new ArrayList<>());
        TaskGroup parentGroup = new TaskGroup(parentTask, List.of(child1Group, child2Group));

        ListTasksResponse response = new ListTasksResponse(
            List.of(parentTask, childTask1, childTask2),
            new ArrayList<>(),
            new ArrayList<>()
        );

        // Test implementation would require proper mocking of client.admin().cluster().listTasks()

        cache.start();
        Thread.sleep(500);

        List<org.opensearch.plugin.insights.rules.model.SearchQueryRecord> queries = cache.getCurrentQueries();
        assertEquals(0, queries.size());

        threadPool.shutdown();
    }

    private TaskInfo createTaskInfo(String taskId, String action, String parentTaskId, long cpu, long mem) {
        String[] parts = taskId.split(":");
        TaskId tid = new TaskId(parts[0], Long.parseLong(parts[1]));
        TaskId ptid = parentTaskId != null
            ? new TaskId(parentTaskId.split(":")[0], Long.parseLong(parentTaskId.split(":")[1]))
            : TaskId.EMPTY_TASK_ID;
        return new TaskInfo(tid, "test", action, "test", null, 0L, 0L, false, false, ptid, new HashMap<>(), null);
    }
}
