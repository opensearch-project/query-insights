/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.insights.core.service;

import static org.mockito.Mockito.mock;

import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class LiveQueriesCacheTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private LiveQueriesCache cache;

    @Before
    public void setup() {
        threadPool = new TestThreadPool("test");
        Client client = mock(Client.class);
        TransportService transportService = mock(TransportService.class);
        cache = new LiveQueriesCache(client, threadPool, transportService);
    }

    @After
    public void cleanup() {
        cache.stop();
        threadPool.shutdown();
    }

    public void testLiveQueriesCache() {
        List<SearchQueryRecord> live = cache.getCurrentQueries();
        assertNotNull(live);
        assertEquals(0, live.size());
    }
}
