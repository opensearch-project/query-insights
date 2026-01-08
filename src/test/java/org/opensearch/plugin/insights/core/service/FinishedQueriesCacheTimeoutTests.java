/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.HashMap;
import java.util.Map;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;

public class FinishedQueriesCacheTimeoutTests extends OpenSearchTestCase {

    public void testCacheExpiresAfterInactivity() throws InterruptedException {
        FinishedQueriesCache cache = new FinishedQueriesCache();

        // Add a query
        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(100L));
        measurements.put(MetricType.CPU, new Measurement(200L));
        measurements.put(MetricType.MEMORY, new Measurement(300L));
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.TASK_ID, "task-1");
        attributes.put(Attribute.NODE_ID, "node-1");
        attributes.put(Attribute.DESCRIPTION, "test query");
        attributes.put(Attribute.IS_CANCELLED, false);
        SearchQueryRecord record = new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, "test-id");
        cache.addFinishedQuery(record);

        // Verify query is present
        assertEquals(1, cache.getFinishedQueries(true).size());

        // Wait for timeout (5 minutes + buffer)
        Thread.sleep(305000);

        // Add another query after timeout - should not be added
        Map<MetricType, Measurement> measurements2 = new HashMap<>();
        measurements2.put(MetricType.LATENCY, new Measurement(100L));
        measurements2.put(MetricType.CPU, new Measurement(200L));
        measurements2.put(MetricType.MEMORY, new Measurement(300L));
        Map<Attribute, Object> attributes2 = new HashMap<>();
        attributes2.put(Attribute.TASK_ID, "task-2");
        attributes2.put(Attribute.NODE_ID, "node-1");
        attributes2.put(Attribute.DESCRIPTION, "test query 2");
        attributes2.put(Attribute.IS_CANCELLED, false);
        SearchQueryRecord record2 = new SearchQueryRecord(System.currentTimeMillis(), measurements2, attributes2, "test-id-2");
        cache.addFinishedQuery(record2);

        // Cache should be empty due to timeout
        assertTrue(cache.getFinishedQueries(true).isEmpty());
    }
}
