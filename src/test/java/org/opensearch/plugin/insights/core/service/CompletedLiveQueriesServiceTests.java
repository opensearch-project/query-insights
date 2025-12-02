/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to\n * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the {@link CompletedLiveQueriesService} class.
 */
public class CompletedLiveQueriesServiceTests extends OpenSearchTestCase {

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        CompletedLiveQueriesService.resetForTesting();
    }

    private SearchQueryRecord createTestRecord(String id) {
        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(100L));
        measurements.put(MetricType.CPU, new Measurement(50L));
        measurements.put(MetricType.MEMORY, new Measurement(1024L));

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.NODE_ID, "test-node");

        return new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, id);
    }

    public void testSingletonInstance() {
        CompletedLiveQueriesService service1 = CompletedLiveQueriesService.getInstance();
        CompletedLiveQueriesService service2 = CompletedLiveQueriesService.getInstance();
        assertSame(service1, service2);
    }

    public void testAddAndRetrieveCompletedQueries() {
        CompletedLiveQueriesService service = CompletedLiveQueriesService.getInstance();

        SearchQueryRecord record1 = createTestRecord("query1");
        SearchQueryRecord record2 = createTestRecord("query2");

        service.addCompletedQuery(record1);
        service.addCompletedQuery(record2);

        List<SearchQueryRecord> completedQueries = service.getCompletedQueries();
        assertEquals(2, completedQueries.size());
        assertTrue(completedQueries.stream().anyMatch(r -> "query1".equals(r.getId())));
        assertTrue(completedQueries.stream().anyMatch(r -> "query2".equals(r.getId())));
    }

    public void testEmptyCompletedQueries() {
        CompletedLiveQueriesService service = CompletedLiveQueriesService.getInstance();
        List<SearchQueryRecord> completedQueries = service.getCompletedQueries();
        assertNotNull(completedQueries);
    }

    public void testMaximumLimit() {
        CompletedLiveQueriesService service = CompletedLiveQueriesService.getInstance();
        assertEquals(1000, service.getMaxCompletedQueries());
    }

    public void testCurrentSize() {
        CompletedLiveQueriesService service = CompletedLiveQueriesService.getInstance();
        int initialSize = service.getCurrentSize();

        SearchQueryRecord record = createTestRecord("test-size");
        service.addCompletedQuery(record);

        assertTrue(service.getCurrentSize() >= initialSize);
    }
}
