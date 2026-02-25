/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.HashMap;
import org.opensearch.plugin.insights.rules.model.FinishedQueryRecord;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;

public class FinishedQueriesCacheTimeoutTests extends OpenSearchTestCase {

    private FinishedQueryRecord createRecord(String id) {
        var measurements = new HashMap<MetricType, Measurement>();
        measurements.put(MetricType.LATENCY, new Measurement(100L));
        measurements.put(MetricType.CPU, new Measurement(200L));
        measurements.put(MetricType.MEMORY, new Measurement(300L));
        SearchQueryRecord base = new SearchQueryRecord(System.currentTimeMillis(), measurements, new HashMap<>(), id);
        return new FinishedQueryRecord(base, "topn-uuid", "completed", "nodeId:51");
    }

    public void testFinishedQueryRecordHasTopNId() {
        FinishedQueryRecord record = createRecord("nodeId:51");
        assertEquals("nodeId:51", record.getId());
        assertEquals("topn-uuid", record.getTopNId());
        assertEquals("completed", record.getStatus());
    }

    public void testFinishedQueryRecordStatuses() {
        var measurements = new HashMap<MetricType, Measurement>();
        measurements.put(MetricType.LATENCY, new Measurement(100L));
        measurements.put(MetricType.CPU, new Measurement(200L));
        measurements.put(MetricType.MEMORY, new Measurement(300L));
        SearchQueryRecord base = new SearchQueryRecord(System.currentTimeMillis(), measurements, new HashMap<>(), "id");

        assertEquals("failed", new FinishedQueryRecord(base, null, "failed", "id:1").getStatus());
        assertEquals("cancelled", new FinishedQueryRecord(base, null, "cancelled", "id:1").getStatus());
        assertEquals("completed", new FinishedQueryRecord(base, null, "completed", "id:1").getStatus());
    }
}
