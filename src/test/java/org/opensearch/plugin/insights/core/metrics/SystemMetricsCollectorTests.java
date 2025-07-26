/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.metrics;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SystemMetricsCollectorTests {

    @Test
    public void testCollectSystemMetrics() {
        Map<String, Object> metrics = SystemMetricsCollector.collectSystemMetrics();
        
        // Verify basic metrics are present
        assertNotNull("System metrics should not be null", metrics);
        assertTrue("System metrics should contain system_load_avg", metrics.containsKey("system_load_avg"));
        assertTrue("System metrics should contain available_processors", metrics.containsKey("available_processors"));
        
        // Verify JVM memory metrics
        assertTrue("JVM heap metrics should be present", 
            metrics.containsKey("jvm_heap_used_bytes") || 
            metrics.containsKey("jvm_heap_committed_bytes"));
        
        // Verify JVM thread metrics
        assertTrue("JVM thread metrics should be present", 
            metrics.containsKey("jvm_thread_count") || 
            metrics.containsKey("jvm_daemon_thread_count"));
        
        // Verify JVM GC metrics
        assertTrue("JVM GC metrics should be present", 
            metrics.containsKey("jvm_gc_total_collection_count") || 
            metrics.containsKey("jvm_gc_total_collection_time_ms"));
        
        // Verify JVM runtime metrics
        assertTrue("JVM runtime metrics should be present", 
            metrics.containsKey("jvm_uptime_ms") || 
            metrics.containsKey("jvm_name"));
        
        // Print all collected metrics for inspection
        System.out.println("Collected System Metrics:");
        metrics.forEach((key, value) -> System.out.println(key + " = " + value));
    }
}