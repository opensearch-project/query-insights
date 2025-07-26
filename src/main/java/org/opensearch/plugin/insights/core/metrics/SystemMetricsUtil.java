/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.metrics;

import java.util.Map;
import java.util.TreeMap;

/**
 * Utility class for displaying and working with system metrics
 */
public class SystemMetricsUtil {

    /**
     * Print all system metrics in a formatted way
     */
    public static void printSystemMetrics() {
        Map<String, Object> metrics = SystemMetricsCollector.collectSystemMetrics();
        
        // Sort metrics by name for easier reading
        Map<String, Object> sortedMetrics = new TreeMap<>(metrics);
        
        System.out.println("=== System Metrics ===");
        System.out.println("Total metrics collected: " + sortedMetrics.size());
        
        // Group metrics by prefix for better organization
        Map<String, StringBuilder> groups = new TreeMap<>();
        
        for (Map.Entry<String, Object> entry : sortedMetrics.entrySet()) {
            String key = entry.getKey();
            String prefix = getMetricPrefix(key);
            
            StringBuilder groupContent = groups.computeIfAbsent(prefix, k -> new StringBuilder());
            groupContent.append(String.format("  %-40s : %s%n", key, formatMetricValue(entry.getValue())));
        }
        
        // Print each group
        for (Map.Entry<String, StringBuilder> group : groups.entrySet()) {
            System.out.println("--- " + group.getKey() + " ---");
            System.out.print(group.getValue().toString());
        }
    }
    
    /**
     * Get the prefix category for a metric name
     */
    private static String getMetricPrefix(String metricName) {
        if (metricName.startsWith("jvm_")) {
            if (metricName.contains("_gc_")) return "JVM - Garbage Collection";
            if (metricName.contains("_heap_")) return "JVM - Heap Memory";
            if (metricName.contains("_non_heap_")) return "JVM - Non-Heap Memory";
            if (metricName.contains("_thread_")) return "JVM - Threads";
            return "JVM - Other";
        } else if (metricName.startsWith("cpu_")) {
            return "CPU";
        } else if (metricName.startsWith("memory_") || metricName.contains("mem")) {
            return "Memory";
        } else if (metricName.startsWith("disk_")) {
            return "Disk";
        } else if (metricName.startsWith("network_")) {
            return "Network";
        } else if (metricName.startsWith("io_")) {
            return "I/O";
        } else if (metricName.startsWith("load_") || metricName.contains("load")) {
            return "System Load";
        } else if (metricName.startsWith("system_")) {
            return "System";
        } else if (metricName.startsWith("process_")) {
            return "Process";
        }
        
        return "Other";
    }
    
    /**
     * Format a metric value for display
     */
    private static String formatMetricValue(Object value) {
        if (value == null) {
            return "null";
        }
        
        if (value instanceof Double) {
            Double doubleValue = (Double) value;
            if (doubleValue == (long) doubleValue.doubleValue()) {
                return String.format("%d", doubleValue.longValue());
            } else {
                return String.format("%.4f", doubleValue);
            }
        }
        
        if (value instanceof Long) {
            long longValue = (Long) value;
            // Format byte values in a human-readable way
            if (longValue > 1024 && String.valueOf(value).length() > 6) {
                if (longValue > 1024 * 1024 * 1024) {
                    return String.format("%.2f GB (%d)", longValue / (1024.0 * 1024.0 * 1024.0), longValue);
                } else if (longValue > 1024 * 1024) {
                    return String.format("%.2f MB (%d)", longValue / (1024.0 * 1024.0), longValue);
                } else {
                    return String.format("%.2f KB (%d)", longValue / 1024.0, longValue);
                }
            }
        }
        
        return value.toString();
    }
    
    /**
     * Main method for testing
     */
    public static void main(String[] args) {
        printSystemMetrics();
    }
}