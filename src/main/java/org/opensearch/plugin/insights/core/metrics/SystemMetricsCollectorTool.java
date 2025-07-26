/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.metrics;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Command-line tool for testing system metrics collection
 */
public class SystemMetricsCollectorTool {

    private static final String OUTPUT_FILE = "system_metrics_samples.json";
    private static final int SAMPLE_INTERVAL_SECONDS = 5;
    private static final int TOTAL_SAMPLES = 12; // 1 minute of samples at 5-second intervals
    
    public static void main(String[] args) {
        System.out.println("Starting System Metrics Collector Tool");
        System.out.println("Collecting " + TOTAL_SAMPLES + " samples at " + SAMPLE_INTERVAL_SECONDS + "-second intervals");
        System.out.println("Output will be written to " + OUTPUT_FILE);
        
        // Print initial metrics
        System.out.println("\nInitial metrics sample:");
        SystemMetricsUtil.printSystemMetrics();
        
        // Schedule periodic collection
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final int[] sampleCount = {0};
        
        try (FileWriter writer = new FileWriter(OUTPUT_FILE)) {
            writer.write("[\n");
            
            Runnable task = () -> {
                try {
                    sampleCount[0]++;
                    System.out.println("\nCollecting sample " + sampleCount[0] + " of " + TOTAL_SAMPLES);
                    
                    Map<String, Object> metrics = SystemMetricsCollector.collectSystemMetrics();
                    String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
                    
                    // Create JSON object with timestamp and metrics
                    StringBuilder json = new StringBuilder();
                    json.append("  {\n");
                    json.append("    \"timestamp\": \"").append(timestamp).append("\",\n");
                    json.append("    \"metrics\": {\n");
                    
                    boolean first = true;
                    for (Map.Entry<String, Object> entry : metrics.entrySet()) {
                        if (!first) {
                            json.append(",\n");
                        }
                        first = false;
                        
                        json.append("      \"").append(entry.getKey()).append("\": ");
                        Object value = entry.getValue();
                        if (value == null) {
                            json.append("null");
                        } else if (value instanceof Number || value instanceof Boolean) {
                            json.append(value);
                        } else {
                            json.append("\"").append(value).append("\"");
                        }
                    }
                    
                    json.append("\n    }\n  }");
                    
                    // Add comma if not the last sample
                    if (sampleCount[0] < TOTAL_SAMPLES) {
                        json.append(",");
                    }
                    json.append("\n");
                    
                    writer.write(json.toString());
                    writer.flush();
                    
                    // If we've collected all samples, shut down
                    if (sampleCount[0] >= TOTAL_SAMPLES) {
                        writer.write("]\n");
                        writer.flush();
                        scheduler.shutdown();
                        System.out.println("\nCollection complete. Results written to " + OUTPUT_FILE);
                    }
                } catch (Exception e) {
                    System.err.println("Error collecting metrics: " + e.getMessage());
                    e.printStackTrace();
                }
            };
            
            // Schedule the task to run periodically
            scheduler.scheduleAtFixedRate(task, 0, SAMPLE_INTERVAL_SECONDS, TimeUnit.SECONDS);
            
            // Wait for all tasks to complete
            try {
                scheduler.awaitTermination(TOTAL_SAMPLES * SAMPLE_INTERVAL_SECONDS + 10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                System.err.println("Collection interrupted: " + e.getMessage());
            }
            
        } catch (IOException e) {
            System.err.println("Error writing to output file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}