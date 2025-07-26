/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Collects system metrics for query performance analysis.
 */
public class SystemMetricsCollector {
    private static final Logger log = LogManager.getLogger(SystemMetricsCollector.class);
    
    /**
     * Collects current system metrics.
     * 
     * @return Map of system metrics
     */
    public static Map<String, Object> collectSystemMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        try {
            // OS metrics
            OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            metrics.put("system_load_avg", osBean.getSystemLoadAverage());
            metrics.put("available_processors", osBean.getAvailableProcessors());
            
            // Try to get more detailed OS metrics using reflection (for com.sun.management.OperatingSystemMXBean)
            collectDetailedOSMetrics(osBean, metrics);
            
            // JVM Memory metrics
            collectJvmMemoryMetrics(metrics);
            
            // JVM Thread metrics
            collectJvmThreadMetrics(metrics);
            
            // JVM Garbage Collection metrics
            collectJvmGcMetrics(metrics);
            
            // JVM Runtime metrics
            collectJvmRuntimeMetrics(metrics);
            
            // Try to get more detailed metrics if running on Linux
            if (new File("/proc/meminfo").exists()) {
                collectLinuxMemoryMetrics(metrics);
            }
            
            if (new File("/proc/stat").exists()) {
                collectLinuxCpuMetrics(metrics);
            }
            
            if (new File("/proc/diskstats").exists()) {
                collectLinuxDiskMetrics(metrics);
            }
            
            if (new File("/proc/pressure/cpu").exists()) {
                collectLinuxPressureMetrics(metrics);
            }
            
            if (new File("/proc/net/dev").exists()) {
                collectLinuxNetworkMetrics(metrics);
            }
            
            if (new File("/proc/loadavg").exists()) {
                collectLinuxLoadAvgMetrics(metrics);
            }
        } catch (Exception e) {
            log.debug("Error collecting system metrics: {}", e.getMessage());
        }
        
        return metrics;
    }
    
    /**
     * Collect detailed OS metrics using reflection for com.sun.management.OperatingSystemMXBean
     * which is implementation-specific but provides valuable metrics
     */
    private static void collectDetailedOSMetrics(OperatingSystemMXBean osBean, Map<String, Object> metrics) {
        try {
            // Methods to extract from the com.sun.management.OperatingSystemMXBean
            String[] methodNames = {
                "getProcessCpuLoad",
                "getSystemCpuLoad",
                "getProcessCpuTime",
                "getFreePhysicalMemorySize",
                "getTotalPhysicalMemorySize",
                "getFreeSwapSpaceSize",
                "getTotalSwapSpaceSize",
                "getCommittedVirtualMemorySize"
            };
            
            for (String methodName : methodNames) {
                try {
                    Method method = osBean.getClass().getDeclaredMethod(methodName);
                    method.setAccessible(true);
                    Object value = method.invoke(osBean);
                    if (value != null) {
                        // Convert method name to snake_case for consistency
                        String metricName = methodName.substring(3); // remove 'get'
                        metricName = metricName.substring(0, 1).toLowerCase() + metricName.substring(1);
                        StringBuilder snakeCaseName = new StringBuilder();
                        for (int i = 0; i < metricName.length(); i++) {
                            char c = metricName.charAt(i);
                            if (Character.isUpperCase(c)) {
                                snakeCaseName.append('_').append(Character.toLowerCase(c));
                            } else {
                                snakeCaseName.append(c);
                            }
                        }
                        metrics.put(snakeCaseName.toString(), value);
                    }
                } catch (Exception e) {
                    // Method not available in this JVM implementation, skip silently
                }
            }
        } catch (Exception e) {
            log.debug("Error collecting detailed OS metrics: {}", e.getMessage());
        }
    }
    
    /**
     * Collect JVM memory metrics
     */
    private static void collectJvmMemoryMetrics(Map<String, Object> metrics) {
        try {
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            
            // Heap memory usage
            MemoryUsage heapMemoryUsage = memoryBean.getHeapMemoryUsage();
            metrics.put("jvm_heap_used_bytes", heapMemoryUsage.getUsed());
            metrics.put("jvm_heap_committed_bytes", heapMemoryUsage.getCommitted());
            metrics.put("jvm_heap_max_bytes", heapMemoryUsage.getMax());
            metrics.put("jvm_heap_init_bytes", heapMemoryUsage.getInit());
            
            // Non-heap memory usage (metaspace, code cache, etc.)
            MemoryUsage nonHeapMemoryUsage = memoryBean.getNonHeapMemoryUsage();
            metrics.put("jvm_non_heap_used_bytes", nonHeapMemoryUsage.getUsed());
            metrics.put("jvm_non_heap_committed_bytes", nonHeapMemoryUsage.getCommitted());
            metrics.put("jvm_non_heap_max_bytes", nonHeapMemoryUsage.getMax());
            metrics.put("jvm_non_heap_init_bytes", nonHeapMemoryUsage.getInit());
            
            // Calculate heap usage percentage
            if (heapMemoryUsage.getMax() > 0) {
                double heapUsagePercent = (double) heapMemoryUsage.getUsed() / heapMemoryUsage.getMax() * 100.0;
                metrics.put("jvm_heap_usage_percent", heapUsagePercent);
            }
        } catch (Exception e) {
            log.debug("Error collecting JVM memory metrics: {}", e.getMessage());
        }
    }
    
    /**
     * Collect JVM thread metrics
     */
    private static void collectJvmThreadMetrics(Map<String, Object> metrics) {
        try {
            ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
            metrics.put("jvm_thread_count", threadBean.getThreadCount());
            metrics.put("jvm_daemon_thread_count", threadBean.getDaemonThreadCount());
            metrics.put("jvm_peak_thread_count", threadBean.getPeakThreadCount());
            metrics.put("jvm_total_started_thread_count", threadBean.getTotalStartedThreadCount());
            
            // Thread CPU time if supported
            if (threadBean.isThreadCpuTimeSupported() && threadBean.isThreadCpuTimeEnabled()) {
                long[] threadIds = threadBean.getAllThreadIds();
                long totalCpuTime = 0;
                long totalUserTime = 0;
                
                for (long threadId : threadIds) {
                    long cpuTime = threadBean.getThreadCpuTime(threadId);
                    long userTime = threadBean.getThreadUserTime(threadId);
                    
                    if (cpuTime != -1) {
                        totalCpuTime += cpuTime;
                    }
                    
                    if (userTime != -1) {
                        totalUserTime += userTime;
                    }
                }
                
                metrics.put("jvm_threads_total_cpu_time_nanos", totalCpuTime);
                metrics.put("jvm_threads_total_user_time_nanos", totalUserTime);
            }
        } catch (Exception e) {
            log.debug("Error collecting JVM thread metrics: {}", e.getMessage());
        }
    }
    
    /**
     * Collect JVM garbage collection metrics
     */
    private static void collectJvmGcMetrics(Map<String, Object> metrics) {
        try {
            List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
            long totalGcCollections = 0;
            long totalGcTime = 0;
            
            for (GarbageCollectorMXBean gcBean : gcBeans) {
                String name = gcBean.getName().replace(" ", "_").toLowerCase();
                long collectionCount = gcBean.getCollectionCount();
                long collectionTime = gcBean.getCollectionTime();
                
                metrics.put("jvm_gc_" + name + "_collection_count", collectionCount);
                metrics.put("jvm_gc_" + name + "_collection_time_ms", collectionTime);
                
                if (collectionCount > 0) {
                    totalGcCollections += collectionCount;
                }
                
                if (collectionTime > 0) {
                    totalGcTime += collectionTime;
                }
            }
            
            metrics.put("jvm_gc_total_collection_count", totalGcCollections);
            metrics.put("jvm_gc_total_collection_time_ms", totalGcTime);
        } catch (Exception e) {
            log.debug("Error collecting JVM GC metrics: {}", e.getMessage());
        }
    }
    
    /**
     * Collect JVM runtime metrics
     */
    private static void collectJvmRuntimeMetrics(Map<String, Object> metrics) {
        try {
            RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
            metrics.put("jvm_uptime_ms", runtimeBean.getUptime());
            metrics.put("jvm_start_time_ms", runtimeBean.getStartTime());
            
            // Get system properties that might be useful
            metrics.put("jvm_name", runtimeBean.getVmName());
            metrics.put("jvm_version", runtimeBean.getVmVersion());
            metrics.put("jvm_vendor", runtimeBean.getVmVendor());
        } catch (Exception e) {
            log.debug("Error collecting JVM runtime metrics: {}", e.getMessage());
        }
    }
    
    private static void collectLinuxMemoryMetrics(Map<String, Object> metrics) {
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/meminfo"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("MemTotal:") || 
                    line.startsWith("MemFree:") || 
                    line.startsWith("MemAvailable:") ||
                    line.startsWith("SwapTotal:") ||
                    line.startsWith("SwapFree:")) {
                    
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 2) {
                        String key = parts[0].replace(":", "").toLowerCase();
                        long value = Long.parseLong(parts[1]);
                        metrics.put(key, value);
                    }
                }
            }
        } catch (IOException e) {
            log.debug("Error reading memory metrics: {}", e.getMessage());
        }
    }
    
    private static void collectLinuxCpuMetrics(Map<String, Object> metrics) {
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/stat"))) {
            String line = reader.readLine();
            if (line != null && line.startsWith("cpu ")) {
                String[] parts = line.split("\\s+");
                if (parts.length >= 8) {
                    long user = Long.parseLong(parts[1]);
                    long nice = Long.parseLong(parts[2]);
                    long system = Long.parseLong(parts[3]);
                    long idle = Long.parseLong(parts[4]);
                    long iowait = Long.parseLong(parts[5]);
                    long irq = Long.parseLong(parts[6]);
                    long softirq = Long.parseLong(parts[7]);
                    
                    metrics.put("cpu_user", user);
                    metrics.put("cpu_nice", nice);
                    metrics.put("cpu_system", system);
                    metrics.put("cpu_idle", idle);
                    metrics.put("cpu_iowait", iowait);
                    metrics.put("cpu_irq", irq);
                    metrics.put("cpu_softirq", softirq);
                    
                    long total = user + nice + system + idle + iowait + irq + softirq;
                    double cpuUsage = 100.0 * (1.0 - (double) idle / total);
                    metrics.put("cpu_usage_percent", cpuUsage);
                }
            }
        } catch (IOException e) {
            log.debug("Error reading CPU metrics: {}", e.getMessage());
        }
    }
    
    private static void collectLinuxDiskMetrics(Map<String, Object> metrics) {
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/diskstats"))) {
            String line;
            long totalReads = 0;
            long totalWrites = 0;
            long totalReadTime = 0;
            long totalWriteTime = 0;
            
            while ((line = reader.readLine()) != null) {
                String[] parts = line.trim().split("\\s+");
                if (parts.length >= 14) {
                    // Skip partitions, only count whole disks
                    if (!Character.isDigit(parts[2].charAt(parts[2].length() - 1))) {
                        totalReads += Long.parseLong(parts[3]);
                        totalWrites += Long.parseLong(parts[7]);
                        totalReadTime += Long.parseLong(parts[6]);
                        totalWriteTime += Long.parseLong(parts[10]);
                    }
                }
            }
            
            metrics.put("disk_reads", totalReads);
            metrics.put("disk_writes", totalWrites);
            metrics.put("disk_read_time_ms", totalReadTime);
            metrics.put("disk_write_time_ms", totalWriteTime);
        } catch (IOException e) {
            log.debug("Error reading disk metrics: {}", e.getMessage());
        }
    }
    
    private static void collectLinuxPressureMetrics(Map<String, Object> metrics) {
        // CPU pressure
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/pressure/cpu"))) {
            String line = reader.readLine();
            if (line != null && line.startsWith("some")) {
                String[] parts = line.split("\\s+");
                for (String part : parts) {
                    if (part.startsWith("avg10=")) {
                        metrics.put("cpu_pressure_10s", Double.parseDouble(part.substring(6)));
                    } else if (part.startsWith("avg60=")) {
                        metrics.put("cpu_pressure_60s", Double.parseDouble(part.substring(6)));
                    } else if (part.startsWith("avg300=")) {
                        metrics.put("cpu_pressure_300s", Double.parseDouble(part.substring(7)));
                    }
                }
            }
            
            // Try to read the full line as well
            line = reader.readLine();
            if (line != null && line.startsWith("full")) {
                String[] parts = line.split("\\s+");
                for (String part : parts) {
                    if (part.startsWith("avg60=")) {
                        metrics.put("cpu_pressure_full_60s", Double.parseDouble(part.substring(6)));
                    }
                }
            }
        } catch (IOException e) {
            log.debug("Error reading CPU pressure metrics: {}", e.getMessage());
        }
        
        // Memory pressure
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/pressure/memory"))) {
            String line = reader.readLine();
            if (line != null && line.startsWith("some")) {
                String[] parts = line.split("\\s+");
                for (String part : parts) {
                    if (part.startsWith("avg10=")) {
                        metrics.put("memory_pressure_10s", Double.parseDouble(part.substring(6)));
                    } else if (part.startsWith("avg60=")) {
                        metrics.put("memory_pressure_60s", Double.parseDouble(part.substring(6)));
                    } else if (part.startsWith("avg300=")) {
                        metrics.put("memory_pressure_300s", Double.parseDouble(part.substring(7)));
                    }
                }
            }
            
            // Try to read the full line as well
            line = reader.readLine();
            if (line != null && line.startsWith("full")) {
                String[] parts = line.split("\\s+");
                for (String part : parts) {
                    if (part.startsWith("avg60=")) {
                        metrics.put("memory_pressure_full_60s", Double.parseDouble(part.substring(6)));
                    }
                }
            }
        } catch (IOException e) {
            log.debug("Error reading memory pressure metrics: {}", e.getMessage());
        }
        
        // IO pressure
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/pressure/io"))) {
            String line = reader.readLine();
            if (line != null && line.startsWith("some")) {
                String[] parts = line.split("\\s+");
                for (String part : parts) {
                    if (part.startsWith("avg10=")) {
                        metrics.put("io_pressure_10s", Double.parseDouble(part.substring(6)));
                    } else if (part.startsWith("avg60=")) {
                        metrics.put("io_pressure_60s", Double.parseDouble(part.substring(6)));
                    } else if (part.startsWith("avg300=")) {
                        metrics.put("io_pressure_300s", Double.parseDouble(part.substring(7)));
                    }
                }
            }
            
            // Try to read the full line as well
            line = reader.readLine();
            if (line != null && line.startsWith("full")) {
                String[] parts = line.split("\\s+");
                for (String part : parts) {
                    if (part.startsWith("avg60=")) {
                        metrics.put("io_pressure_full_60s", Double.parseDouble(part.substring(6)));
                    }
                }
            }
        } catch (IOException e) {
            log.debug("Error reading IO pressure metrics: {}", e.getMessage());
        }
    }
    
    /**
     * Collect Linux network metrics from /proc/net/dev
     */
    private static void collectLinuxNetworkMetrics(Map<String, Object> metrics) {
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/net/dev"))) {
            String line;
            // Skip header lines
            reader.readLine(); // Inter-|   Receive                                                |  Transmit
            reader.readLine(); // face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
            
            long totalRxBytes = 0;
            long totalTxBytes = 0;
            long totalRxPackets = 0;
            long totalTxPackets = 0;
            long totalRxErrors = 0;
            long totalTxErrors = 0;
            
            while ((line = reader.readLine()) != null) {
                String[] parts = line.trim().split(":\\s+");
                if (parts.length >= 2) {
                    String interfaceName = parts[0].trim();
                    // Skip loopback interface
                    if ("lo".equals(interfaceName)) {
                        continue;
                    }
                    
                    String[] stats = parts[1].trim().split("\\s+");
                    if (stats.length >= 16) {
                        // Receive stats
                        long rxBytes = Long.parseLong(stats[0]);
                        long rxPackets = Long.parseLong(stats[1]);
                        long rxErrors = Long.parseLong(stats[2]);
                        
                        // Transmit stats
                        long txBytes = Long.parseLong(stats[8]);
                        long txPackets = Long.parseLong(stats[9]);
                        long txErrors = Long.parseLong(stats[10]);
                        
                        totalRxBytes += rxBytes;
                        totalTxBytes += txBytes;
                        totalRxPackets += rxPackets;
                        totalTxPackets += txPackets;
                        totalRxErrors += rxErrors;
                        totalTxErrors += txErrors;
                    }
                }
            }
            
            metrics.put("network_rx_bytes", totalRxBytes);
            metrics.put("network_tx_bytes", totalTxBytes);
            metrics.put("network_rx_packets", totalRxPackets);
            metrics.put("network_tx_packets", totalTxPackets);
            metrics.put("network_rx_errors", totalRxErrors);
            metrics.put("network_tx_errors", totalTxErrors);
            metrics.put("network_total_bytes", totalRxBytes + totalTxBytes);
        } catch (IOException e) {
            log.debug("Error reading network metrics: {}", e.getMessage());
        }
    }
    
    /**
     * Collect Linux load average metrics from /proc/loadavg
     */
    private static void collectLinuxLoadAvgMetrics(Map<String, Object> metrics) {
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/loadavg"))) {
            String line = reader.readLine();
            if (line != null) {
                String[] parts = line.trim().split("\\s+");
                if (parts.length >= 5) {
                    metrics.put("load_avg_1m", Double.parseDouble(parts[0]));
                    metrics.put("load_avg_5m", Double.parseDouble(parts[1]));
                    metrics.put("load_avg_15m", Double.parseDouble(parts[2]));
                    
                    // Running/total processes
                    String[] procParts = parts[3].split("/");
                    if (procParts.length == 2) {
                        metrics.put("running_processes", Integer.parseInt(procParts[0]));
                        metrics.put("total_processes", Integer.parseInt(procParts[1]));
                    }
                }
            }
        } catch (IOException e) {
            log.debug("Error reading load average metrics: {}", e.getMessage());
        }
    }
}