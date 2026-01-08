/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.core.tasks.resourcetracker.TaskResourceStats;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.transport.TransportService;

/**
 * Helper utility for creating SearchQueryRecord from TaskInfo
 */
public final class LiveQueriesHelper {
    private static final String TOTAL = "total";

    private LiveQueriesHelper() {}

    /**
     * Create a SearchQueryRecord from TaskInfo
     */
    public static SearchQueryRecord createRecordFromTask(
        TaskInfo task,
        TransportService transportService,
        boolean includeVerboseAttributes,
        Map<String, Map<String, Object>> taskUserInfoCache
    ) {
        return createRecordFromTask(task, transportService, includeVerboseAttributes, taskUserInfoCache, null);
    }

    /**
     * Create a SearchQueryRecord from TaskInfo with child tasks
     */
    public static SearchQueryRecord createRecordFromTask(
        TaskInfo task,
        TransportService transportService,
        boolean includeVerboseAttributes,
        Map<String, Map<String, Object>> taskUserInfoCache,
        List<TaskInfo> childTasks
    ) {
        long cpuNanos = 0L;
        long memBytes = 0L;
        TaskResourceStats stats = task.getResourceStats();
        if (stats != null) {
            Map<String, TaskResourceUsage> usageInfo = stats.getResourceUsageInfo();
            if (usageInfo != null) {
                TaskResourceUsage totalUsage = usageInfo.get(TOTAL);
                if (totalUsage != null) {
                    cpuNanos = totalUsage.getCpuTimeInNanos();
                    memBytes = totalUsage.getMemoryInBytes();
                }
            }
        }

        String workloadGroup = null;

        // Get from TaskManager for local tasks
        if (transportService != null) {
            try {
                Task runningTask = null;
                if (transportService.getLocalNode().getId().equals(task.getTaskId().getNodeId())) {
                    runningTask = transportService.getTaskManager().getTask(task.getTaskId().getId());
                }

                String wlmGroupId = null;
                if (runningTask instanceof org.opensearch.wlm.WorkloadGroupTask workloadTask) {
                    wlmGroupId = workloadTask.getWorkloadGroupId();
                }
                workloadGroup = wlmGroupId;
            } catch (Exception e) {
                // Ignore
            }
        }

        Map<MetricType, Measurement> measurements = new HashMap<>();
        measurements.put(MetricType.LATENCY, new Measurement(task.getRunningTimeNanos()));
        measurements.put(MetricType.CPU, new Measurement(cpuNanos));
        measurements.put(MetricType.MEMORY, new Measurement(memBytes));

        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.NODE_ID, task.getTaskId().getNodeId());
        attributes.put(Attribute.WLM_GROUP_ID, workloadGroup);

        if (includeVerboseAttributes) {
            attributes.put(Attribute.DESCRIPTION, task.getDescription());
            attributes.put(Attribute.IS_CANCELLED, task.isCancelled());

            // Add task resource usage info
            List<org.opensearch.core.tasks.resourcetracker.TaskResourceInfo> taskResourceUsages = new ArrayList<>();
            if (stats != null) {
                TaskResourceUsage totalUsage = stats.getResourceUsageInfo() != null ? stats.getResourceUsageInfo().get(TOTAL) : null;
                taskResourceUsages.add(
                    new org.opensearch.core.tasks.resourcetracker.TaskResourceInfo(
                        task.getAction(),
                        task.getId(),
                        task.getParentTaskId().getId(),
                        task.getTaskId().getNodeId(),
                        totalUsage != null ? totalUsage : new TaskResourceUsage(0L, 0L)
                    )
                );
            }

            // Add child task resource usages
            if (childTasks != null) {
                for (TaskInfo childTask : childTasks) {
                    TaskResourceStats childStats = childTask.getResourceStats();
                    TaskResourceUsage childUsage = null;
                    if (childStats != null && childStats.getResourceUsageInfo() != null) {
                        childUsage = childStats.getResourceUsageInfo().get(TOTAL);
                    }
                    taskResourceUsages.add(
                        new org.opensearch.core.tasks.resourcetracker.TaskResourceInfo(
                            childTask.getAction(),
                            childTask.getId(),
                            childTask.getParentTaskId().getId(),
                            childTask.getTaskId().getNodeId(),
                            childUsage != null ? childUsage : new TaskResourceUsage(0L, 0L)
                        )
                    );
                }
            }

            attributes.put(Attribute.TASK_RESOURCE_USAGES, taskResourceUsages);
        }

        // Add user info if available
        if (taskUserInfoCache != null) {
            Map<String, Object> userInfo = taskUserInfoCache.get(task.getTaskId().toString());
            if (userInfo != null) {
                String username = (String) userInfo.get("username");
                if (username != null) {
                    attributes.put(Attribute.USERNAME, username);
                }
                String[] roles = (String[]) userInfo.get("roles");
                if (roles != null) {
                    attributes.put(Attribute.USER_ROLES, roles);
                }
            }
        }

        return new SearchQueryRecord(task.getStartTime(), measurements, attributes, task.getTaskId().toString());
    }

    /**
     * Process task groups and create SearchQueryRecords with aggregated child resources
     */
    public static Map<String, SearchQueryRecord> processTaskGroups(
        List<org.opensearch.action.admin.cluster.node.tasks.list.TaskGroup> taskGroups,
        TransportService transportService,
        boolean includeVerboseAttributes,
        Map<String, Map<String, Object>> taskUserInfoCache,
        String wlmGroupIdFilter,
        String taskIdFilter
    ) {
        Map<String, SearchQueryRecord> recordMap = new HashMap<>();

        for (var taskGroup : taskGroups) {
            TaskInfo parentTask = taskGroup.getTaskInfo();
            if (!parentTask.getAction().equals("indices:data/read/search")) {
                continue;
            }

            String taskId = parentTask.getTaskId().toString();
            List<TaskInfo> children = new ArrayList<>();
            collectChildTasks(taskGroup, children);

            SearchQueryRecord record = createRecordFromTask(
                parentTask,
                transportService,
                includeVerboseAttributes,
                taskUserInfoCache,
                children
            );

            // Aggregate child resources
            long cpu = ((Number) record.getMeasurement(MetricType.CPU)).longValue();
            long mem = ((Number) record.getMeasurement(MetricType.MEMORY)).longValue();

            for (TaskInfo child : children) {
                if (child.getResourceStats() != null) {
                    var usageInfo = child.getResourceStats().getResourceUsageInfo();
                    if (usageInfo != null) {
                        var totalUsage = usageInfo.get(TOTAL);
                        if (totalUsage != null) {
                            cpu += totalUsage.getCpuTimeInNanos();
                            mem += totalUsage.getMemoryInBytes();
                        }
                    }
                }
            }

            record.getMeasurements().put(MetricType.CPU, new Measurement(cpu));
            record.getMeasurements().put(MetricType.MEMORY, new Measurement(mem));

            // Apply filters
            if (wlmGroupIdFilter != null
                && !wlmGroupIdFilter.equals(
                    record.getAttributes().get(org.opensearch.plugin.insights.rules.model.Attribute.WLM_GROUP_ID)
                )) {
                continue;
            }
            if (taskIdFilter != null && !taskIdFilter.equals(taskId)) {
                continue;
            }

            recordMap.put(taskId, record);
        }

        return recordMap;
    }

    private static void collectChildTasks(
        org.opensearch.action.admin.cluster.node.tasks.list.TaskGroup taskGroup,
        List<TaskInfo> children
    ) {
        for (var child : taskGroup.getChildTasks()) {
            children.add(child.getTaskInfo());
            collectChildTasks(child, children);
        }
    }
}
