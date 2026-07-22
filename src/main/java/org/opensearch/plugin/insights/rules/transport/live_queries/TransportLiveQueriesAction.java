/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.live_queries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.resourcetracker.TaskResourceStats;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.plugin.insights.core.auth.TopQueriesRbacFilter;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext.UserPrincipalInfo;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.live_queries.FinishedQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.FinishedQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesResponse;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesUserInfoAction;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueriesUserInfoRequest;
import org.opensearch.plugin.insights.rules.action.live_queries.LiveQueryUserInfoDTO;
import org.opensearch.plugin.insights.rules.model.FilterByMode;
import org.opensearch.plugin.insights.rules.model.FinishedQueryRecord;
import org.opensearch.plugin.insights.rules.model.LiveQueryRecord;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.TaskDetails;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Transport action for fetching ongoing live queries
 */
public class TransportLiveQueriesAction extends HandledTransportAction<LiveQueriesRequest, LiveQueriesResponse> {

    private static final Logger logger = LogManager.getLogger(TransportLiveQueriesAction.class);
    private static final String TOTAL = "total";

    private final Client client;
    private final TransportService transportService;
    private final QueryInsightsService queryInsightsService;

    @Inject
    public TransportLiveQueriesAction(
        final TransportService transportService,
        final Client client,
        final ActionFilters actionFilters,
        final QueryInsightsService queryInsightsService
    ) {
        super(LiveQueriesAction.NAME, transportService, actionFilters, LiveQueriesRequest::new, ThreadPool.Names.GENERIC);
        this.transportService = transportService;
        this.client = client;
        this.queryInsightsService = queryInsightsService;
        queryInsightsService.setTaskManager(transportService.getTaskManager());
    }

    @Override
    protected void doExecute(final Task task, final LiveQueriesRequest request, final ActionListener<LiveQueriesResponse> listener) {
        ListTasksRequest listTasksRequest = new ListTasksRequest().setDetailed(request.isVerbose()).setActions("indices:data/read/search*");

        // Set nodes filter if provided in the request
        String[] requestedNodeIds = request.nodesIds();
        if (requestedNodeIds != null && requestedNodeIds.length > 0) {
            listTasksRequest.setNodes(requestedNodeIds);
        }

        // Execute tasks request asynchronously to avoid blocking
        client.admin().cluster().listTasks(listTasksRequest, new ActionListener<ListTasksResponse>() {
            @Override
            public void onResponse(ListTasksResponse taskResponse) {
                try {
                    List<LiveQueryRecord> allRecords = new ArrayList<>();

                    for (TaskGroup taskGroup : taskResponse.getTaskGroups()) {
                        TaskInfo coordinatorInfo = taskGroup.getTaskInfo();
                        String action = coordinatorInfo.getAction();

                        if (!action.equals("indices:data/read/search")) {
                            continue;
                        }

                        // Build the query id with the shared key helper so it matches the key the
                        // listener stores user info under (nodeId:taskId) and can't drift from
                        // TaskId.toString()'s internal format.
                        String queryId = QueryInsightsService.buildLiveQueryTaskKey(
                            coordinatorInfo.getTaskId().getNodeId(),
                            coordinatorInfo.getTaskId().getId()
                        );

                        // Get WLM group ID
                        String wlmGroupId = null;
                        Task runningTask = null;
                        if (transportService.getLocalNode().getId().equals(coordinatorInfo.getTaskId().getNodeId())) {
                            runningTask = transportService.getTaskManager().getTask(coordinatorInfo.getTaskId().getId());
                        }
                        if (runningTask instanceof org.opensearch.wlm.WorkloadGroupTask workloadTask) {
                            wlmGroupId = workloadTask.getWorkloadGroupId();
                        }

                        String targetWlmGroupId = request.getWlmGroupId();
                        if (targetWlmGroupId != null && !targetWlmGroupId.equals(wlmGroupId)) {
                            continue;
                        }

                        // Build coordinator task
                        TaskResourceStats coordStats = coordinatorInfo.getResourceStats();
                        long coordCpu = 0L;
                        long coordMem = 0L;
                        if (coordStats != null) {
                            Map<String, TaskResourceUsage> usageInfo = coordStats.getResourceUsageInfo();
                            if (usageInfo != null) {
                                TaskResourceUsage totalUsage = usageInfo.get(TOTAL);
                                if (totalUsage != null) {
                                    coordCpu = totalUsage.getCpuTimeInNanos();
                                    coordMem = totalUsage.getMemoryInBytes();
                                }
                            }
                        }

                        // Build shard tasks (recursively collect all descendants)
                        List<TaskDetails> shardTasks = new ArrayList<>();
                        collectChildTasks(taskGroup, shardTasks);

                        long totalCpu = coordCpu + shardTasks.stream().mapToLong(t -> {
                            Map<String, TaskResourceUsage> u = t.getTaskInfo().getResourceStats() != null
                                ? t.getTaskInfo().getResourceStats().getResourceUsageInfo()
                                : null;
                            return (u != null && u.get(TOTAL) != null) ? u.get(TOTAL).getCpuTimeInNanos() : 0L;
                        }).sum();
                        long totalMem = coordMem + shardTasks.stream().mapToLong(t -> {
                            Map<String, TaskResourceUsage> u = t.getTaskInfo().getResourceStats() != null
                                ? t.getTaskInfo().getResourceStats().getResourceUsageInfo()
                                : null;
                            return (u != null && u.get(TOTAL) != null) ? u.get(TOTAL).getMemoryInBytes() : 0L;
                        }).sum();

                        // Determine status based on coordinator cancellation
                        String queryStatus = coordinatorInfo.isCancelled() ? "cancelled" : "running";

                        // User info is resolved after the fan-out below; build the record with the
                        // task key so it can be enriched once per-node identity comes back.
                        LiveQueryRecord record = new LiveQueryRecord(
                            queryId,
                            queryStatus,
                            coordinatorInfo.getStartTime(),
                            wlmGroupId,
                            TimeUnit.NANOSECONDS.toMillis(coordinatorInfo.getRunningTimeNanos()),
                            totalCpu,
                            totalMem,
                            new TaskDetails(coordinatorInfo, queryStatus),
                            shardTasks,
                            null,
                            List.of(),
                            List.of()
                        );

                        allRecords.add(record);
                    }

                    List<LiveQueryRecord> sortedRecords = allRecords.stream().sorted((a, b) -> {
                        switch (request.getSortBy()) {
                            case CPU:
                                return Long.compare(b.getTotalCpu(), a.getTotalCpu());
                            case MEMORY:
                                return Long.compare(b.getTotalMemory(), a.getTotalMemory());
                            default:
                                return Long.compare(b.getTotalLatency(), a.getTotalLatency());
                        }
                    }).limit(request.getSize() < 0 ? Long.MAX_VALUE : request.getSize()).toList();

                    // Fan out to all nodes to resolve user identity captured on the coordinating node of
                    // each search, then continue with finished-queries handling (if requested).
                    resolveUserInfoAndRespond(sortedRecords, request, listener);
                } catch (Exception ex) {
                    logger.error("Failed to process live queries response", ex);
                    listener.onFailure(ex);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to retrieve live queries", e);
                listener.onFailure(e);
            }
        });
    }

    /**
     * Fans out to all nodes to resolve user identity for the given live query records (each record's
     * id is the {@code nodeId:taskId} key used by the listener), enriches the records, and then
     * completes the response — appending finished queries if the request asked for them.
     */
    private void resolveUserInfoAndRespond(
        final List<LiveQueryRecord> records,
        final LiveQueriesRequest request,
        final ActionListener<LiveQueriesResponse> listener
    ) {
        // No live queries — skip the cluster-wide user info fan-out entirely.
        if (records.isEmpty()) {
            respondWithFinishedQueries(records, request, listener);
            return;
        }

        List<String> taskKeys = records.stream().map(LiveQueryRecord::getQueryId).toList();

        // A task key is "nodeId:taskId"; user info for a task lives only on the node that
        // coordinated it. Target the fan-out at just those owning nodes instead of the whole
        // cluster (the identity was captured there, so other nodes have nothing to contribute).
        // Issue 3: Use indexOf(':') since taskId is always numeric (the first ':' separates
        // nodeId from taskId), matching the format produced by buildLiveQueryTaskKey().
        String[] targetNodeIds = taskKeys.stream().map(key -> {
            int sep = key.indexOf(':');
            return sep > 0 ? key.substring(0, sep) : key;
        }).distinct().toArray(String[]::new);

        client.execute(
            LiveQueriesUserInfoAction.INSTANCE,
            new LiveQueriesUserInfoRequest(taskKeys, targetNodeIds),
            ActionListener.wrap(userInfoResponse -> {
                List<LiveQueryRecord> enrichedRecords = enrichWithUserInfo(records, userInfoResponse.getAllUserInfo());
                List<LiveQueryRecord> filteredRecords = filterLiveRecordsByMode(enrichedRecords);
                respondWithFinishedQueries(filteredRecords, request, listener);
            }, ex -> {
                // User info is best-effort — on failure, fall back to records without identity
                logger.error("Failed to resolve live query user info from nodes", ex);
                List<LiveQueryRecord> filteredRecords = filterLiveRecordsByMode(records);
                respondWithFinishedQueries(filteredRecords, request, listener);
            })
        );
    }

    private List<LiveQueryRecord> enrichWithUserInfo(
        final List<LiveQueryRecord> records,
        final Map<String, LiveQueryUserInfoDTO> userInfoByTaskKey
    ) {
        if (userInfoByTaskKey.isEmpty()) {
            return records;
        }
        List<LiveQueryRecord> enriched = new ArrayList<>(records.size());
        for (LiveQueryRecord record : records) {
            LiveQueryUserInfoDTO userInfo = userInfoByTaskKey.get(record.getQueryId());
            if (userInfo == null) {
                enriched.add(record);
                continue;
            }
            enriched.add(
                new LiveQueryRecord(
                    record.getQueryId(),
                    record.getStatus(),
                    record.getStartTime(),
                    record.getWlmGroupId(),
                    record.getTotalLatency(),
                    record.getTotalCpu(),
                    record.getTotalMemory(),
                    record.getCoordinatorTask(),
                    record.getShardTasks(),
                    userInfo.getUsername(),
                    userInfo.getRoles(),
                    userInfo.getBackendRoles()
                )
            );
        }
        return enriched;
    }

    private void respondWithFinishedQueries(
        final List<LiveQueryRecord> finalRecords,
        final LiveQueriesRequest request,
        final ActionListener<LiveQueriesResponse> listener
    ) {
        if (request.isUseFinishedCache()) {
            // Touch the local cache on the coordinating node to schedule the idle-check
            // timer and update lastAccessTime. Fan-out nodes use getFinishedQueriesIfActive()
            // to avoid activating caches cluster-wide.
            queryInsightsService.getFinishedQueriesCache().getFinishedQueries();

            client.execute(
                FinishedQueriesAction.INSTANCE,
                new FinishedQueriesRequest(request.nodesIds()),
                ActionListener.wrap(finishedResponse -> {
                    List<FinishedQueryRecord> finishedRecords = sortAndLimit(finishedResponse.getAllFinishedQueries(), request);
                    finishedRecords = filterFinishedRecordsByMode(finishedRecords);
                    listener.onResponse(new LiveQueriesResponse(finalRecords, finishedRecords, true));
                }, ex -> {
                    logger.error("Failed to retrieve finished queries from nodes", ex);
                    listener.onFailure(ex);
                })
            );
        } else {
            listener.onResponse(new LiveQueriesResponse(finalRecords, List.of(), false));
        }
    }

    private void collectChildTasks(TaskGroup group, List<TaskDetails> result) {
        for (TaskGroup child : group.getChildTasks()) {
            TaskInfo info = child.getTaskInfo();
            result.add(new TaskDetails(info, info.isCancelled() ? "cancelled" : "running"));
            collectChildTasks(child, result);
        }
    }

    private List<FinishedQueryRecord> sortAndLimit(List<FinishedQueryRecord> records, LiveQueriesRequest request) {
        return records.stream().sorted((a, b) -> {
            Measurement ma = a.getMeasurements().get(request.getSortBy());
            Measurement mb = b.getMeasurements().get(request.getSortBy());
            if (ma == null && mb == null) return 0;
            if (ma == null) return 1;
            if (mb == null) return -1;
            return Double.compare(mb.getMeasurement().doubleValue(), ma.getMeasurement().doubleValue());
        }).limit(request.getSize() < 0 ? Long.MAX_VALUE : request.getSize()).toList();
    }

    /**
     * Applies RBAC filtering to finished query records based on filter_by_mode.
     * Finished records carry username/roles (stamped in addToFinishedCache), so the same
     * access control must apply as for live records.
     */
    @SuppressWarnings("unchecked")
    private List<FinishedQueryRecord> filterFinishedRecordsByMode(List<FinishedQueryRecord> records) {
        FilterByMode mode = queryInsightsService.getFilterByMode();
        if (mode == null || mode == FilterByMode.NONE) {
            return records;
        }
        UserPrincipalInfo requestingUser;
        try {
            requestingUser = new UserPrincipalContext(transportService.getThreadPool()).extractUserInfo();
        } catch (Exception e) {
            return Collections.emptyList();
        }
        if (requestingUser == null) {
            return Collections.emptyList();
        }
        if (TopQueriesRbacFilter.isAdmin(requestingUser)) {
            return records;
        }
        List<SearchQueryRecord> filtered = TopQueriesRbacFilter.filterRecords(
            (List<SearchQueryRecord>) (List<?>) records,
            mode,
            requestingUser
        );
        return (List<FinishedQueryRecord>) (List<?>) filtered;
    }

    /**
     * Applies RBAC filtering to live query records based on the configured filter_by_mode,
     * mirroring the same access control that top queries enforces.
     */
    private List<LiveQueryRecord> filterLiveRecordsByMode(List<LiveQueryRecord> records) {
        FilterByMode mode = queryInsightsService.getFilterByMode();
        if (mode == null || mode == FilterByMode.NONE) {
            return records;
        }

        UserPrincipalInfo requestingUser;
        try {
            requestingUser = new UserPrincipalContext(transportService.getThreadPool()).extractUserInfo();
        } catch (Exception e) {
            logger.warn("Failed to extract user info for live queries RBAC filtering", e);
            return Collections.emptyList();
        }

        if (requestingUser == null) {
            return Collections.emptyList();
        }

        if (TopQueriesRbacFilter.isAdmin(requestingUser)) {
            return records;
        }

        switch (mode) {
            case USERNAME:
                if (requestingUser.getUserName() == null) {
                    return Collections.emptyList();
                }
                return records.stream().filter(r -> requestingUser.getUserName().equals(r.getUsername())).collect(Collectors.toList());
            case BACKEND_ROLES:
                if (requestingUser.getBackendRoles() == null || requestingUser.getBackendRoles().isEmpty()) {
                    return Collections.emptyList();
                }
                Set<String> myRoles = new HashSet<>(requestingUser.getBackendRoles());
                return records.stream()
                    .filter(r -> r.getBackendRoles() != null && !Collections.disjoint(myRoles, new HashSet<>(r.getBackendRoles())))
                    .collect(Collectors.toList());
            default:
                return records;
        }
    }
}
