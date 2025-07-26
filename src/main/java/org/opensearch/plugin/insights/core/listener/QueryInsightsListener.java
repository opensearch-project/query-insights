/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.listener;

import static org.opensearch.plugin.insights.rules.model.SearchQueryRecord.DEFAULT_TOP_N_QUERY_MAP;
import static org.opensearch.plugin.insights.settings.QueryCategorizationSettings.SEARCH_QUERY_METRICS_ENABLED_SETTING;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_GROUPING_FIELD_NAME;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_GROUPING_FIELD_TYPE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_GROUP_BY;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getTopNEnabledSetting;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getTopNSizeSetting;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getTopNWindowSizeSetting;

// Added
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
// Added
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.ClusterState;
//import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.index.Index;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.metrics.SystemMetricsCollector;
import org.opensearch.plugin.insights.core.analysis.QueryStructureAnalyzer;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import reactor.util.annotation.NonNull;

/**
 * The listener for query insights services.
 * It forwards query-related data to the appropriate query insights stores,
 * either for each request or for each phase.
 */
public final class QueryInsightsListener extends SearchRequestOperationsListener {

    private static final Logger log = LogManager.getLogger(QueryInsightsListener.class);

    private final QueryInsightsService queryInsightsService;
    private final ClusterService clusterService;
    private final Client client;
    private boolean groupingFieldNameEnabled;
    private boolean groupingFieldTypeEnabled;
    private final QueryShapeGenerator queryShapeGenerator;


    // Added:
    // For Query Export to directory
    //private final String queryExportFilePath;

    // For writing to the same file to aggregate the workload data:
    private final String queryExportFilePath = "/hdd1/mnt/env/root/apollo/env/swift-us-east-1-integ-OS_2_19AMI-ES2-p001/var/es/data/SystemAndQueryMetrics.json";

    private final List<SearchQueryRecord> queryBuffer = new ArrayList<>();
    private static final int BATCH_SIZE = 1;

    // Cache for document counts by index key
    private final Map<String, Long> docCountCache = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Constructor for QueryInsightsListener
     *
     * @param clusterService       The Node's cluster service.
     * @param queryInsightsService The topQueriesByLatencyService associated with this listener
     */
    @Inject
    public QueryInsightsListener(final ClusterService clusterService, final QueryInsightsService queryInsightsService) {
        this(clusterService, queryInsightsService, null, false);
        groupingFieldNameEnabled = false;
        groupingFieldTypeEnabled = false;
    }

    /**
     * Constructor for QueryInsightsListener
     *
     * @param clusterService       The Node's cluster service.
     * @param queryInsightsService The topQueriesByLatencyService associated with this listener
     * @param initiallyEnabled Is the listener initially enabled/disabled
     */
    public QueryInsightsListener(
        final ClusterService clusterService,
        final QueryInsightsService queryInsightsService,
        final Client client,
        boolean initiallyEnabled
    ) {
        super(initiallyEnabled);
        this.clusterService = clusterService;
        this.queryInsightsService = queryInsightsService;
        this.client = client;
        this.queryShapeGenerator = new QueryShapeGenerator(clusterService);
        queryInsightsService.setQueryShapeGenerator(queryShapeGenerator);

        // Generate unique filename with readable timestamp
        //this.queryExportFilePath = "BenchmarkOutputs/queryMetrics_" +
        //    DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss").withZone(ZoneOffset.UTC).format(Instant.now()) + ".json";

        // Setting endpoints set up for top n queries, including enabling top n queries, window size, and top n size
        // Expected metricTypes are Latency, CPU, and Memory.
        for (MetricType type : MetricType.allMetricTypes()) {
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(getTopNEnabledSetting(type), v -> this.setEnableTopQueries(type, v));
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(
                    getTopNSizeSetting(type),
                    v -> this.queryInsightsService.setTopNSize(type, v),
                    v -> this.queryInsightsService.validateTopNSize(type, v)
                );
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(
                    getTopNWindowSizeSetting(type),
                    v -> this.queryInsightsService.setWindowSize(type, v),
                    v -> this.queryInsightsService.validateWindowSize(type, v)
                );

            this.setEnableTopQueries(type, clusterService.getClusterSettings().get(getTopNEnabledSetting(type)));
            this.queryInsightsService.validateTopNSize(type, clusterService.getClusterSettings().get(getTopNSizeSetting(type)));
            this.queryInsightsService.setTopNSize(type, clusterService.getClusterSettings().get(getTopNSizeSetting(type)));
            this.queryInsightsService.validateWindowSize(type, clusterService.getClusterSettings().get(getTopNWindowSizeSetting(type)));
            this.queryInsightsService.setWindowSize(type, clusterService.getClusterSettings().get(getTopNWindowSizeSetting(type)));
        }

        // Settings endpoints set for grouping top n queries
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_QUERIES_GROUP_BY,
                v -> this.queryInsightsService.setGrouping(v),
                v -> this.queryInsightsService.validateGrouping(v)
            );
        this.queryInsightsService.validateGrouping(clusterService.getClusterSettings().get(TOP_N_QUERIES_GROUP_BY));
        this.queryInsightsService.setGrouping(clusterService.getClusterSettings().get(TOP_N_QUERIES_GROUP_BY));

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N,
                v -> this.queryInsightsService.setMaximumGroups(v),
                v -> this.queryInsightsService.validateMaximumGroups(v)
            );
        this.queryInsightsService.validateMaximumGroups(clusterService.getClusterSettings().get(TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N));
        this.queryInsightsService.setMaximumGroups(clusterService.getClusterSettings().get(TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N));

        // Internal settings for grouping attributes
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TOP_N_QUERIES_GROUPING_FIELD_NAME, this::setGroupingFieldNameEnabled);
        setGroupingFieldNameEnabled(clusterService.getClusterSettings().get(TOP_N_QUERIES_GROUPING_FIELD_NAME));

        clusterService.getClusterSettings().addSettingsUpdateConsumer(TOP_N_QUERIES_GROUPING_FIELD_TYPE, this::setGroupingFieldTypeEnabled);
        setGroupingFieldTypeEnabled(clusterService.getClusterSettings().get(TOP_N_QUERIES_GROUPING_FIELD_TYPE));

        // Settings endpoints set for search query metrics
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SEARCH_QUERY_METRICS_ENABLED_SETTING, this::setSearchQueryMetricsEnabled);
        setSearchQueryMetricsEnabled(clusterService.getClusterSettings().get(SEARCH_QUERY_METRICS_ENABLED_SETTING));
    }

    /**
     * Enable or disable top queries insights collection for {@link MetricType}.
     * This function will enable or disable the corresponding listeners
     * and query insights services.
     *
     * @param metricType {@link MetricType}
     * @param isCurrentMetricEnabled boolean
     */
    public void setEnableTopQueries(final MetricType metricType, final boolean isCurrentMetricEnabled) {
        this.queryInsightsService.enableCollection(metricType, isCurrentMetricEnabled);
        updateQueryInsightsState();
    }

    /**
     * Set search query metrics enabled to enable collection of search query categorization metrics.
     * @param searchQueryMetricsEnabled boolean flag
     */
    public void setSearchQueryMetricsEnabled(boolean searchQueryMetricsEnabled) {
        this.queryInsightsService.enableSearchQueryMetricsFeature(searchQueryMetricsEnabled);
        updateQueryInsightsState();
    }

    public void setGroupingFieldNameEnabled(Boolean fieldNameEnabled) {
        this.groupingFieldNameEnabled = fieldNameEnabled;
    }

    public void setGroupingFieldTypeEnabled(Boolean fieldTypeEnabled) {
        this.groupingFieldTypeEnabled = fieldTypeEnabled;
    }

    /**
     * Update the query insights service state based on the enabled features.
     * If any feature is enabled, it starts the service. If no features are enabled, it stops the service.
     */
    private void updateQueryInsightsState() {
        boolean anyFeatureEnabled = queryInsightsService.isAnyFeatureEnabled();

        if (anyFeatureEnabled && !super.isEnabled()) {
            super.setEnabled(true);
            queryInsightsService.stop(); // Ensures a clean restart
            queryInsightsService.start();
        } else if (!anyFeatureEnabled && super.isEnabled()) {
            super.setEnabled(false);
            queryInsightsService.stop();
        }
    }

    @Override
    public boolean isEnabled() {
        return super.isEnabled();
    }

    @Override
    public void onPhaseStart(SearchPhaseContext context) {}

    @Override
    public void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}

    @Override
    public void onPhaseFailure(SearchPhaseContext context, Throwable cause) {}

    @Override
    public void onRequestStart(SearchRequestContext searchRequestContext) {}

    @Override
    public void onRequestEnd(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        constructSearchQueryRecord(context, searchRequestContext);
    }

    @Override
    public void onRequestFailure(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        constructSearchQueryRecord(context, searchRequestContext);
    }

    private boolean skipSearchRequest(final SearchRequestContext searchRequestContext) {
        // Check if this is a benchmark query we want to capture
        SearchRequest request = searchRequestContext.getRequest();
        if (request != null && request.source() != null) {
            String sourceStr = request.source().toString().toLowerCase();
            // Don't skip benchmark queries
            if (sourceStr.contains("name.raw") || sourceStr.contains("feature_class") ||
                sourceStr.contains("nested") || sourceStr.contains("join_field") ||
                sourceStr.contains("answer") || sourceStr.contains("comment") ||
                sourceStr.contains("question") || sourceStr.contains("tag") ||
                sourceStr.contains("event_type") || sourceStr.contains("sonested")) {
                log.debug("Not skipping benchmark query: {}",
                          sourceStr.substring(0, Math.min(100, sourceStr.length())) + "...");
                return false;
            }
        }

        boolean shouldSkip = Optional.ofNullable(searchRequestContext)
            .map(SearchRequestContext::getSuccessfulSearchShardIndices)
            .map(indices -> {
                String indexNames = indices.stream().map(Index::getName).collect(Collectors.joining(","));
                log.info("Checking indices: {}", indexNames);
                return indices.stream().map(Index::getName).anyMatch(this::matchedExcludedIndices);
            })
            .orElse(false);

        if (shouldSkip) {
            log.info("Skipping excluded indices");
        }
        return shouldSkip;
    }

    private boolean matchedExcludedIndices(String indexName) {
        return false;
    }

    // Added:
    /**
     * Construct SearchQueryRecord from search context and request context
     *
     * @param context SearchPhaseContext containing search phase information
     * @param searchRequestContext SearchRequestContext containing request details
     */
    private void constructSearchQueryRecord(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        String indices = Optional.ofNullable(context.getRequest().indices())
            .map(arr -> String.join(",", arr))
            .orElse("unknown");
        log.info("Processing search request for indices: {}", indices);

        if (skipSearchRequest(searchRequestContext)) {
            return;
        }

        SearchTask searchTask = context.getTask();
        List<TaskResourceInfo> tasksResourceUsages = searchRequestContext.getPhaseResourceUsage();
        tasksResourceUsages.add(
            new TaskResourceInfo(
                searchTask.getAction(),
                searchTask.getId(),
                searchTask.getParentTaskId().getId(),
                clusterService.localNode().getId(),
                searchTask.getTotalResourceStats()
            )
        );

        final SearchRequest request = context.getRequest();
        try {
            Map<MetricType, Measurement> measurements = new HashMap<>();
            measurements.put(
                MetricType.LATENCY,
                new Measurement(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchRequestContext.getAbsoluteStartNanos()))
            );
            measurements.put(
                MetricType.CPU,
                new Measurement(
                    tasksResourceUsages.stream().map(a -> a.getTaskResourceUsage().getCpuTimeInNanos()).mapToLong(Long::longValue).sum()
                )
            );
            measurements.put(
                MetricType.MEMORY,
                new Measurement(
                    tasksResourceUsages.stream().map(a -> a.getTaskResourceUsage().getMemoryInBytes()).mapToLong(Long::longValue).sum()
                )
            );

            Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.SEARCH_TYPE, request.searchType().toString().toLowerCase(Locale.ROOT));
            attributes.put(Attribute.SOURCE, request.source());
            attributes.put(Attribute.TOTAL_SHARDS, context.getNumShards());
            attributes.put(Attribute.INDICES, request.indices());
            attributes.put(Attribute.PHASE_LATENCY_MAP, searchRequestContext.phaseTookMap());
            attributes.put(Attribute.TASK_RESOURCE_USAGES, tasksResourceUsages);
            attributes.put(Attribute.GROUP_BY, QueryInsightsSettings.DEFAULT_GROUPING_TYPE);
            attributes.put(Attribute.NODE_ID, clusterService.localNode().getId());
            attributes.put(Attribute.TOP_N_QUERY, new HashMap<>(DEFAULT_TOP_N_QUERY_MAP));

            // Extract query structure features for ML model training
            Map<String, Object> queryFeatures = QueryStructureAnalyzer.extractQueryFeatures(request);
            attributes.put(Attribute.QUERY_FEATURES, queryFeatures);

            if (queryInsightsService.isGroupingEnabled() || log.isTraceEnabled()) {
                // Generate the query shape only if grouping is enabled or trace logging is enabled
                final String queryShape = queryShapeGenerator.buildShape(
                    request.source(),
                    groupingFieldNameEnabled,
                    groupingFieldTypeEnabled,
                    searchRequestContext.getSuccessfulSearchShardIndices()
                );

                // Print the query shape if tracer is enabled
                if (log.isTraceEnabled()) {
                    log.trace("Query Shape:\n{}", queryShape);
                }

                // Add hashcode attribute when grouping is enabled
                if (queryInsightsService.isGroupingEnabled()) {
                    String hashcode = queryShapeGenerator.getShapeHashCodeAsString(queryShape);
                    attributes.put(Attribute.QUERY_GROUP_HASHCODE, hashcode);
                }
            }

            Map<String, Object> labels = new HashMap<>();
            // Retrieve user provided label if exists
            String userProvidedLabel = context.getTask().getHeader(Task.X_OPAQUE_ID);
            if (userProvidedLabel != null) {
                labels.put(Task.X_OPAQUE_ID, userProvidedLabel);
            }
            attributes.put(Attribute.LABELS, labels);

            // Construct SearchQueryRecord from attributes and measurements
            SearchQueryRecord record = new SearchQueryRecord(request.getOrCreateAbsoluteStartMillis(), measurements, attributes);
            queryInsightsService.addRecord(record);

            // Added
            // Export query data
            exportQueryData(record);

        } catch (Exception e) {
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.DATA_INGEST_EXCEPTIONS);
            log.error(String.format(Locale.ROOT, "fail to ingest query insight data, error: %s", e));
        }
    }

    /**
     * Validate the index name for excluded indices
     * @param excludedIndices list of index to validate
     */
    public void validateExcludedIndices(@NonNull List<String> excludedIndices) {
        for (String index : excludedIndices) {
            if (index == null) {
                throw new IllegalArgumentException("Excluded index name cannot be null.");
            }
            if (index.isBlank()) {
                throw new IllegalArgumentException("Excluded index name cannot be blank.");
            }
            if (index.chars().anyMatch(Character::isUpperCase)) {
                throw new IllegalArgumentException("Index name must be lowercase.");
            }
        }
    }


    // Added:
    /**
     * Export query data to configured export destination
     *
     * @param record SearchQueryRecord containing query data to export
     */
    private synchronized void exportQueryData(SearchQueryRecord record) {
        // Add debug logging to help diagnose issues with specific workloads
        Object source = record.getAttributes().get(Attribute.SOURCE);
        String[] indices = (String[]) record.getAttributes().get(Attribute.INDICES);
        String indicesStr = (indices != null && indices.length > 0) ? String.join(",", indices) : "_all";

        log.debug("Exporting query data for indices: {}, query: {}", indicesStr,
                 source != null ? source.toString().substring(0, Math.min(100, source.toString().length())) + "..." : "null");

        queryBuffer.add(record);
        if (queryBuffer.size() >= BATCH_SIZE) {
            List<SearchQueryRecord> batch = new ArrayList<>(queryBuffer);
            queryBuffer.clear();
            clusterService.getClusterApplierService().threadPool().executor(QUERY_INSIGHTS_EXECUTOR).execute(() -> writeQueryBatch(batch));
        }
    }

    // Added:
    /**
     * Get active shard count for indices in the query
     *
     * @param record SearchQueryRecord containing query attributes
     * @return int representing the total active shard count
     */
    private int getActiveShardCount(SearchQueryRecord record) {
        ClusterState clusterState = clusterService.state();
        int activeShards = 0;
        String[] indices = (String[]) record.getAttributes().get(Attribute.INDICES);

        // Handle special case for "_all" or empty indices
        boolean isAllIndices = indices == null || indices.length == 0 ||
            (indices.length == 1 && (indices[0] == null || indices[0].equals("_all")));

        if (isAllIndices) {
            // Try to detect the actual index from the query
            Object source = record.getAttributes().get(Attribute.SOURCE);
            if (source != null) {
                String sourceStr = source.toString();
                String detectedIndex = null;

                if (sourceStr.contains("name.raw") || sourceStr.contains("feature_class") ||
                    sourceStr.contains("feature_code") || sourceStr.contains("country_code")) {
                    detectedIndex = "geonames";
                } else if (sourceStr.contains("tag") && sourceStr.contains("term")) {
                    // This is likely the stackexchange/nested benchmark
                    // Check if sonested index exists
                    try {
                        if (clusterService.state().metadata().hasIndex("sonested")) {
                            detectedIndex = "sonested";
                        } else {
                            // Find the index with the most shards
                            int maxShards = 0;
                            String bestIndex = null;

                            for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
                                if (indexRoutingTable.getIndex().getName().startsWith(".")) continue; // Skip system indices
                                if (indexRoutingTable.getIndex().getName().startsWith("top_queries")) continue; // Skip query insights indices

                                int activeShardCount = 0;
                                for (IndexShardRoutingTable shardTable : indexRoutingTable) {
                                    for (ShardRouting shard : shardTable) {
                                        if (shard.active()) {
                                            activeShardCount++;
                                        }
                                    }
                                }

                                if (activeShardCount > maxShards) {
                                    maxShards = activeShardCount;
                                    bestIndex = indexRoutingTable.getIndex().getName();
                                }
                            }

                            if (bestIndex != null) {
                                detectedIndex = bestIndex;
                            } else {
                                detectedIndex = "sonested";
                            }
                        }
                    } catch (Exception e) {
                        log.debug("Error detecting index for tag queries: {}", e.getMessage());
                        detectedIndex = "sonested";
                    }
                } else if (sourceStr.contains("event_type") || sourceStr.contains("timestamp") ||
                           sourceStr.contains("message") || sourceStr.contains("host")) {
                    detectedIndex = "eventdata";
                } else if (sourceStr.contains("sonested") || sourceStr.contains("song") ||
                           sourceStr.contains("artist") || sourceStr.contains("album")) {
                    detectedIndex = "sonested";
                }

                if (detectedIndex != null) {
                    // Use the detected index
                    try {
                        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(detectedIndex);
                        if (indexRoutingTable != null) {
                            for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                                for (ShardRouting shardRouting : shardRoutingTable) {
                                    if (shardRouting.active()) {
                                        activeShards++;
                                    }
                                }
                            }
                        }
                        return activeShards;
                    } catch (Exception e) {
                        // Fall back to counting all shards
                    }
                }
            }

            // If we couldn't detect a specific index, count all active shards
            for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
                if (indexRoutingTable != null) {
                    for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                        for (ShardRouting shardRouting : shardRoutingTable) {
                            if (shardRouting.active()) {
                                activeShards++;
                            }
                        }
                    }
                }
            }
            return activeShards;
        }

        // Handle specific indices
        for (String indexName : indices) {
            try {
                IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
                if (indexRoutingTable != null) {
                    for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                        for (ShardRouting shardRouting : shardRoutingTable) {
                            if (shardRouting.active()) {
                                activeShards++;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // Index might not exist, skip
                log.debug("Failed to get active shard count for index {}: {}", indexName, e.getMessage());
            }
        }
        return activeShards;
    }


    // Added:
    /**
     * Write batch of query records to export file
     *
     * @param records List of SearchQueryRecord to export
     */
    private void writeQueryBatch(List<SearchQueryRecord> records) {
        try (java.io.FileWriter writer = new java.io.FileWriter(queryExportFilePath, true)) {
            for (SearchQueryRecord record : records) {
                Object source = record.getAttributes().get(Attribute.SOURCE);
                String queryJson = source != null ? source.toString() : "{}";
                String queryType = getQueryType(record);

                String formattedDate = DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(record.getTimestamp()));
                String[] indices = (String[]) record.getAttributes().get(Attribute.INDICES);

                // Try to determine actual indices from the query if possible
                String indicesStr;
                if (indices == null || indices.length == 0 || (indices.length == 1 && "_all".equals(indices[0]))) {
                    // First check if we can find the index in the query source
                    if (source != null) {
                        String sourceStr = source.toString();

                        // Check for explicit index in the query
                        int indexPos = sourceStr.indexOf("\"index\":");
                        if (indexPos > 0) {
                            int startQuote = sourceStr.indexOf('"', indexPos + 8);
                            if (startQuote > 0) {
                                int endQuote = sourceStr.indexOf('"', startQuote + 1);
                                if (endQuote > 0) {
                                    indicesStr = sourceStr.substring(startQuote + 1, endQuote);
                                } else {
                                    indicesStr = "_all";
                                }
                            } else {
                                indicesStr = "_all";
                            }
                        } else {
                            // Check for field patterns that might indicate the index
                            // For geonames benchmark
                            if (sourceStr.contains("name.raw") || sourceStr.contains("feature_class") ||
                                sourceStr.contains("feature_code") || sourceStr.contains("country_code")) {
                                indicesStr = "geonames";
                            }
                            // For tag-based queries (likely stackexchange/nested benchmark)
                            else if (sourceStr.contains("tag") && sourceStr.contains("term")) {
                                // Check if sonested index exists
                                try {
                                    if (clusterService.state().metadata().hasIndex("sonested")) {
                                        indicesStr = "sonested";
                                    } else {
                                        // Find the index with the most documents
                                        IndicesStatsRequest request = new IndicesStatsRequest();
                                        request.docs(true);
                                        IndicesStatsResponse response = client.admin().indices().stats(request).actionGet();

                                        String bestIndex = null;
                                        long maxDocs = 0;

                                        for (String indexName : response.getIndices().keySet()) {
                                            if (indexName.startsWith(".")) continue; // Skip system indices
                                            if (indexName.startsWith("top_queries")) continue; // Skip query insights indices

                                            IndexStats stats = response.getIndex(indexName);
                                            if (stats != null && stats.getPrimaries().getDocs() != null) {
                                                long docCount = stats.getPrimaries().getDocs().getCount();
                                                if (docCount > maxDocs) {
                                                    maxDocs = docCount;
                                                    bestIndex = indexName;
                                                }
                                            }
                                        }

                                        if (bestIndex != null) {
                                            indicesStr = bestIndex;
                                        } else {
                                            indicesStr = "sonested";
                                        }
                                    }
                                } catch (Exception e) {
                                    log.debug("Error detecting index for tag queries: {}", e.getMessage());
                                    indicesStr = "sonested";
                                }
                            }
                            // For eventdata benchmark
                            else if (sourceStr.contains("event_type") || sourceStr.contains("timestamp") ||
                                     sourceStr.contains("message") || sourceStr.contains("host")) {
                                indicesStr = "eventdata";
                            }
                            // For sonested benchmark
                            else if (sourceStr.contains("sonested") || sourceStr.contains("song") ||
                                     sourceStr.contains("artist") || sourceStr.contains("album")) {
                                indicesStr = "sonested";
                            }
                            else {
                                // Try to get index from task header if available
                                String taskHeader = (String) record.getAttributes().get(Attribute.LABELS);
                                if (taskHeader != null && taskHeader.toString().contains("geonames")) {
                                    indicesStr = "geonames";
                                } else {
                                    indicesStr = "_all";
                                }
                            }
                        }
                    } else {
                        indicesStr = "_all";
                    }
                } else {
                    indicesStr = String.join(";", indices);
                }

                // Get query features
                Map<String, Object> queryFeatures = (Map<String, Object>) record.getAttributes().get(Attribute.QUERY_FEATURES);
                StringBuilder featureJson = new StringBuilder("{")
                    .append(queryFeatures != null ? formatQueryFeatures(queryFeatures) : "");
                if (featureJson.length() > 1) {
                    featureJson.append(",");
                }

                // Collect system metrics
                Map<String, Object> systemMetrics = SystemMetricsCollector.collectSystemMetrics();
                if (!systemMetrics.isEmpty()) {
                    // Group system metrics by category for better organization
                    Map<String, Map<String, Object>> categorizedMetrics = new HashMap<>();

                    // Define categories and their prefixes
                    String[][] categories = {
                        {"os", "system_", "load_", "cpu_", "memory_", "disk_", "io_", "network_", "process_"},
                        {"jvm", "jvm_"}
                    };

                    // Categorize metrics
                    for (Map.Entry<String, Object> entry : systemMetrics.entrySet()) {
                        String key = entry.getKey();
                        Object value = entry.getValue();
                        boolean categorized = false;

                        for (String[] category : categories) {
                            String categoryName = category[0];
                            for (int i = 1; i < category.length; i++) {
                                if (key.startsWith(category[i])) {
                                    categorizedMetrics.computeIfAbsent(categoryName, k -> new HashMap<>())
                                                     .put(key, value);
                                    categorized = true;
                                    break;
                                }
                            }
                            if (categorized) break;
                        }

                        // If not categorized, put in "other"
                        if (!categorized) {
                            categorizedMetrics.computeIfAbsent("other", k -> new HashMap<>())
                                             .put(key, value);
                        }
                    }

                    // Format system metrics as JSON with categories
                    StringBuilder systemMetricsJson = new StringBuilder();
                    boolean firstCategory = true;

                    for (Map.Entry<String, Map<String, Object>> category : categorizedMetrics.entrySet()) {
                        if (!firstCategory) {
                            systemMetricsJson.append(",");
                        }
                        firstCategory = false;

                        systemMetricsJson.append('"').append(category.getKey()).append('"').append(":{")
                                      .append(formatMetricsMap(category.getValue()))
                                      .append("}");
                    }

                    // Add system metrics to the output
                    if (systemMetricsJson.length() > 0) {
                        featureJson.append("\"system_metrics\":{")
                                  .append(systemMetricsJson)
                                  .append("}");
                    }
                }

                // Add basic metrics
                featureJson.append(String.format(
                    ",\"timestamp\":\"%s\",\"query_type\":\"%s\",\"latency_ms\":%d,\"cpu_nanos\":%d,\"memory_bytes\":%d,\"document_count\":%d,\"search_type\":\"%s\",\"indices\":\"%s\",\"total_shards\":%d,\"active_shard_count\":%d,\"node_id\":\"%s\",\"requested_size\":%d",
                    formattedDate,
                    queryType,
                    record.getMeasurement(MetricType.LATENCY).longValue(),
                    record.getMeasurement(MetricType.CPU).longValue(),
                    record.getMeasurement(MetricType.MEMORY).longValue(),
                    getDocumentCount(record),
                    record.getAttributes().get(Attribute.SEARCH_TYPE),
                    indicesStr,
                    (Integer) record.getAttributes().get(Attribute.TOTAL_SHARDS),
                    getActiveShardCount(record),
                    record.getAttributes().get(Attribute.NODE_ID),
                    getRequestedSize(record)
                ));

                // Add the query at the end
                featureJson.append(",\"query\":")
                          .append(queryJson);

                featureJson.append("}\n");
                writer.append(featureJson.toString());
            }
        } catch (IOException e) {
            log.error("Failed to write to CSV file: {}", e.getMessage());
        }
    }


    // Added:
    /**
     * Get requested size from query source
     *
     * @param record SearchQueryRecord containing query attributes
     * @return int representing the requested size or 0 if not found
     */
    private int getRequestedSize(SearchQueryRecord record) {
        Object source = record.getAttributes().get(Attribute.SOURCE);
        if (source != null) {
            String sourceStr = source.toString();
            if (sourceStr.contains("\"size\":")) {
                try {
                    int sizeIndex = sourceStr.indexOf("\"size\":");
                    String sizeSubstring = sourceStr.substring(sizeIndex + 7);
                    int commaIndex = sizeSubstring.indexOf(",");
                    int braceIndex = sizeSubstring.indexOf("}");
                    int endIndex = (commaIndex != -1 && commaIndex < braceIndex) ? commaIndex : braceIndex;
                    return Integer.parseInt(sizeSubstring.substring(0, endIndex).trim());
                } catch (Exception e) {
                    return -1;
                }
            }
        }
        return 0; // Default size
    }

    // Added:
    /**
     * Get document count for indices targeted by the query. Caches repeated indices.
     *
     * @param record SearchQueryRecord containing query attributes
     * @return long representing total document count across indices
     */
    private long getDocumentCount(SearchQueryRecord record) {
        if (client == null) {
            return 0;
        }

        String[] indices = (String[]) record.getAttributes().get(Attribute.INDICES);

        // Handle special case for "_all" or empty indices
        boolean isAllIndices = indices == null || indices.length == 0 ||
            (indices.length == 1 && (indices[0] == null || "_all".equals(indices[0])));

        // Try to detect the actual index if it's _all
        String[] actualIndices = indices;
        if (isAllIndices) {
            // Try to detect the index from the query
            Object source = record.getAttributes().get(Attribute.SOURCE);
            if (source != null) {
                String sourceStr = source.toString();

                // Detect index based on query fields
                if (sourceStr.contains("name.raw") || sourceStr.contains("feature_class") ||
                    sourceStr.contains("feature_code") || sourceStr.contains("country_code")) {
                    actualIndices = new String[] {"geonames"};
                    isAllIndices = false;
                } else if (sourceStr.contains("tag") && sourceStr.contains("term")) {
                    // This is likely the stackexchange/nested benchmark
                    // Check if sonested index exists and has documents
                    try {
                        if (clusterService.state().metadata().hasIndex("sonested")) {
                            actualIndices = new String[] {"sonested"};
                            isAllIndices = false;
                        } else {
                            // If no suitable index exists, use the index with the most documents
                            IndicesStatsRequest request = new IndicesStatsRequest();
                            request.docs(true);
                            IndicesStatsResponse response = client.admin().indices().stats(request).actionGet();

                            String bestIndex = null;
                            long maxDocs = 0;

                            for (String indexName : response.getIndices().keySet()) {
                                if (indexName.startsWith(".")) continue; // Skip system indices
                                if (indexName.startsWith("top_queries")) continue; // Skip query insights indices

                                IndexStats stats = response.getIndex(indexName);
                                if (stats != null && stats.getPrimaries().getDocs() != null) {
                                    long docCount = stats.getPrimaries().getDocs().getCount();
                                    if (docCount > maxDocs) {
                                        maxDocs = docCount;
                                        bestIndex = indexName;
                                    }
                                }
                            }

                            if (bestIndex != null) {
                                actualIndices = new String[] {bestIndex};
                                isAllIndices = false;
                            } else {
                                actualIndices = new String[] {"sonested"};
                                isAllIndices = false;
                            }
                        }
                    } catch (Exception e) {
                        log.debug("Error detecting index for tag queries: {}", e.getMessage());
                        actualIndices = new String[] {"sonested"};
                        isAllIndices = false;
                    }
                } else if (sourceStr.contains("event_type") || sourceStr.contains("timestamp") ||
                           sourceStr.contains("message") || sourceStr.contains("host")) {
                    actualIndices = new String[] {"eventdata"};
                    isAllIndices = false;
                } else if (sourceStr.contains("sonested") || sourceStr.contains("song") ||
                           sourceStr.contains("artist") || sourceStr.contains("album")) {
                    actualIndices = new String[] {"sonested"};
                    isAllIndices = false;
                }
            }
        }

        String cacheKey = isAllIndices ? "_all" : String.join(",", actualIndices);

        // Check cache first
        if (docCountCache.containsKey(cacheKey)) {
            return docCountCache.get(cacheKey);
        }

        // Not in cache, make API call
        try {
            IndicesStatsRequest request = new IndicesStatsRequest();
            if (!isAllIndices) {
                request.indices(actualIndices);
            }
            request.docs(true);

            IndicesStatsResponse response = client.admin().indices().stats(request).actionGet();
            long totalCount = 0;

            if (isAllIndices) {
                // For _all, use the total count across all indices
                totalCount = response.getTotal().getDocs() != null ?
                    response.getTotal().getDocs().getCount() : 0;
            } else {
                // Sum up document counts for each specific index in the query
                for (String indexName : actualIndices) {
                    IndexStats indexStats = response.getIndex(indexName);
                    if (indexStats != null && indexStats.getPrimaries().getDocs() != null) {
                        totalCount += indexStats.getPrimaries().getDocs().getCount();
                    }
                }
            }

            // Cache the result
            docCountCache.put(cacheKey, totalCount);
            return totalCount;
        } catch (Exception e) {
            log.debug("Failed to get document count for indices {}: {}",
                isAllIndices ? "_all" : String.join(",", indices), e.getMessage());
            // Cache 0 to avoid repeated failures
            docCountCache.put(cacheKey, 0L);
            return 0;
        }
    }

    // Added:
    /**
     * Classify query type based on query source content
     *
     * @param record SearchQueryRecord containing query attributes
     * @return String representing the query type or "UNKNOWN"
     */
    private String getQueryType(SearchQueryRecord record) {
        Object source = record.getAttributes().get(Attribute.SOURCE);
        if (source != null) {
            String sourceStr = source.toString().toLowerCase();
            if (sourceStr.contains("range")) return "RANGE";
            if (sourceStr.contains("match_all")) return "MATCH_ALL";
            if (sourceStr.contains("match_phrase")) return "MATCH_PHRASE";
            if (sourceStr.contains("match")) return "MATCH";
            if (sourceStr.contains("multi_match")) return "MULTI_MATCH";
            if (sourceStr.contains("term")) return "TERM";
            if (sourceStr.contains("terms")) return "TERMS";
            if (sourceStr.contains("bool")) return "BOOL";
            if (sourceStr.contains("wildcard")) return "WILDCARD";
            if (sourceStr.contains("prefix")) return "PREFIX";
            if (sourceStr.contains("fuzzy")) return "FUZZY";
            if (sourceStr.contains("regexp")) return "REGEXP";
            if (sourceStr.contains("exists")) return "EXISTS";
            if (sourceStr.contains("ids")) return "IDS";
            // More:
            if (sourceStr.contains("nested")) return "NESTED";
            if (sourceStr.contains("geo_polygon")) return "GEO_POLYGON";
            if (sourceStr.contains("geo_distance")) return "GEO_DISTANCE";
            if (sourceStr.contains("geo_bounding_box")) return "GEO_BOUNDING_BOX";
        }
        return "UNKNOWN";
    }

    // Added:
    /**
     * Format query features as JSON string for export
     *
     * @param metrics Map of feature names to values
     * @return Formatted JSON string of features
     */
    private String formatMetricsMap(Map<String, Object> metrics) {
        if (metrics == null || metrics.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        boolean first = true;

        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            first = false;

            Object value = entry.getValue();
            sb.append('"').append(entry.getKey()).append('"').append(':');

            if (value == null) {
                sb.append("null");
            } else if (value instanceof Number) {
                sb.append(value);
            } else if (value instanceof Boolean) {
                sb.append(value);
            } else if (value instanceof Set) {
                // Handle Set by converting to JSON array
                sb.append('[');
                boolean firstItem = true;
                for (Object item : (Set<?>) value) {
                    if (!firstItem) {
                        sb.append(',');
                    }
                    firstItem = false;

                    if (item instanceof String) {
                        sb.append('"').append(item).append('"');
                    } else {
                        sb.append(item);
                    }
                }
                sb.append(']');
            } else {
                // Treat as string for other types
                sb.append('"').append(value).append('"');
            }
        }

        return sb.toString();
    }

    // Added:
    /**
     * Format query features as JSON string for export
     *
     * @param features Map of feature names to values
     * @return Formatted JSON string of features
     */
    private String formatQueryFeatures(Map<String, Object> features) {
        if (features == null || features.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        boolean first = true;

        // Make sure we include all features, including the new ones
        String[] importantFeatures = {
            "query_depth", "boolean_clause_count", "field_count",
            "has_wildcards", "has_fuzzy_matching", "has_function_score",
            "has_script_fields", "query_type_count", "aggregation_count",
            "aggregation_complexity", "sort_field_count", "sort_complexity",
            "query_complexity_score", "query_size_bytes", "field_complexity_score"
        };

        // First add the important features in a specific order
        for (String key : importantFeatures) {
            if (features.containsKey(key)) {
                if (!first) {
                    sb.append(",");
                }
                first = false;

                Object value = features.get(key);
                sb.append('"').append(key).append('"').append(':');

                if (value == null) {
                    sb.append("null");
                } else if (value instanceof Number) {
                    sb.append(value);
                } else if (value instanceof Boolean) {
                    sb.append(value);
                } else if (value instanceof Set) {
                    // Handle Set by converting to JSON array
                    sb.append('[');
                    boolean firstItem = true;
                    for (Object item : (Set<?>) value) {
                        if (!firstItem) {
                            sb.append(',');
                        }
                        firstItem = false;

                        if (item instanceof String) {
                            sb.append('"').append(item).append('"');
                        } else {
                            sb.append(item);
                        }
                    }
                    sb.append(']');
                } else {
                    // Treat as string for other types
                    sb.append('"').append(value).append('"');
                }
            }
        }

        // Then add any remaining features
        for (Map.Entry<String, Object> entry : features.entrySet()) {
            String key = entry.getKey();
            // Skip if already added
            boolean alreadyAdded = false;
            for (String importantKey : importantFeatures) {
                if (key.equals(importantKey)) {
                    alreadyAdded = true;
                    break;
                }
            }
            if (alreadyAdded) {
                continue;
            }

            if (!first) {
                sb.append(",");
            }
            first = false;

            Object value = entry.getValue();
            sb.append('"').append(key).append('"').append(':');

            if (value == null) {
                sb.append("null");
            } else if (value instanceof Number) {
                sb.append(value);
            } else if (value instanceof Boolean) {
                sb.append(value);
            } else if (value instanceof Set) {
                // Handle Set by converting to JSON array
                sb.append('[');
                boolean firstItem = true;
                for (Object item : (Set<?>) value) {
                    if (!firstItem) {
                        sb.append(',');
                    }
                    firstItem = false;

                    if (item instanceof String) {
                        sb.append('"').append(item).append('"');
                    } else {
                        sb.append(item);
                    }
                }
                sb.append(']');
            } else {
                // Treat as string for other types
                sb.append('"').append(value).append('"');
            }
        }

        return sb.toString();
    }

}
