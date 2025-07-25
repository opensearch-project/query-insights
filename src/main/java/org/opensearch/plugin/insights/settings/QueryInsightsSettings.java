/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.settings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.insights.core.exporter.SinkType;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;

/**
 * Settings for Query Insights Plugin
 */
public class QueryInsightsSettings {
    /**
     * Executors settings
     */
    public static final String QUERY_INSIGHTS_EXECUTOR = "query_insights_executor";
    /**
     * Max number of thread
     */
    public static final int MAX_THREAD_COUNT = 5;
    /**
     * Max number of requests for the consumer to collect at one time
     */
    public static final int QUERY_RECORD_QUEUE_CAPACITY = 1000;
    /**
     * Time interval for record queue consumer to run
     */
    public static final TimeValue QUERY_RECORD_QUEUE_DRAIN_INTERVAL = new TimeValue(5, TimeUnit.SECONDS);
    /**
     * Default Values and Settings
     */
    public static final TimeValue MAX_WINDOW_SIZE = new TimeValue(1, TimeUnit.DAYS);
    /**
     * Minimal window size
     */
    public static final TimeValue MIN_WINDOW_SIZE = new TimeValue(1, TimeUnit.MINUTES);
    /**
     * Valid window sizes
     */
    public static final Set<TimeValue> VALID_WINDOW_SIZES_IN_MINUTES = new HashSet<>(
        Arrays.asList(
            new TimeValue(1, TimeUnit.MINUTES),
            new TimeValue(5, TimeUnit.MINUTES),
            new TimeValue(10, TimeUnit.MINUTES),
            new TimeValue(30, TimeUnit.MINUTES)
        )
    );

    /** Default N size for top N queries */
    public static final int MAX_N_SIZE = 100;
    /** Default window size in seconds to keep the top N queries with latency data in query insight store */
    public static final TimeValue DEFAULT_WINDOW_SIZE = new TimeValue(5, TimeUnit.MINUTES);
    /** Default top N size to keep the data in query insight store */
    public static final int DEFAULT_TOP_N_SIZE = 10;
    /**
     * Query Insights base uri
     */
    public static final String PLUGINS_BASE_URI = "/_insights";

    public static final GroupingType DEFAULT_GROUPING_TYPE = GroupingType.NONE;
    public static final int DEFAULT_GROUPS_EXCLUDING_TOPN_LIMIT = 100;

    public static final int MAX_GROUPS_EXCLUDING_TOPN_LIMIT = 10000;
    public static final String QUERY_INSIGHTS_HEALTH_STATS_URI = PLUGINS_BASE_URI + "/health_stats";

    /**
     * Settings for Top Queries
     *
     */
    public static final String TOP_QUERIES_BASE_URI = PLUGINS_BASE_URI + "/top_queries";

    /**
     * Settings for Live Queries
     */
    public static final String LIVE_QUERIES_BASE_URI = PLUGINS_BASE_URI + "/live_queries";

    /**
     * Default number of results to return for live queries
     */
    public static final int DEFAULT_LIVE_QUERIES_SIZE = 100;

    /**
     * Default timeout for search requests in query insights operations
     */
    public static final TimeValue DEFAULT_SEARCH_REQUEST_TIMEOUT = new TimeValue(10, TimeUnit.SECONDS);

    /** Default prefix for top N queries feature */
    public static final String TOP_N_QUERIES_SETTING_PREFIX = "search.insights.top_queries";
    /** Default prefix for top N queries grouping feature */
    public static final String TOP_N_QUERIES_GROUPING_SETTING_PREFIX = "search.insights.top_queries.grouping";
    /** Default prefix for top N queries by latency feature */
    public static final String TOP_N_LATENCY_QUERIES_PREFIX = TOP_N_QUERIES_SETTING_PREFIX + ".latency";
    /** Default prefix for top N queries by cpu feature */
    public static final String TOP_N_CPU_QUERIES_PREFIX = TOP_N_QUERIES_SETTING_PREFIX + ".cpu";
    /** Default prefix for top N queries by memory feature */
    public static final String TOP_N_MEMORY_QUERIES_PREFIX = TOP_N_QUERIES_SETTING_PREFIX + ".memory";
    /**
     * Boolean setting for enabling top queries by latency.
     */
    public static final Setting<Boolean> TOP_N_LATENCY_QUERIES_ENABLED = Setting.boolSetting(
        TOP_N_LATENCY_QUERIES_PREFIX + ".enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Int setting to define the top n size for top queries by latency.
     */
    public static final Setting<Integer> TOP_N_LATENCY_QUERIES_SIZE = Setting.intSetting(
        TOP_N_LATENCY_QUERIES_PREFIX + ".top_n_size",
        DEFAULT_TOP_N_SIZE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Time setting to define the window size in seconds for top queries by latency.
     */
    public static final Setting<TimeValue> TOP_N_LATENCY_QUERIES_WINDOW_SIZE = Setting.positiveTimeSetting(
        TOP_N_LATENCY_QUERIES_PREFIX + ".window_size",
        DEFAULT_WINDOW_SIZE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Define the group_by option for Top N queries to group queries.
     */
    public static final Setting<String> TOP_N_QUERIES_GROUP_BY = Setting.simpleString(
        TOP_N_QUERIES_GROUPING_SETTING_PREFIX + ".group_by",
        DEFAULT_GROUPING_TYPE.getValue(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Define the max_groups_excluding_topn option for Top N queries to group queries.
     */
    public static final Setting<Integer> TOP_N_QUERIES_MAX_GROUPS_EXCLUDING_N = Setting.intSetting(
        TOP_N_QUERIES_GROUPING_SETTING_PREFIX + ".max_groups_excluding_topn",
        DEFAULT_GROUPS_EXCLUDING_TOPN_LIMIT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> TOP_N_QUERIES_GROUPING_FIELD_NAME = Setting.boolSetting(
        TOP_N_QUERIES_GROUPING_SETTING_PREFIX + ".attributes.field_name",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> TOP_N_QUERIES_GROUPING_FIELD_TYPE = Setting.boolSetting(
        TOP_N_QUERIES_GROUPING_SETTING_PREFIX + ".attributes.field_type",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Boolean setting for enabling top queries by cpu.
     */
    public static final Setting<Boolean> TOP_N_CPU_QUERIES_ENABLED = Setting.boolSetting(
        TOP_N_CPU_QUERIES_PREFIX + ".enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Int setting to define the top n size for top queries by cpu.
     */
    public static final Setting<Integer> TOP_N_CPU_QUERIES_SIZE = Setting.intSetting(
        TOP_N_CPU_QUERIES_PREFIX + ".top_n_size",
        DEFAULT_TOP_N_SIZE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Time setting to define the window size in seconds for top queries by cpu.
     */
    public static final Setting<TimeValue> TOP_N_CPU_QUERIES_WINDOW_SIZE = Setting.positiveTimeSetting(
        TOP_N_CPU_QUERIES_PREFIX + ".window_size",
        DEFAULT_WINDOW_SIZE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Boolean setting for enabling top queries by memory.
     */
    public static final Setting<Boolean> TOP_N_MEMORY_QUERIES_ENABLED = Setting.boolSetting(
        TOP_N_MEMORY_QUERIES_PREFIX + ".enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Int setting to define the top n size for top queries by memory.
     */
    public static final Setting<Integer> TOP_N_MEMORY_QUERIES_SIZE = Setting.intSetting(
        TOP_N_MEMORY_QUERIES_PREFIX + ".top_n_size",
        DEFAULT_TOP_N_SIZE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Time setting to define the window size in seconds for top queries by memory.
     */
    public static final Setting<TimeValue> TOP_N_MEMORY_QUERIES_WINDOW_SIZE = Setting.positiveTimeSetting(
        TOP_N_MEMORY_QUERIES_PREFIX + ".window_size",
        DEFAULT_WINDOW_SIZE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Config key for exporter type
     */
    public static final String EXPORTER_TYPE = "type";
    /**
     * Settings and defaults for top queries exporters
     */
    private static final String TOP_N_QUERIES_EXPORTER_PREFIX = TOP_N_QUERIES_SETTING_PREFIX + ".exporter";
    /**
     * Index name prefix for top queries indices
     */
    public static final String TOP_QUERIES_INDEX_PREFIX = "top_queries";

    /**
     * Date format pattern for top queries indices
     */
    public static final String INDEX_DATE_FORMAT_PATTERN = "yyyy.MM.dd";

    /**
     * Default index pattern of top n queries
     */
    public static final String DEFAULT_TOP_N_QUERIES_INDEX_PATTERN = "'" + TOP_QUERIES_INDEX_PREFIX + "-'" + INDEX_DATE_FORMAT_PATTERN;
    /**
     * Default exporter type of top queries
     */
    public static final String DEFAULT_TOP_QUERIES_EXPORTER_TYPE = SinkType.LOCAL_INDEX.toString();
    /**
     * Default template priority for top queries indices
     */
    public static final long DEFAULT_TEMPLATE_PRIORITY = 1847L;
    /**
     * Default Top N local indices retention period in days
     */
    public static final int DEFAULT_DELETE_AFTER_VALUE = 7;
    /**
     * Minimum Top N local indices retention period in days
     */
    public static final int MIN_DELETE_AFTER_VALUE = 1;
    /**
     * Maximum Top N local indices retention period in days
     */
    public static final int MAX_DELETE_AFTER_VALUE = 180;

    /**
     * Setting for Top N local indices retention period
     * <p>
     * Note: This setting is only applicable when sink type is "local_index"
     * and it applies to exporters of all metric types
     */
    public static final Setting<Integer> TOP_N_EXPORTER_DELETE_AFTER = Setting.intSetting(
        TOP_N_QUERIES_EXPORTER_PREFIX + ".delete_after_days",
        DEFAULT_DELETE_AFTER_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Settings for the top n queries exporter
     */
    public static final Setting<String> TOP_N_EXPORTER_TYPE = Setting.simpleString(
        TOP_N_QUERIES_EXPORTER_PREFIX + ".type",
        DEFAULT_TOP_QUERIES_EXPORTER_TYPE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Settings for the list of indices that excluded from top queries.
     */
    public static final Setting<List<String>> TOP_N_QUERIES_EXCLUDED_INDICES = Setting.listSetting(
        TOP_N_QUERIES_SETTING_PREFIX + ".excluded_indices",
        Collections.emptyList(),
        Function.identity(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Index pattern glob for matching top query indices
     */
    public static final String TOP_QUERIES_INDEX_PATTERN_GLOB = TOP_QUERIES_INDEX_PREFIX + "-*";

    /**
     * Setting for Top N local indices template priority
     */
    public static final Setting<Long> TOP_N_EXPORTER_TEMPLATE_PRIORITY = Setting.longSetting(
        TOP_N_QUERIES_EXPORTER_PREFIX + ".template_priority",
        DEFAULT_TEMPLATE_PRIORITY,
        0L, // min value is 0
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Get the enabled setting based on type
     * @param type MetricType
     * @return enabled setting
     */
    public static Setting<Boolean> getTopNEnabledSetting(MetricType type) {
        switch (type) {
            case CPU:
                return TOP_N_CPU_QUERIES_ENABLED;
            case MEMORY:
                return TOP_N_MEMORY_QUERIES_ENABLED;
            default:
                return TOP_N_LATENCY_QUERIES_ENABLED;
        }
    }

    /**
     * Get the top n size setting based on type
     * @param type MetricType
     * @return top n size setting
     */
    public static Setting<Integer> getTopNSizeSetting(MetricType type) {
        switch (type) {
            case CPU:
                return TOP_N_CPU_QUERIES_SIZE;
            case MEMORY:
                return TOP_N_MEMORY_QUERIES_SIZE;
            default:
                return TOP_N_LATENCY_QUERIES_SIZE;
        }
    }

    /**
     * Get the window size setting based on type
     * @param type MetricType
     * @return top n queries window size setting
     */
    public static Setting<TimeValue> getTopNWindowSizeSetting(MetricType type) {
        switch (type) {
            case CPU:
                return TOP_N_CPU_QUERIES_WINDOW_SIZE;
            case MEMORY:
                return TOP_N_MEMORY_QUERIES_WINDOW_SIZE;
            default:
                return TOP_N_LATENCY_QUERIES_WINDOW_SIZE;
        }
    }

    /**
     * Default constructor
     */
    public QueryInsightsSettings() {}
}
