/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;

/**
 * The searches a SQL/PPL query ran, kept in the query's record: how many there were, and the first
 * {@link #MAX_SEARCHES} with the fields its detail view shows. It's indexed with the record, unmapped.
 */
public final class ChildSearches implements Writeable, ToXContentObject {

    /** At most this many searches are kept per query. {@link #getCount()} counts all of them. */
    public static final int MAX_SEARCHES = 100;

    private static final String COUNT = "count";
    private static final String SEARCHES = "searches";

    private final int count;
    private final List<Search> searches;

    /**
     * @param count    how many searches the query ran
     * @param searches the first of them, at most {@link #MAX_SEARCHES}
     */
    public ChildSearches(final int count, final List<Search> searches) {
        this.count = count;
        this.searches = List.copyOf(searches);
    }

    /**
     * @param in the stream to read from
     * @throws IOException if reading fails
     */
    public ChildSearches(final StreamInput in) throws IOException {
        this.count = in.readVInt();
        this.searches = in.readList(Search::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(count);
        out.writeList(searches);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(COUNT, count);
        builder.startArray(SEARCHES);
        for (Search search : searches) {
            search.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

    /**
     * Reads what {@link #toXContent} wrote, as stored in the record's document.
     *
     * @param parser positioned on the object's start
     * @return the searches
     * @throws IOException if parsing fails
     */
    public static ChildSearches fromXContent(final XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        int count = 0;
        final List<Search> searches = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            final String field = parser.currentName();
            parser.nextToken();
            if (COUNT.equals(field)) {
                count = parser.intValue();
            } else if (SEARCHES.equals(field)) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    searches.add(Search.fromXContent(parser));
                }
            } else {
                parser.skipChildren();
            }
        }
        return new ChildSearches(count, searches);
    }

    /**
     * @return how many searches the query ran
     */
    public int getCount() {
        return count;
    }

    /**
     * @return the first of them, at most {@link #MAX_SEARCHES}
     */
    public List<Search> getSearches() {
        return searches;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ChildSearches)) {
            return false;
        }
        final ChildSearches other = (ChildSearches) o;
        return count == other.count && searches.equals(other.searches);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, searches);
    }

    /** One search a SQL/PPL query ran. */
    public static final class Search implements Writeable, ToXContentObject {

        private static final String TIMESTAMP = "timestamp";
        private static final String INDICES = "indices";
        private static final String TOTAL_SHARDS = "total_shards";
        private static final String LATENCY_MILLIS = "latency_millis";
        private static final String CPU_NANOS = "cpu_nanos";
        private static final String MEMORY_BYTES = "memory_bytes";
        private static final String FAILED = "failed";
        private static final String WLM_GROUP_ID = "wlm_group_id";

        private final long timestamp;
        private final List<String> indices;
        private final int totalShards;
        private final long latencyMillis;
        private final long cpuNanos;
        private final long memoryBytes;
        private final boolean failed;
        private final String wlmGroupId;

        /**
         * @param timestamp     when it started, epoch millis
         * @param indices       the indices it read
         * @param totalShards   how many shards it read
         * @param latencyMillis its latency
         * @param cpuNanos      its CPU time
         * @param memoryBytes   its memory
         * @param failed        whether it failed
         * @param wlmGroupId    its workload group, or {@code null}
         */
        public Search(
            final long timestamp,
            final List<String> indices,
            final int totalShards,
            final long latencyMillis,
            final long cpuNanos,
            final long memoryBytes,
            final boolean failed,
            final String wlmGroupId
        ) {
            this.timestamp = timestamp;
            this.indices = indices == null ? List.of() : List.copyOf(indices);
            this.totalShards = totalShards;
            this.latencyMillis = latencyMillis;
            this.cpuNanos = cpuNanos;
            this.memoryBytes = memoryBytes;
            this.failed = failed;
            this.wlmGroupId = wlmGroupId;
        }

        /**
         * @param in the stream to read from
         * @throws IOException if reading fails
         */
        public Search(final StreamInput in) throws IOException {
            this.timestamp = in.readLong();
            this.indices = in.readStringList();
            this.totalShards = in.readVInt();
            this.latencyMillis = in.readLong();
            this.cpuNanos = in.readLong();
            this.memoryBytes = in.readLong();
            this.failed = in.readBoolean();
            this.wlmGroupId = in.readOptionalString();
        }

        /**
         * Takes what the detail view shows from a search's record.
         *
         * @param record the search's record, as the search listener built it
         * @return the search
         */
        public static Search of(final SearchQueryRecord record) {
            final Object totalShards = record.getAttributes().get(Attribute.TOTAL_SHARDS);
            final Object wlmGroupId = record.getAttributes().get(Attribute.WLM_GROUP_ID);
            return new Search(
                record.getTimestamp(),
                indicesOf(record.getAttributes().get(Attribute.INDICES)),
                totalShards instanceof Number number ? number.intValue() : 0,
                asLong(record.getMeasurement(MetricType.LATENCY)),
                asLong(record.getMeasurement(MetricType.CPU)),
                asLong(record.getMeasurement(MetricType.MEMORY)),
                Boolean.TRUE.equals(record.getAttributes().get(Attribute.FAILED)),
                wlmGroupId == null ? null : wlmGroupId.toString()
            );
        }

        private static List<String> indicesOf(final Object indices) {
            if (indices instanceof Object[] array) {
                return Arrays.stream(array).map(String::valueOf).toList();
            }
            if (indices instanceof Collection<?> collection) {
                return collection.stream().map(String::valueOf).toList();
            }
            return List.of();
        }

        private static long asLong(final Number number) {
            return number == null ? 0L : number.longValue();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeLong(timestamp);
            out.writeStringCollection(indices);
            out.writeVInt(totalShards);
            out.writeLong(latencyMillis);
            out.writeLong(cpuNanos);
            out.writeLong(memoryBytes);
            out.writeBoolean(failed);
            out.writeOptionalString(wlmGroupId);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            builder.field(TIMESTAMP, timestamp);
            builder.field(INDICES, indices);
            builder.field(TOTAL_SHARDS, totalShards);
            builder.field(LATENCY_MILLIS, latencyMillis);
            builder.field(CPU_NANOS, cpuNanos);
            builder.field(MEMORY_BYTES, memoryBytes);
            builder.field(FAILED, failed);
            if (wlmGroupId != null) {
                builder.field(WLM_GROUP_ID, wlmGroupId);
            }
            return builder.endObject();
        }

        static Search fromXContent(final XContentParser parser) throws IOException {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
            long timestamp = 0L;
            List<String> indices = List.of();
            int totalShards = 0;
            long latencyMillis = 0L;
            long cpuNanos = 0L;
            long memoryBytes = 0L;
            boolean failed = false;
            String wlmGroupId = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                final String field = parser.currentName();
                parser.nextToken();
                switch (field) {
                    case TIMESTAMP:
                        timestamp = parser.longValue();
                        break;
                    case INDICES:
                        indices = parser.list().stream().map(String::valueOf).toList();
                        break;
                    case TOTAL_SHARDS:
                        totalShards = parser.intValue();
                        break;
                    case LATENCY_MILLIS:
                        latencyMillis = parser.longValue();
                        break;
                    case CPU_NANOS:
                        cpuNanos = parser.longValue();
                        break;
                    case MEMORY_BYTES:
                        memoryBytes = parser.longValue();
                        break;
                    case FAILED:
                        failed = parser.booleanValue();
                        break;
                    case WLM_GROUP_ID:
                        wlmGroupId = parser.text();
                        break;
                    default:
                        parser.skipChildren();
                }
            }
            return new Search(timestamp, indices, totalShards, latencyMillis, cpuNanos, memoryBytes, failed, wlmGroupId);
        }

        /**
         * @return when it started, epoch millis
         */
        public long getTimestamp() {
            return timestamp;
        }

        /**
         * @return the indices it read
         */
        public List<String> getIndices() {
            return indices;
        }

        /**
         * @return how many shards it read
         */
        public int getTotalShards() {
            return totalShards;
        }

        /**
         * @return its latency, in milliseconds
         */
        public long getLatencyMillis() {
            return latencyMillis;
        }

        /**
         * @return its CPU time, in nanoseconds
         */
        public long getCpuNanos() {
            return cpuNanos;
        }

        /**
         * @return its memory, in bytes
         */
        public long getMemoryBytes() {
            return memoryBytes;
        }

        /**
         * @return whether it failed
         */
        public boolean isFailed() {
            return failed;
        }

        /**
         * @return its workload group, or {@code null}
         */
        public String getWlmGroupId() {
            return wlmGroupId;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Search)) {
                return false;
            }
            final Search other = (Search) o;
            return timestamp == other.timestamp
                && totalShards == other.totalShards
                && latencyMillis == other.latencyMillis
                && cpuNanos == other.cpuNanos
                && memoryBytes == other.memoryBytes
                && failed == other.failed
                && indices.equals(other.indices)
                && Objects.equals(wlmGroupId, other.wlmGroupId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, indices, totalShards, latencyMillis, cpuNanos, memoryBytes, failed, wlmGroupId);
        }
    }
}
