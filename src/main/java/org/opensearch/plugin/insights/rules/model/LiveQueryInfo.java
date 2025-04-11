/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Model class for live query information
 */
public class LiveQueryInfo implements Writeable, ToXContentFragment {
    private final String id;
    private final String action;
    private final String description;
    private final long startTime;
    private final long runningTimeNanos;
    private final Map<String, Object> headers;
    private final Map<String, Object> queryDetails;

    /**
     * Constructor for LiveQueryInfo
     * 
     * @param id Query task ID
     * @param action Query task action
     * @param description Query task description
     * @param startTime Query task start time
     * @param runningTimeNanos Query task running time in nanoseconds
     * @param headers Query task headers
     * @param queryDetails Query task details (only included when detailed is true)
     */
    public LiveQueryInfo(
        final String id,
        final String action,
        final String description,
        final long startTime,
        final long runningTimeNanos,
        final Map<String, Object> headers,
        final Map<String, Object> queryDetails
    ) {
        this.id = id;
        this.action = action;
        this.description = description;
        this.startTime = startTime;
        this.runningTimeNanos = runningTimeNanos;
        this.headers = headers;
        this.queryDetails = queryDetails;
    }

    /**
     * Constructor for LiveQueryInfo from a StreamInput
     * 
     * @param in The input stream
     * @throws IOException If there's an error reading from the stream
     */
    public LiveQueryInfo(final StreamInput in) throws IOException {
        this.id = in.readString();
        this.action = in.readString();
        this.description = in.readString();
        this.startTime = in.readLong();
        this.runningTimeNanos = in.readLong();
        this.headers = in.readMap();
        this.queryDetails = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(action);
        out.writeString(description);
        out.writeLong(startTime);
        out.writeLong(runningTimeNanos);
        out.writeMap(headers);
        out.writeMap(queryDetails);
    }

    /**
     * Get the query task ID
     * 
     * @return query task ID
     */
    public String getId() {
        return id;
    }

    /**
     * Get the query task action
     * 
     * @return query task action
     */
    public String getAction() {
        return action;
    }

    /**
     * Get the query task description
     * 
     * @return query task description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Get the query task start time
     * 
     * @return query task start time
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Get the query task running time in nanoseconds
     * 
     * @return query task running time in nanoseconds
     */
    public long getRunningTimeNanos() {
        return runningTimeNanos;
    }

    /**
     * Get the query task headers
     * 
     * @return query task headers
     */
    public Map<String, Object> getHeaders() {
        return headers;
    }

    /**
     * Get the query task details
     * 
     * @return query task details
     */
    public Map<String, Object> getQueryDetails() {
        return queryDetails;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        builder.field("action", action);
        builder.field("description", description);
        builder.field("start_time", startTime);
        builder.field("running_time_nanos", runningTimeNanos);
        
        if (headers != null && !headers.isEmpty()) {
            builder.field("headers", headers);
        }
        
        if (queryDetails != null && !queryDetails.isEmpty()) {
            builder.field("query_details", queryDetails);
        }
        
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LiveQueryInfo that = (LiveQueryInfo) o;
        return startTime == that.startTime 
            && runningTimeNanos == that.runningTimeNanos 
            && Objects.equals(id, that.id) 
            && Objects.equals(action, that.action) 
            && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, action, description, startTime, runningTimeNanos);
    }
} 