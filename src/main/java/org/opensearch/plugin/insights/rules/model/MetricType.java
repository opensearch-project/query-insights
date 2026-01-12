/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Valid metric types for a search query record
 */
public enum MetricType implements Comparator<Number> {
    /**
     * Latency metric type
     */
    LATENCY,
    /**
     * CPU usage metric type
     */
    CPU,
    /**
     * JVM heap usage metric type
     */
    MEMORY,
    /**
     * Failure metric type
     */
    FAILURE;

    /**
     * Read a MetricType from a StreamInput
     *
     * @param in the StreamInput to read from
     * @return MetricType
     * @throws IOException IOException
     */
    public static MetricType readFromStream(final StreamInput in) throws IOException {
        return fromString(in.readString());
    }

    /**
     * Create MetricType from String
     *
     * @param metricType the String representation of MetricType
     * @return MetricType
     */
    public static MetricType fromString(final String metricType) {
        return MetricType.valueOf(metricType.toUpperCase(Locale.ROOT));
    }

    /**
     * Write MetricType to a StreamOutput
     *
     * @param out the StreamOutput to write
     * @param metricType the MetricType to write
     * @throws IOException IOException
     */
    public static void writeTo(final StreamOutput out, final MetricType metricType) throws IOException {
        out.writeString(metricType.toString());
    }

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }

    /**
     * Get all valid metrics
     *
     * @return A set of String that contains all valid metrics
     */
    public static Set<MetricType> allMetricTypes() {
        return Arrays.stream(values()).collect(Collectors.toSet());
    }

    /**
     * Compare two numbers based on the metric type
     *
     * @param a the first Number to be compared.
     * @param b the second Number to be compared.
     * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second
     */
    public int compare(final Number a, final Number b) {
        switch (this) {
            case LATENCY:
            case CPU:
            case MEMORY:
            case FAILURE:
                return Long.compare(a.longValue(), b.longValue());
        }
        return -1;
    }

    /**
     * Parse a value with the correct type based on MetricType
     *
     * @param o the generic object to parse
     * @return {@link Number}
     */
    Number parseValue(final Object o) {
        switch (this) {
            case LATENCY:
            case CPU:
            case MEMORY:
            case FAILURE:
                return (Long) o;
            default:
                return (Number) o;
        }
    }

    /**
     * Returns the earliest version that is aware of this MetricType
     *
     * @return {@link Version}
     */
    Version getMinimalSupportedVersion() {
        switch (this) {
            case LATENCY:
            case CPU:
            case MEMORY:
                return Version.V_2_12_0;
            case FAILURE:
                return Version.V_3_5_0;
            default:
                throw new UnsupportedOperationException("Unknown metric type [" + this + "]");
        }
    }

}
