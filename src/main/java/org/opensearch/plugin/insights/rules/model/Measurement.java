/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Measurement that is stored in the SearchQueryRecord. Measurement can be of a specific DimensionType and MetricType
 */
public class Measurement implements ToXContentObject, Writeable {
    private static int DEFAULT_COUNT = 1;
    private DimensionType dimensionType;
    private MetricType metricType;
    private Number number;
    private int count;

    /**
     * Constructor
     * @param metricType metricType
     * @param number number
     * @param count count
     * @param dimensionType dimensionType
     */
    public Measurement(MetricType metricType, Number number, int count, DimensionType dimensionType) {
        this.metricType = metricType;
        this.number = number;
        this.count = count;
        this.dimensionType = dimensionType;
    }

    /**
     * Constructor
     * @param metricType metricType
     * @param number number
     * @param dimensionType dimensionType
     */
    public Measurement(MetricType metricType, Number number, DimensionType dimensionType) {
        this(metricType, number, DEFAULT_COUNT, dimensionType);
    }

    /**
     * Constructor
     * @param metricType metricType
     * @param number number
     */
    public Measurement(MetricType metricType, Number number) {
        this(metricType, number, DEFAULT_COUNT, DimensionType.DEFUALT_DIMENSION_TYPE);
    }

    /**
     * Add measurement number to the current number based on the dimension type
     * @param toAdd number to add
     */
    public void addMeasurement(Number toAdd) {
        switch (dimensionType) {
            case NONE:
                dimensionType = DimensionType.SUM;
                setMeasurement(MetricType.addMeasurements(number, toAdd, metricType));
                break;
            case SUM:
                setMeasurement(MetricType.addMeasurements(number, toAdd, metricType));
                break;
            case AVERAGE:
                count += 1;
                setMeasurement(MetricType.addMeasurements(number, toAdd, metricType));
                break;
            default:
                throw new IllegalArgumentException("The following dimension type is not supported : " + dimensionType);
        }
    }

    /**
     * Get measurement number based on the dimension type
     * @return measurement number
     */
    public Number getMeasurement() {
        switch (dimensionType) {
            case NONE:
            case SUM:
                return number;
            case AVERAGE:
                return MetricType.getAverageMeasurement(number, count, metricType);
            default:
                throw new IllegalArgumentException("Dimension Type should be set for measurement.");
        }
    }

    /**
     * Set measurement
     * @param measurement measurement number
     */
    public void setMeasurement(Number measurement) {
        number = measurement;
    }

    /**
     * Set dimension type
     * @param dimensionType dimension type
     */
    public void setDimensionType(DimensionType dimensionType) {
        this.dimensionType = dimensionType;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("metricType", metricType.toString());
        builder.field("number", number);
        builder.field("count", count);
        builder.field("dimensionType", dimensionType.toString());
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(metricType.toString());
        out.writeGenericValue(metricType.parseValue(number));
        out.writeInt(count);
        out.writeString(dimensionType.toString());
    }

    public static Measurement readFromStream(StreamInput in) throws IOException {
        MetricType metricType = MetricType.valueOf(in.readString().toUpperCase(Locale.ROOT));
        Number number = metricType.parseValue(in.readGenericValue());
        int count = in.readInt();
        DimensionType dimensionType = DimensionType.valueOf(in.readString());
        return new Measurement(metricType, number, count, dimensionType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Measurement that = (Measurement) o;
        return count == that.count
            && metricType == that.metricType
            && Objects.equals(number, that.number)
            && dimensionType == that.dimensionType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricType, number, count, dimensionType);
    }
}
