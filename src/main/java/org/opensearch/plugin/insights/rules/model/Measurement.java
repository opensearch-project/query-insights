/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.Objects;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Measurement that is stored in the SearchQueryRecord. Measurement can be of a specific AggregationType
 */
public class Measurement implements ToXContentObject, Writeable {
    private static int DEFAULT_COUNT = 1;

    private static final String NUMBER = "number";
    private static final String COUNT = "count";
    private static final String AGGREGATION_TYPE = "aggregationType";

    private AggregationType aggregationType;
    private Number number;
    private int count;

    /**
     * Constructor
     * @param number number
     * @param count count
     * @param aggregationType aggregationType
     */
    public Measurement(Number number, int count, AggregationType aggregationType) {
        this.number = number;
        this.count = count;
        this.aggregationType = aggregationType;
    }

    /**
     * Constructor
     * @param number number
     * @param aggregationType aggregationType
     */
    public Measurement(Number number, AggregationType aggregationType) {
        this(number, DEFAULT_COUNT, aggregationType);
    }

    /**
     * Constructor
     * @param number number
     */
    public Measurement(Number number) {
        this(number, DEFAULT_COUNT, AggregationType.DEFAULT_AGGREGATION_TYPE);
    }

    private Measurement() {}

    /**
     * Construct a measurement from {@link XContentParser}
     *
     * @param parser {@link XContentParser}
     * @return {@link Measurement}
     * @throws IOException IOException
     */
    public static Measurement fromXContent(XContentParser parser) throws IOException {
        Measurement builder = new Measurement();
        builder.parseXContent(parser);
        return builder;
    }

    /**
     * Add measurement number to the current number based on the aggregationType.
     * If aggregateType is NONE, replace the number since we are not aggregating in this case.
     * @param toAdd number to add
     */
    public void addMeasurement(Number toAdd) {
        switch (aggregationType) {
            case NONE:
                setMeasurement(toAdd);
                break;
            case SUM:
                setMeasurement(addMeasurementInferType(number, toAdd));
                break;
            case AVERAGE:
                count += 1;
                setMeasurement(addMeasurementInferType(number, toAdd));
                break;
            default:
                throw new IllegalArgumentException("The following aggregation type is not supported : " + aggregationType);
        }
    }

    private Number addMeasurementInferType(Number a, Number b) {
        if (a instanceof Long && b instanceof Long) {
            return a.longValue() + b.longValue();
        } else if (a instanceof Integer && b instanceof Integer) {
            return a.intValue() + b.intValue();
        } else if (a instanceof Double && b instanceof Double) {
            return a.doubleValue() + b.doubleValue();
        } else if (a instanceof Float && b instanceof Float) {
            return a.floatValue() + b.floatValue();
        } else {
            throw new IllegalArgumentException("Unsupported number type: " + a.getClass() + " or " + b.getClass());
        }
    }

    /**
     * Get measurement number based on the aggragation type
     * @return measurement number
     */
    public Number getMeasurement() {
        switch (aggregationType) {
            case NONE:
            case SUM:
                return number;
            case AVERAGE:
                return getAverageMeasurement(number, count);
            default:
                throw new IllegalArgumentException("Aggregation Type should be set for measurement.");
        }
    }

    /**
     * Get average measurement number based on the total and count
     * @param total total measurement value
     * @param count count of measurements
     * @return average measurement value
     */
    private Number getAverageMeasurement(Number total, int count) {
        if (count == 0) {
            throw new IllegalArgumentException("Count cannot be zero for average calculation.");
        }

        if (total instanceof Long) {
            return ((Long) total) / count;
        } else if (total instanceof Integer) {
            return ((Integer) total) / count;
        } else if (total instanceof Double) {
            return ((Double) total) / count;
        } else if (total instanceof Float) {
            return ((Float) total) / count;
        } else {
            throw new IllegalArgumentException("Unsupported number type: " + total.getClass());
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
     * Set aggregation type
     * @param aggregationType aggregation type
     */
    public void setAggregationType(AggregationType aggregationType) {
        this.aggregationType = aggregationType;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(NUMBER, number);
        builder.field(COUNT, count);
        builder.field(AGGREGATION_TYPE, aggregationType.toString());
        builder.endObject();
        return builder;
    }

    /**
     * Parse a measurement from {@link XContentParser}
     *
     * @param parser {@link XContentParser}
     * @throws IOException IOException
     */
    private void parseXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected [" + XContentParser.Token.START_OBJECT + "] but found [" + token + "]",
                parser.getTokenLocation()
            );
        } else {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (NUMBER.equals(currentFieldName)) {
                        this.number = parser.numberValue();
                    } else if (COUNT.equals(currentFieldName)) {
                        this.count = parser.intValue();
                    } else if (AGGREGATION_TYPE.equals(currentFieldName)) {
                        this.aggregationType = AggregationType.valueOf(parser.text());
                    }
                }
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeNumber(out, number);
        out.writeInt(count);
        out.writeString(aggregationType.toString());
    }

    private void writeNumber(StreamOutput out, Number number) throws IOException {
        if (number instanceof Long) {
            out.writeByte((byte) 0); // Type indicator for Long
            out.writeLong((Long) number);
        } else if (number instanceof Integer) {
            out.writeByte((byte) 1); // Type indicator for Integer
            out.writeInt((Integer) number);
        } else if (number instanceof Double) {
            out.writeByte((byte) 2); // Type indicator for Double
            out.writeDouble((Double) number);
        } else if (number instanceof Float) {
            out.writeByte((byte) 3); // Type indicator for Float
            out.writeFloat((Float) number);
        } else {
            throw new IOException("Unsupported number type: " + number.getClass());
        }
    }

    private static Number readNumber(StreamInput in) throws IOException {
        byte typeIndicator = in.readByte();
        switch (typeIndicator) {
            case 0:
                return in.readLong();
            case 1:
                return in.readInt();
            case 2:
                return in.readDouble();
            case 3:
                return in.readFloat();
            default:
                throw new IOException("Unsupported number type indicator: " + typeIndicator);
        }
    }

    public static Measurement readFromStream(StreamInput in) throws IOException {
        Number number = readNumber(in);
        int count = in.readInt();
        AggregationType aggregationType = AggregationType.valueOf(in.readString());
        return new Measurement(number, count, aggregationType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Measurement that = (Measurement) o;
        return count == that.count && Objects.equals(number, that.number) && aggregationType == that.aggregationType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(number, count, aggregationType);
    }
}
