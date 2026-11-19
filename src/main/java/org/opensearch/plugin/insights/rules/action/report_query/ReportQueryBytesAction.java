/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.rules.action.report_query;

/**
 * Constants for the transport action the SQL plugin uses to report a finished SQL/PPL query.
 * <p>
 * The request is the core {@link org.opensearch.transport.BytesTransportRequest} rather than a plugin
 * class, so both plugins load the same class and the same-node call works across their classloaders.
 * It isn't an {@code ActionRequest}, so the handler is registered directly with the transport service.
 * <p>
 * Payload, in order. The SQL plugin's {@code QueryInsightsReporter} writes the same fields.
 * <pre>
 *   vInt    format version
 *   String  query source, e.g. "PPL"
 *   String  marker, "&lt;source&gt;:&lt;nodeId&gt;:&lt;taskId&gt;"
 *   String  node id
 *   String  query text
 *   vLong   start time, epoch millis
 *   vLong   latency, millis
 *   vLong   CPU, nanos
 *   vLong   memory, bytes
 *   String  user info, "" without security
 *   boolean failed
 * </pre>
 */
public final class ReportQueryBytesAction {

    /** Action name. The SQL plugin sends to this exact string. */
    public static final String NAME = "cluster:admin/opensearch/query_insights/report_query_bytes";

    /** Wire format version. Must match {@code QueryInsightsReporter.FORMAT_VERSION} in the SQL plugin. */
    public static final int FORMAT_VERSION = 1;

    private ReportQueryBytesAction() {}
}
