/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.recommendations;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request to analyze a query and generate recommendations
 */
public class AnalyzeQueryRequest extends ActionRequest {
    private final String querySource;
    private final List<String> indices;

    /**
     * Constructor
     * @param querySource the query source JSON
     * @param indices the indices to analyze against
     */
    public AnalyzeQueryRequest(String querySource, List<String> indices) {
        this.querySource = querySource;
        this.indices = indices != null ? indices : Collections.emptyList();
    }

    /**
     * Constructor from stream
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public AnalyzeQueryRequest(StreamInput in) throws IOException {
        super(in);
        this.querySource = in.readString();
        this.indices = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(querySource);
        out.writeStringCollection(indices);
    }

    /**
     * Get the query source
     * @return the query source JSON
     */
    public String getQuerySource() {
        return querySource;
    }

    /**
     * Get the indices
     * @return the list of indices
     */
    public List<String> getIndices() {
        return indices;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (querySource == null || querySource.isEmpty()) {
            validationException = new ActionRequestValidationException();
            validationException.addValidationError("query source must not be null or empty");
        }
        return validationException;
    }
}
