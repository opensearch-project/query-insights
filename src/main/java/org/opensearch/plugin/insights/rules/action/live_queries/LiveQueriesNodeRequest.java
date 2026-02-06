/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

/**
 * A request to get live queries information from a single node
 */
public class LiveQueriesNodeRequest extends TransportRequest {

    private LiveQueriesRequest request;

    public LiveQueriesNodeRequest(LiveQueriesRequest request) {
        this.request = request;
    }

    public LiveQueriesNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.request = new LiveQueriesRequest(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }

    public LiveQueriesRequest getRequest() {
        return request;
    }
}
