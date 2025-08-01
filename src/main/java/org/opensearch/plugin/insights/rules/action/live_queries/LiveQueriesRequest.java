package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

public class LiveQueriesRequest extends ActionRequest {

    private final boolean verbose;
    private final MetricType sortBy;
    private final int size;
    private final String[] nodeIds;
    private final String wlmGroup;

    /**
     * Deserialization constructor
     */
    public LiveQueriesRequest(final StreamInput in) throws IOException {
        super(in);
        this.verbose = in.readBoolean();
        this.sortBy = MetricType.readFromStream(in);
        this.size = in.readInt();
        this.nodeIds = in.readStringArray();
        this.wlmGroup = in.readOptionalString();
    }

    /**
     * Main constructor with all parameters
     */
    public LiveQueriesRequest(final boolean verbose, final MetricType sortBy, final int size, final String[] nodeIds, String wlmGroup) {
        this.verbose = verbose;
        this.sortBy = sortBy;
        this.size = size;
        this.nodeIds = nodeIds;
        this.wlmGroup = wlmGroup;
    }

    /**
     * Convenience constructor
     */
    public LiveQueriesRequest(final boolean verbose, final String... nodeIds) {
        this(verbose, MetricType.LATENCY, QueryInsightsSettings.DEFAULT_LIVE_QUERIES_SIZE, nodeIds, null);
    }

    public boolean isVerbose() {
        return verbose;
    }

    public MetricType getSortBy() {
        return sortBy;
    }

    public int getSize() {
        return size;
    }

    public String[] nodesIds() {
        return nodeIds;
    }

    public String getWlmGroup() {
        return wlmGroup;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(verbose);
        MetricType.writeTo(out, sortBy);
        out.writeInt(size);
        out.writeStringArray(nodeIds);
        out.writeOptionalString(wlmGroup);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
