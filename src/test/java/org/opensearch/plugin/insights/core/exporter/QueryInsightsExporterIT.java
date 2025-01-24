/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.io.IOException;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

/** Rest Action tests for query  */
public class QueryInsightsExporterIT extends QueryInsightsRestTestCase {
    /**
     * Test Top Queries setting endpoints
     *
     * @throws IOException IOException
     */
    public void testQueryInsightsExporterSettings() throws IOException {
        // test invalid settings
        for (String setting : invalidExporterSettings()) {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(setting);
            try {
                client().performRequest(request);
                fail("Should not succeed with invalid exporter settings");
            } catch (ResponseException e) {
                assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
            }
        }

        // Test enable Top N Queries feature
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(defaultExporterSettings());
        Response response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private String defaultExporterSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.exporter.type\" : \"local_index\"\n"
            + "    }\n"
            + "}";
    }

    private String[] invalidExporterSettings() {
        return new String[] {
            "{\n" + "    \"persistent\" : {\n" + "        \"search.insights.top_queries.exporter.type\" : invalid_type\n" + "    }\n" + "}",
            "{\n"
                + "    \"persistent\" : {\n"
                + "        \"search.insights.top_queries.exporter.type\" : local_index,\n"
                + "        \"search.insights.top_queries.exporter.config.index\" : \"1a2b\"\n"
                + "    }\n"
                + "}" };
    }
}
