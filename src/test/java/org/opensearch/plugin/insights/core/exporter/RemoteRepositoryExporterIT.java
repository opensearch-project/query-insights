/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.nio.charset.StandardCharsets;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

/** Integration tests for Remote Repository Exporter */
public class RemoteRepositoryExporterIT extends QueryInsightsRestTestCase {

    public void testRemoteRepositoryExporter() throws Exception {
        createDocument();
        setupS3Repository();
        configureRemoteExporterAndEnableLatency();

        // Wait for exporter to be created and settings to propagate
        Thread.sleep(5000);

        // Perform searches to generate query insights data
        for (int i = 0; i < 3; i++) {
            performSearch(5);
            Thread.sleep(1000);
        }

        // Wait for window rotation (1 minute window)
        Thread.sleep(60000);
        verifyRemoteExporterConfiguration();
    }

    private void setupS3Repository() throws Exception {
        Request request = new Request("PUT", "/_snapshot/test-repo");
        request.setJsonEntity(
            "{ \"type\": \"s3\", "
                + "\"settings\": { "
                + "\"bucket\": \"test-bucket\", "
                + "\"region\": \"us-east-1\", "
                + "\"endpoint\": \"http://localhost:4566\", "
                + "\"path_style_access\": true } }"
        );
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void configureRemoteExporterAndEnableLatency() throws Exception {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{ \"persistent\": { "
                + "\"search.insights.top_queries.exporter.remote.repository\": \"test-repo\", "
                + "\"search.insights.top_queries.exporter.remote.path\": \"query-insights\", "
                + "\"search.insights.top_queries.exporter.remote.enabled\": true, "
                + "\"search.insights.top_queries.latency.enabled\": \"true\", "
                + "\"search.insights.top_queries.latency.window_size\": \"1m\" } }"
        );
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void verifyRemoteExporterConfiguration() throws Exception {
        // Check top queries API to see if queries were collected
        Request topQueriesRequest = new Request("GET", "/_insights/top_queries?type=latency");
        Response topQueriesResponse = client().performRequest(topQueriesRequest);
        String topQueriesContent = new String(topQueriesResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        // Verify we have queries collected
        assertTrue("Expected to find query records", topQueriesContent.contains("timestamp"));

        // Verify repository exists
        Request request = new Request("GET", "/_snapshot/test-repo");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("Expected repository to exist", content.contains("test-repo"));

        // Verify remote exporter settings are active
        Request settingsRequest = new Request("GET", "/_cluster/settings");
        Response settingsResponse = client().performRequest(settingsRequest);
        String settingsContent = new String(settingsResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("Expected remote exporter to be enabled", settingsContent.contains("test-repo"));
        assertTrue("Expected remote path to be configured", settingsContent.contains("query-insights"));

        // Note: Actual verification that files were exported to S3 happens in the GitHub Actions workflow
        // The workflow uses AWS CLI to check the S3 bucket contents after this test completes
        // See .github/workflows/remote_repository_exporter.yml for the S3 verification step
    }

}
