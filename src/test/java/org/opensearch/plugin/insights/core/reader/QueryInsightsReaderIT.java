package org.opensearch.plugin.insights.core.reader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

public class QueryInsightsReaderIT extends QueryInsightsRestTestCase {
    public void testQueryInsightsReaderSettings() throws IOException, InterruptedException {
        createDocument();
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(defaultExporterSettings());
        Response response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        setLatencyWindowSize("1m");
        waitForWindowToPass(10);
        performSearch();
        waitForWindowToPass(70);
        fetchHistoricalTopQueries();

    }

    private void performSearch() throws IOException {
        String searchJson = "{\n"
            + "  \"query\": {\n"
            + "    \"match\": {\n"
            + "      \"title\": \"Test Document\"\n"
            + "    }\n"
            + "  }\n"
            + "}";

        Request searchRequest = new Request("POST", "/my-index-0/_search?size=20&pretty");
        searchRequest.setJsonEntity(searchJson);
        Response response = client().performRequest(searchRequest);

        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        assertTrue("Expected search results for title='Test Document'", responseContent.contains("\"Test Document\""));
    }

    private void createDocument() throws IOException {
        String documentJson = "{\n"
            + "  \"title\": \"Test Document\",\n"
            + "  \"content\": \"This is a test document for OpenSearch\"\n"
            + "}";

        Request createDocumentRequest = new Request("POST", "/my-index-0/_doc/");
        createDocumentRequest.setJsonEntity(documentJson);
        Response response = client().performRequest(createDocumentRequest);
        assertEquals(201, response.getStatusLine().getStatusCode());
    }

    private void setLatencyWindowSize(String windowSize) throws IOException {
        String windowSizeJson = "{ \"persistent\": { \"search.insights.top_queries.latency.window_size\": \"" + windowSize + "\" } }";
        Request windowSizeRequest = new Request("PUT", "/_cluster/settings");
        windowSizeRequest.setJsonEntity(windowSizeJson);
        client().performRequest(windowSizeRequest);
    }

    private void waitForWindowToPass(int seconds) throws InterruptedException {
        Thread.sleep(seconds * 1000);
    }

    private String defaultExporterSettings() {
        return "{ \"persistent\" : { \"search.insights.top_queries.exporter.type\" : \"local_index\" } }";
    }

    private void fetchHistoricalTopQueries() throws IOException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
        String from = formatter.format(Instant.now().minusSeconds(9600)); // 1 hour ago
        String to = formatter.format(Instant.now()); // current time

        Request fetchRequest = new Request("GET", "/_insights/top_queries?from=" + from + "&to=" + to);
        Response fetchResponse = client().performRequest(fetchRequest);

        assertEquals(200, fetchResponse.getStatusLine().getStatusCode());
        String fetchResponseContent = new String(fetchResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(
            "Expected historical top queries data",
            fetchResponseContent.contains("\"match\":{\"title\":{\"query\":\"Test Document\"")
        );
    }
}
