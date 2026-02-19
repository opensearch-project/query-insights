/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.top_queries;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.junit.After;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;

/**
 * Integration tests for RBAC filtering on the Top Queries API.
 * These tests require the security plugin to be installed and are run via integTestWithSecurity.
 * When run without security (e.g., via integTest), these tests are skipped via assumeTrue.
 */
public class TopQueriesRbacIT extends QueryInsightsRestTestCase {
    private static final Logger log = Logger.getLogger(TopQueriesRbacIT.class.getName());
    private static final String USER_TEAM_A = "user_team_a";
    private static final String USER_TEAM_B = "user_team_b";
    private static final String USER_TEAM_C = "user_team_c";
    private static final String PASSWORD_A = "UserTeamA_1234";
    private static final String PASSWORD_B = "UserTeamB_1234";
    private static final String PASSWORD_C = "UserTeamC_1234";
    private static final String TEST_ROLE = "query_insights_test_role";
    private static final String TEST_INDEX = "test-rbac-index";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    @After
    public void cleanupRbac() throws Exception {
        // Reset filter_by_mode and other settings
        try {
            Request resetSettings = new Request("PUT", "/_cluster/settings");
            resetSettings.setJsonEntity(
                "{"
                    + "\"persistent\":{"
                    + "\"search.insights.top_queries.filter_by_mode\":\"none\","
                    + "\"search.insights.top_queries.latency.window_size\":\"5m\","
                    + "\"search.insights.top_queries.latency.top_n_size\":10"
                    + "}"
                    + "}"
            );
            client().performRequest(resetSettings);
        } catch (Exception e) {
            log.warning("Failed to reset cluster settings: " + e.getMessage());
        }

        // Delete test users, role mapping, role
        deleteSecurityResource("internalusers", USER_TEAM_A);
        deleteSecurityResource("internalusers", USER_TEAM_B);
        deleteSecurityResource("internalusers", USER_TEAM_C);
        deleteSecurityResource("rolesmapping", TEST_ROLE);
        deleteSecurityResource("roles", TEST_ROLE);

        // Delete test index
        try {
            client().performRequest(new Request("DELETE", "/" + TEST_INDEX));
        } catch (Exception e) {
            log.warning("Failed to delete test index: " + e.getMessage());
        }
    }

    /**
     * Test RBAC filtering on in-memory top queries:
     * - filter_by_mode=none: all users see all queries
     * - filter_by_mode=username: each user sees only their own queries, admin sees all
     * - filter_by_mode=backend_roles: users with shared backend role see each other's queries,
     *   users without shared backend role see only their own
     * - invalid filter_by_mode is rejected
     * - backend_roles field is present in response records
     */
    @SuppressWarnings("unchecked")
    public void testRbacFilteringOnTopQueries() throws Exception {
        assumeTrue("This test requires the security plugin", isSecurityPluginInstalled());

        // Setup security users and roles
        setupSecurityUsersAndRoles();

        // Configure top queries with 1m window, high top_n_size, local_index exporter, filter_by_mode=none
        configureTopQueriesForRbac();

        // Create test index with data
        createTestIndexWithDocuments();

        // Run queries as each user targeting the first node
        runSearchesAsUser(USER_TEAM_A, PASSWORD_A, 5);
        runSearchesAsUser(USER_TEAM_B, PASSWORD_B, 5);
        runSearchesAsUser(USER_TEAM_C, PASSWORD_C, 5);

        // Wait for query insights to process records
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis() + 3000);

        // === Test filter_by_mode = none ===
        List<Map<String, Object>> adminQueriesNone = getTopQueriesAsUser(null, null);
        int adminCountNone = adminQueriesNone.size();
        assertTrue("Admin should see queries in none mode, got " + adminCountNone, adminCountNone > 0);

        List<Map<String, Object>> userAQueriesNone = getTopQueriesAsUser(USER_TEAM_A, PASSWORD_A);
        assertEquals("In none mode, user_team_a should see same count as admin", adminCountNone, userAQueriesNone.size());

        // === Test filter_by_mode = username ===
        setFilterByMode("username");

        // Admin bypass - admin should still see all queries
        List<Map<String, Object>> adminQueriesUsername = getTopQueriesAsUser(null, null);
        assertEquals("Admin bypass should see all queries in username mode", adminCountNone, adminQueriesUsername.size());

        // user_team_a should see only their own queries
        List<Map<String, Object>> userAQueriesUsername = getTopQueriesAsUser(USER_TEAM_A, PASSWORD_A);
        Set<String> userAUsernames = extractUsernames(userAQueriesUsername);
        assertTrue("user_team_a should see some queries", userAQueriesUsername.size() > 0);
        assertEquals("user_team_a should see only own queries", Set.of(USER_TEAM_A), userAUsernames);

        // user_team_b should see only their own queries
        List<Map<String, Object>> userBQueriesUsername = getTopQueriesAsUser(USER_TEAM_B, PASSWORD_B);
        Set<String> userBUsernames = extractUsernames(userBQueriesUsername);
        assertTrue("user_team_b should see some queries", userBQueriesUsername.size() > 0);
        assertEquals("user_team_b should see only own queries", Set.of(USER_TEAM_B), userBUsernames);

        // Filtered counts should not exceed admin total
        int totalFiltered = userAQueriesUsername.size() + userBQueriesUsername.size();
        assertTrue(
            "Filtered counts (" + totalFiltered + ") should be <= admin total (" + adminCountNone + ")",
            totalFiltered <= adminCountNone
        );

        // === Test filter_by_mode = backend_roles ===
        setFilterByMode("backend_roles");

        // Admin bypass
        List<Map<String, Object>> adminQueriesBackendRoles = getTopQueriesAsUser(null, null);
        assertEquals("Admin bypass in backend_roles mode", adminCountNone, adminQueriesBackendRoles.size());

        // user_team_a and user_team_b share 'shared_team' backend role, so each should see both users' queries
        List<Map<String, Object>> userAQueriesBackendRoles = getTopQueriesAsUser(USER_TEAM_A, PASSWORD_A);
        Set<String> userABrUsernames = extractUsernames(userAQueriesBackendRoles);
        assertTrue("user_team_a should see queries in backend_roles mode", userAQueriesBackendRoles.size() > 0);
        assertTrue(
            "user_team_a should see queries from both A and B via shared_team, but saw: " + userABrUsernames,
            userABrUsernames.contains(USER_TEAM_A) && userABrUsernames.contains(USER_TEAM_B)
        );
        assertFalse(
            "user_team_a should NOT see user_team_c queries (no shared backend role), but saw: " + userABrUsernames,
            userABrUsernames.contains(USER_TEAM_C)
        );

        // user_team_c has backend_roles [team_gamma] with no overlap — should see only own queries
        List<Map<String, Object>> userCQueriesBackendRoles = getTopQueriesAsUser(USER_TEAM_C, PASSWORD_C);
        Set<String> userCBrUsernames = extractUsernames(userCQueriesBackendRoles);
        assertTrue("user_team_c should see some queries in backend_roles mode", userCQueriesBackendRoles.size() > 0);
        assertEquals("user_team_c should see only own queries (no shared backend role with A or B)", Set.of(USER_TEAM_C), userCBrUsernames);

        // === Test invalid filter_by_mode ===
        try {
            Request invalidRequest = new Request("PUT", "/_cluster/settings");
            invalidRequest.setJsonEntity("{\"persistent\":{\"search.insights.top_queries.filter_by_mode\":\"invalid_value\"}}");
            client().performRequest(invalidRequest);
            fail("Invalid filter_by_mode should be rejected");
        } catch (ResponseException e) {
            assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        }

        // === Verify backend_roles field in response ===
        setFilterByMode("none");
        List<Map<String, Object>> allQueries = getTopQueriesAsUser(null, null);
        boolean hasBackendRoles = allQueries.stream().anyMatch(q -> q.containsKey("backend_roles"));
        assertTrue("backend_roles field should be present in at least one record", hasBackendRoles);
    }

    /**
     * Test RBAC filtering on historical (from/to) top queries data.
     * Waits for window rotation so records are exported to local index,
     * then verifies RBAC filtering on the historical path.
     */
    @SuppressWarnings("unchecked")
    public void testRbacFilteringOnHistoricalData() throws Exception {
        assumeTrue("This test requires the security plugin", isSecurityPluginInstalled());

        // Setup
        setupSecurityUsersAndRoles();
        configureTopQueriesForRbac();
        createTestIndexWithDocuments();

        // Record from timestamp
        String fromTs = FORMATTER.format(Instant.now().minusSeconds(120));

        // Run queries as each user
        runSearchesAsUser(USER_TEAM_A, PASSWORD_A, 5);
        runSearchesAsUser(USER_TEAM_B, PASSWORD_B, 5);
        runSearchesAsUser(USER_TEAM_C, PASSWORD_C, 5);

        // Wait for query insights to process records
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis() + 3000);

        // Wait for window rotation (1m window + buffer)
        log.info("Waiting for window rotation (70 seconds)...");
        Thread.sleep(70000);

        // Fire additional queries to trigger drain/rotation
        runSearchesAsUser(USER_TEAM_A, PASSWORD_A, 3);
        runSearchesAsUser(USER_TEAM_B, PASSWORD_B, 3);
        runSearchesAsUser(USER_TEAM_C, PASSWORD_C, 3);
        Thread.sleep(QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL.millis() + 3000);

        String toTs = FORMATTER.format(Instant.now().plusSeconds(60));

        // === Test historical filter_by_mode = none ===
        setFilterByMode("none");

        // Use assertBusy because the exporter may take a moment to write records
        assertBusy(() -> {
            List<Map<String, Object>> adminHistNone = getHistoricalTopQueriesAsUser(null, null, fromTs, toTs);
            assertTrue("Admin should see historical queries in none mode", adminHistNone.size() > 0);
        }, 30, TimeUnit.SECONDS);

        List<Map<String, Object>> adminHistNone = getHistoricalTopQueriesAsUser(null, null, fromTs, toTs);
        int adminHistCount = adminHistNone.size();

        List<Map<String, Object>> userAHistNone = getHistoricalTopQueriesAsUser(USER_TEAM_A, PASSWORD_A, fromTs, toTs);
        assertEquals("In none mode, user_team_a should see all historical queries", adminHistCount, userAHistNone.size());

        // === Test historical filter_by_mode = username ===
        setFilterByMode("username");

        // Admin bypass
        List<Map<String, Object>> adminHistUsername = getHistoricalTopQueriesAsUser(null, null, fromTs, toTs);
        assertEquals("Admin bypass on historical data", adminHistCount, adminHistUsername.size());

        // user_team_a should see only own
        List<Map<String, Object>> userAHistUsername = getHistoricalTopQueriesAsUser(USER_TEAM_A, PASSWORD_A, fromTs, toTs);
        Set<String> userAHistUsernames = extractUsernames(userAHistUsername);
        assertTrue("user_team_a should see some historical queries", userAHistUsername.size() > 0);
        assertEquals("user_team_a should see only own historical queries", Set.of(USER_TEAM_A), userAHistUsernames);

        // user_team_b should see only own
        List<Map<String, Object>> userBHistUsername = getHistoricalTopQueriesAsUser(USER_TEAM_B, PASSWORD_B, fromTs, toTs);
        Set<String> userBHistUsernames = extractUsernames(userBHistUsername);
        assertTrue("user_team_b should see some historical queries", userBHistUsername.size() > 0);
        assertEquals("user_team_b should see only own historical queries", Set.of(USER_TEAM_B), userBHistUsernames);

        // === Test historical filter_by_mode = backend_roles ===
        setFilterByMode("backend_roles");

        // Admin bypass
        List<Map<String, Object>> adminHistBr = getHistoricalTopQueriesAsUser(null, null, fromTs, toTs);
        assertEquals("Admin bypass on historical data in backend_roles mode", adminHistCount, adminHistBr.size());

        // user_team_a should see both A and B via shared_team, but not C
        List<Map<String, Object>> userAHistBr = getHistoricalTopQueriesAsUser(USER_TEAM_A, PASSWORD_A, fromTs, toTs);
        Set<String> userAHistBrUsernames = extractUsernames(userAHistBr);
        assertTrue("user_team_a should see historical queries in backend_roles mode", userAHistBr.size() > 0);
        assertTrue(
            "user_team_a should see historical queries from both A and B via shared backend_role, but saw: " + userAHistBrUsernames,
            userAHistBrUsernames.contains(USER_TEAM_A) && userAHistBrUsernames.contains(USER_TEAM_B)
        );
        assertFalse(
            "user_team_a should NOT see user_team_c historical queries (no shared backend role), but saw: " + userAHistBrUsernames,
            userAHistBrUsernames.contains(USER_TEAM_C)
        );

        // user_team_c should see only own historical queries (no shared backend role with A or B)
        List<Map<String, Object>> userCHistBr = getHistoricalTopQueriesAsUser(USER_TEAM_C, PASSWORD_C, fromTs, toTs);
        Set<String> userCHistBrUsernames = extractUsernames(userCHistBr);
        assertTrue("user_team_c should see some historical queries in backend_roles mode", userCHistBr.size() > 0);
        assertEquals(
            "user_team_c should see only own historical queries (no shared backend role)",
            Set.of(USER_TEAM_C),
            userCHistBrUsernames
        );
    }

    // ========================= Helper Methods =========================

    /**
     * Check whether the security plugin is installed by querying _cat/plugins.
     */
    @SuppressWarnings("unchecked")
    private boolean isSecurityPluginInstalled() throws IOException {
        Request request = new Request("GET", "/_cat/plugins?format=json");
        Response response = client().performRequest(request);
        String body = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        return body.contains("opensearch-security");
    }

    /**
     * Create a security role, three test users with different backend roles, and a role mapping.
     * user_team_a: [team_alpha, shared_team]
     * user_team_b: [team_beta, shared_team]
     * user_team_c: [team_gamma] (no overlap with A or B)
     */
    private void setupSecurityUsersAndRoles() throws IOException {
        // Create role with query insights and search permissions
        Request createRole = new Request("PUT", "/_plugins/_security/api/roles/" + TEST_ROLE);
        createRole.setJsonEntity(
            "{"
                + "\"cluster_permissions\":[\"cluster:admin/opensearch/insights/top_queries\",\"cluster_monitor\"],"
                + "\"index_permissions\":[{"
                + "\"index_patterns\":[\"test-*\"],"
                + "\"allowed_actions\":[\"read\",\"search\",\"indices:data/read/search\",\"crud\"]"
                + "}]"
                + "}"
        );
        Response roleResp = client().performRequest(createRole);
        assertTrue(
            "Role creation should succeed",
            roleResp.getStatusLine().getStatusCode() == 200 || roleResp.getStatusLine().getStatusCode() == 201
        );

        // Create user_team_a with backend_roles [team_alpha, shared_team]
        Request createUserA = new Request("PUT", "/_plugins/_security/api/internalusers/" + USER_TEAM_A);
        createUserA.setJsonEntity("{\"password\":\"" + PASSWORD_A + "\",\"backend_roles\":[\"team_alpha\",\"shared_team\"]}");
        Response userAResp = client().performRequest(createUserA);
        assertTrue(
            "User A creation should succeed",
            userAResp.getStatusLine().getStatusCode() == 200 || userAResp.getStatusLine().getStatusCode() == 201
        );

        // Create user_team_b with backend_roles [team_beta, shared_team]
        Request createUserB = new Request("PUT", "/_plugins/_security/api/internalusers/" + USER_TEAM_B);
        createUserB.setJsonEntity("{\"password\":\"" + PASSWORD_B + "\",\"backend_roles\":[\"team_beta\",\"shared_team\"]}");
        Response userBResp = client().performRequest(createUserB);
        assertTrue(
            "User B creation should succeed",
            userBResp.getStatusLine().getStatusCode() == 200 || userBResp.getStatusLine().getStatusCode() == 201
        );

        // Create user_team_c with backend_roles [team_gamma] — no overlap with A or B
        Request createUserC = new Request("PUT", "/_plugins/_security/api/internalusers/" + USER_TEAM_C);
        createUserC.setJsonEntity("{\"password\":\"" + PASSWORD_C + "\",\"backend_roles\":[\"team_gamma\"]}");
        Response userCResp = client().performRequest(createUserC);
        assertTrue(
            "User C creation should succeed",
            userCResp.getStatusLine().getStatusCode() == 200 || userCResp.getStatusLine().getStatusCode() == 201
        );

        // Map role to all three users
        Request createMapping = new Request("PUT", "/_plugins/_security/api/rolesmapping/" + TEST_ROLE);
        createMapping.setJsonEntity("{\"users\":[\"" + USER_TEAM_A + "\",\"" + USER_TEAM_B + "\",\"" + USER_TEAM_C + "\"]}");
        Response mappingResp = client().performRequest(createMapping);
        assertTrue(
            "Role mapping creation should succeed",
            mappingResp.getStatusLine().getStatusCode() == 200 || mappingResp.getStatusLine().getStatusCode() == 201
        );
    }

    /**
     * Configure top queries for RBAC testing: latency enabled, top_n_size=100, window_size=1m,
     * exporter=local_index, filter_by_mode=none
     */
    private void configureTopQueriesForRbac() throws Exception {
        // Disable first to clear any existing queries
        updateClusterSettings(this::disableTopQueriesSettings);
        waitForSettingsDisabled("latency");

        // Enable with RBAC testing configuration
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{"
                + "\"persistent\":{"
                + "\"search.insights.top_queries.latency.enabled\":true,"
                + "\"search.insights.top_queries.latency.top_n_size\":100,"
                + "\"search.insights.top_queries.latency.window_size\":\"1m\","
                + "\"search.insights.top_queries.exporter.type\":\"local_index\","
                + "\"search.insights.top_queries.filter_by_mode\":\"none\""
                + "}"
                + "}"
        );
        Response response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());

        waitForSettingsPropagation("latency");
    }

    /**
     * Create a test index with 20 documents for search queries.
     */
    private void createTestIndexWithDocuments() throws IOException {
        // Create index with 1 shard, 0 replicas
        Request createIndex = new Request("PUT", "/" + TEST_INDEX);
        createIndex.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        try {
            client().performRequest(createIndex);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 400) {
                throw e;
            }
        }

        // Index 20 documents
        for (int i = 1; i <= 20; i++) {
            Request doc = new Request("POST", "/" + TEST_INDEX + "/_doc");
            doc.setJsonEntity("{\"title\":\"document " + i + "\",\"value\":" + i + "}");
            client().performRequest(doc);
        }

        // Refresh the index
        client().performRequest(new Request("POST", "/" + TEST_INDEX + "/_refresh"));
    }

    /**
     * Run search queries as a specific user, targeting the first node.
     */
    private void runSearchesAsUser(String username, String password, int count) throws IOException {
        try (RestClient userClient = buildFirstNodeClientForUser(username, password)) {
            for (int i = 1; i <= count; i++) {
                Request search = new Request("POST", "/" + TEST_INDEX + "/_search");
                search.setJsonEntity("{\"query\":{\"match\":{\"title\":\"document " + i + "\"}},\"size\":" + (i * 2) + "}");
                Response response = userClient.performRequest(search);
                Assert.assertEquals(200, response.getStatusLine().getStatusCode());
            }
        }
    }

    /**
     * Build a RestClient targeting the first node with the given user credentials.
     */
    private RestClient buildFirstNodeClientForUser(String username, String password) throws IOException {
        List<HttpHost> hosts = getClusterHosts();
        if (hosts.isEmpty()) {
            throw new IllegalStateException("No cluster hosts available");
        }
        HttpHost firstHost = hosts.get(0);
        return buildClientForHost(firstHost, username, password);
    }

    /**
     * Build a RestClient with the given user credentials for the default cluster.
     */
    private RestClient buildClientForUser(String username, String password) throws IOException {
        List<HttpHost> hosts = getClusterHosts();
        return buildClientForHost(hosts.toArray(new HttpHost[0]), username, password);
    }

    /**
     * Build a RestClient targeting a single host with user credentials.
     */
    private RestClient buildClientForHost(HttpHost host, String username, String password) throws IOException {
        return buildClientForHost(new HttpHost[] { host }, username, password);
    }

    /**
     * Build a RestClient targeting specific hosts with user credentials.
     */
    private RestClient buildClientForHost(HttpHost[] hosts, String username, String password) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);

        if (isHttps()) {
            builder.setHttpClientConfigCallback(httpClientBuilder -> {
                BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                    new AuthScope(null, -1),
                    new UsernamePasswordCredentials(username, password.toCharArray())
                );
                try {
                    final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                        .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                        .setSslContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build())
                        .setTlsDetailsFactory(sslEngine -> new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol()))
                        .build();
                    final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                        .setTlsStrategy(tlsStrategy)
                        .build();
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setConnectionManager(connectionManager);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            builder.setHttpClientConfigCallback(httpClientBuilder -> {
                BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                    new AuthScope(null, -1),
                    new UsernamePasswordCredentials(username, password.toCharArray())
                );
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            });
        }

        builder.setStrictDeprecationMode(false);
        return builder.build();
    }

    /**
     * Get top queries as a specific user. If username is null, uses the admin client.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getTopQueriesAsUser(String username, String password) throws IOException {
        Request request = new Request("GET", "/_insights/top_queries?pretty");

        Response response;
        if (username == null) {
            response = client().performRequest(request);
        } else {
            try (RestClient userClient = buildClientForUser(username, password)) {
                response = userClient.performRequest(request);
            }
        }

        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        return parseTopQueriesResponse(response);
    }

    /**
     * Get historical top queries (with from/to) as a specific user.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getHistoricalTopQueriesAsUser(String username, String password, String from, String to)
        throws IOException {
        String endpoint = "/_insights/top_queries?from=" + from + "&to=" + to;
        Request request = new Request("GET", endpoint);

        Response response;
        if (username == null) {
            response = client().performRequest(request);
        } else {
            try (RestClient userClient = buildClientForUser(username, password)) {
                response = userClient.performRequest(request);
            }
        }

        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        return parseTopQueriesResponse(response);
    }

    /**
     * Parse the top_queries array from a response.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> parseTopQueriesResponse(Response response) throws IOException {
        byte[] content = response.getEntity().getContent().readAllBytes();
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                content
            )
        ) {
            Map<String, Object> root = parser.map();
            List<Map<String, Object>> topQueries = (List<Map<String, Object>>) root.get("top_queries");
            assertNotNull("Expected 'top_queries' field in response", topQueries);
            return topQueries;
        }
    }

    /**
     * Extract unique usernames from a list of top query records.
     */
    private Set<String> extractUsernames(List<Map<String, Object>> topQueries) {
        Set<String> usernames = new HashSet<>();
        for (Map<String, Object> query : topQueries) {
            String username = (String) query.get("username");
            if (username != null) {
                usernames.add(username);
            }
        }
        return usernames;
    }

    /**
     * Set the filter_by_mode cluster setting.
     */
    private void setFilterByMode(String mode) throws IOException, InterruptedException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"persistent\":{\"search.insights.top_queries.filter_by_mode\":\"" + mode + "\"}}");
        Response response = client().performRequest(request);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        // Small wait for settings propagation
        Thread.sleep(1000);
    }

    /**
     * Delete a security resource (user, role, role mapping), ignoring errors.
     */
    private void deleteSecurityResource(String resourceType, String resourceName) {
        try {
            client().performRequest(new Request("DELETE", "/_plugins/_security/api/" + resourceType + "/" + resourceName));
        } catch (Exception e) {
            log.warning("Failed to delete " + resourceType + "/" + resourceName + ": " + e.getMessage());
        }
    }
}
