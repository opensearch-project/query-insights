/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.auth;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext.UserPrincipalInfo;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.FilterByMode;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link TopQueriesRbacFilter}
 */
public class TopQueriesRbacFilterTests extends OpenSearchTestCase {

    private SearchQueryRecord createRecord(String username, String[] userRoles, String[] backendRoles) {
        Map<Attribute, Object> attributes = new HashMap<>();
        if (username != null) {
            attributes.put(Attribute.USERNAME, username);
        }
        if (userRoles != null) {
            attributes.put(Attribute.USER_ROLES, userRoles);
        }
        if (backendRoles != null) {
            attributes.put(Attribute.BACKEND_ROLES, backendRoles);
        }
        Map<MetricType, Measurement> measurements = Map.of(MetricType.LATENCY, new Measurement(1L));
        return new SearchQueryRecord(System.currentTimeMillis(), measurements, attributes, "test-id-" + randomAlphaOfLength(5));
    }

    public void testFilterByModeNone_returnsAllRecords() {
        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(createRecord("user1", new String[] { "role1" }, new String[] { "br1" }));
        records.add(createRecord("user2", new String[] { "role2" }, new String[] { "br2" }));

        UserPrincipalInfo userInfo = new UserPrincipalInfo("user1", List.of("br1"), List.of("role1"));

        List<SearchQueryRecord> result = TopQueriesRbacFilter.filterRecords(records, FilterByMode.NONE, userInfo);
        assertEquals(2, result.size());
    }

    public void testFilterByUsername_matchingRecords() {
        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(createRecord("user1", new String[] { "role1" }, new String[] { "br1" }));
        records.add(createRecord("user2", new String[] { "role2" }, new String[] { "br2" }));
        records.add(createRecord("user1", new String[] { "role1" }, new String[] { "br1" }));

        UserPrincipalInfo userInfo = new UserPrincipalInfo("user1", List.of("br1"), List.of("role1"));

        List<SearchQueryRecord> result = TopQueriesRbacFilter.filterRecords(records, FilterByMode.USERNAME, userInfo);
        assertEquals(2, result.size());
    }

    public void testFilterByUsername_noMatch() {
        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(createRecord("user1", new String[] { "role1" }, new String[] { "br1" }));
        records.add(createRecord("user2", new String[] { "role2" }, new String[] { "br2" }));

        UserPrincipalInfo userInfo = new UserPrincipalInfo("user3", List.of("br3"), List.of("role3"));

        List<SearchQueryRecord> result = TopQueriesRbacFilter.filterRecords(records, FilterByMode.USERNAME, userInfo);
        assertEquals(0, result.size());
    }

    public void testFilterByBackendRoles_overlappingRole() {
        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(createRecord("user1", new String[] { "role1" }, new String[] { "team_a", "team_b" }));
        records.add(createRecord("user2", new String[] { "role2" }, new String[] { "team_c" }));

        UserPrincipalInfo userInfo = new UserPrincipalInfo("user3", List.of("team_a"), List.of("role3"));

        List<SearchQueryRecord> result = TopQueriesRbacFilter.filterRecords(records, FilterByMode.BACKEND_ROLES, userInfo);
        assertEquals(1, result.size());
    }

    public void testFilterByBackendRoles_noOverlap() {
        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(createRecord("user1", new String[] { "role1" }, new String[] { "team_a" }));
        records.add(createRecord("user2", new String[] { "role2" }, new String[] { "team_b" }));

        UserPrincipalInfo userInfo = new UserPrincipalInfo("user3", List.of("team_c"), List.of("role3"));

        List<SearchQueryRecord> result = TopQueriesRbacFilter.filterRecords(records, FilterByMode.BACKEND_ROLES, userInfo);
        assertEquals(0, result.size());
    }

    public void testAdminBypass_usernameMode() {
        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(createRecord("user1", new String[] { "role1" }, new String[] { "br1" }));
        records.add(createRecord("user2", new String[] { "role2" }, new String[] { "br2" }));

        UserPrincipalInfo adminInfo = new UserPrincipalInfo("admin_user", List.of("admin_br"), List.of("all_access"));

        List<SearchQueryRecord> result = TopQueriesRbacFilter.filterRecords(records, FilterByMode.USERNAME, adminInfo);
        assertEquals(2, result.size());
    }

    public void testAdminBypass_backendRolesMode() {
        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(createRecord("user1", new String[] { "role1" }, new String[] { "br1" }));
        records.add(createRecord("user2", new String[] { "role2" }, new String[] { "br2" }));

        UserPrincipalInfo adminInfo = new UserPrincipalInfo("admin_user", List.of("admin_br"), List.of("all_access"));

        List<SearchQueryRecord> result = TopQueriesRbacFilter.filterRecords(records, FilterByMode.BACKEND_ROLES, adminInfo);
        assertEquals(2, result.size());
    }

    public void testNullUserInfo_returnsEmpty() {
        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(createRecord("user1", new String[] { "role1" }, new String[] { "br1" }));

        List<SearchQueryRecord> result = TopQueriesRbacFilter.filterRecords(records, FilterByMode.USERNAME, null);
        assertEquals(0, result.size());
    }

    public void testRecordsWithoutUserData_excluded() {
        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(createRecord(null, null, null)); // Record without user data
        records.add(createRecord("user1", new String[] { "role1" }, new String[] { "br1" }));

        UserPrincipalInfo userInfo = new UserPrincipalInfo("user1", List.of("br1"), List.of("role1"));

        // Username mode: record without USERNAME should be excluded
        List<SearchQueryRecord> usernameResult = TopQueriesRbacFilter.filterRecords(records, FilterByMode.USERNAME, userInfo);
        assertEquals(1, usernameResult.size());

        // Backend roles mode: record without BACKEND_ROLES should be excluded
        List<SearchQueryRecord> backendRolesResult = TopQueriesRbacFilter.filterRecords(records, FilterByMode.BACKEND_ROLES, userInfo);
        assertEquals(1, backendRolesResult.size());
    }

    public void testIsAdmin() {
        assertTrue(TopQueriesRbacFilter.isAdmin(new UserPrincipalInfo("admin", List.of("br"), List.of("all_access"))));
        assertFalse(TopQueriesRbacFilter.isAdmin(new UserPrincipalInfo("user", List.of("br"), List.of("some_role"))));
        assertFalse(TopQueriesRbacFilter.isAdmin(null));
    }
}
