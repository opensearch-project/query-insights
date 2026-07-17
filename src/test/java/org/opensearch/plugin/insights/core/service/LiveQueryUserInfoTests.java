/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import java.util.List;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext.UserPrincipalInfo;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for live query user info map operations in {@link QueryInsightsService}
 */
public class LiveQueryUserInfoTests extends OpenSearchTestCase {

    public void testBuildLiveQueryTaskKey() {
        String key = QueryInsightsService.buildLiveQueryTaskKey("nodeA", 42);
        assertEquals("nodeA:42", key);
    }

    public void testBuildLiveQueryTaskKeyWithLongId() {
        String key = QueryInsightsService.buildLiveQueryTaskKey("H1xZaeAmQk6kjw2KSA7swQ", 99999);
        assertEquals("H1xZaeAmQk6kjw2KSA7swQ:99999", key);
    }

    public void testUserPrincipalInfoBasic() {
        UserPrincipalInfo info = new UserPrincipalInfo("admin", List.of("backend1"), List.of("role1", "role2"));
        assertEquals("admin", info.getUserName());
        assertEquals(List.of("role1", "role2"), info.getRoles());
        assertEquals(List.of("backend1"), info.getBackendRoles());
    }

    public void testUserPrincipalInfoWithNullUsername() {
        UserPrincipalInfo info = new UserPrincipalInfo(null, List.of("backend1"), List.of("role1"));
        assertNull(info.getUserName());
        assertEquals(List.of("role1"), info.getRoles());
        assertEquals(List.of("backend1"), info.getBackendRoles());
    }

    public void testUserPrincipalInfoWithEmptyRoles() {
        UserPrincipalInfo info = new UserPrincipalInfo("user1", List.of(), List.of());
        assertEquals("user1", info.getUserName());
        assertTrue(info.getRoles().isEmpty());
        assertTrue(info.getBackendRoles().isEmpty());
    }

    public void testUserPrincipalInfoWithMultipleRoles() {
        UserPrincipalInfo info = new UserPrincipalInfo(
            "analyst",
            List.of("readall", "devteam"),
            List.of("all_access", "readall", "kibana_user")
        );
        assertEquals("analyst", info.getUserName());
        assertEquals(3, info.getRoles().size());
        assertEquals(2, info.getBackendRoles().size());
        assertTrue(info.getRoles().contains("all_access"));
        assertTrue(info.getBackendRoles().contains("devteam"));
    }
}
