/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.auth;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext.UserPrincipalInfo;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * Unit tests for {@link UserPrincipalContext}
 */
public class UserPrincipalContextTests extends OpenSearchTestCase {
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("UserPrincipalContextTest");
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testExtractUserInfoFromThreadContext() {
        threadPool.getThreadContext().putTransient("_opendistro_security_user_info", "testuser|role1,role2|admin,user|tenant1|access1");
        UserPrincipalContext userPrincipalContext = new UserPrincipalContext(threadPool);

        assertEquals("testuser|role1,role2|admin,user|tenant1|access1", userPrincipalContext.getUserString());

        UserPrincipalInfo userInfo = userPrincipalContext.extractUserInfo();
        assertNotNull(userInfo);
        assertEquals("testuser", userInfo.getUserName());
        assertEquals(List.of("role1", "role2"), userInfo.getBackendRoles());
        assertEquals(List.of("admin", "user"), userInfo.getRoles());
    }

    public void testExtractUserInfoNoThreadContext() {
        UserPrincipalContext userPrincipalContext = new UserPrincipalContext(threadPool);

        assertNull(userPrincipalContext.getUserString());
        assertNull(userPrincipalContext.extractUserInfo());
    }

    public void testExtractUserInfoInvalidFormat() {
        threadPool.getThreadContext().putTransient("_opendistro_security_user_info", "|role1,role2|admin");
        UserPrincipalContext userPrincipalContext = new UserPrincipalContext(threadPool);

        assertNotNull(userPrincipalContext.getUserString());
        assertNull(userPrincipalContext.extractUserInfo());
    }

    public void testExtractBackendRolesFromThreadContext() {
        threadPool.getThreadContext()
            .putTransient("_opendistro_security_user_info", "admin_user|backend_role1,backend_role2|all_access|tenant1|access1");
        UserPrincipalContext userPrincipalContext = new UserPrincipalContext(threadPool);
        UserPrincipalInfo userInfo = userPrincipalContext.extractUserInfo();

        assertNotNull(userInfo);
        assertEquals("admin_user", userInfo.getUserName());
        assertEquals(List.of("backend_role1", "backend_role2"), userInfo.getBackendRoles());
        assertEquals(List.of("all_access"), userInfo.getRoles());
    }

    public void testExtractUserInfoNoBackendRoles() {
        threadPool.getThreadContext().putTransient("_opendistro_security_user_info", "testuser||admin,user|tenant1|access1");
        UserPrincipalContext userPrincipalContext = new UserPrincipalContext(threadPool);
        UserPrincipalInfo userInfo = userPrincipalContext.extractUserInfo();

        assertNotNull(userInfo);
        assertEquals("testuser", userInfo.getUserName());
        assertEquals(List.of(), userInfo.getBackendRoles());
        assertEquals(List.of("admin", "user"), userInfo.getRoles());
    }
}
