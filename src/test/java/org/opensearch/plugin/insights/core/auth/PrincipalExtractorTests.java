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
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * Unit tests for {@link PrincipalExtractor}
 */
public class PrincipalExtractorTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private PrincipalExtractor principalExtractor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("PrincipalExtractorTest");
        principalExtractor = new PrincipalExtractor(threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private PrincipalExtractor.UserPrincipalInfo extractUserInfoWithContext(String userInfoValue) {
        ThreadContext threadContext = threadPool.getThreadContext();
        threadContext.putTransient("_opendistro_security_user_info", userInfoValue);
        return principalExtractor.extractUserInfo();
    }

    public void testExtractUserInfoFromThreadContext() {
        PrincipalExtractor.UserPrincipalInfo userInfo = extractUserInfoWithContext("testuser|role1,role2|admin,user|tenant1|access1");
        assertNotNull(userInfo);
        assertEquals("testuser", userInfo.getUserName());
        assertEquals(List.of("admin", "user"), userInfo.getRoles());
    }

    public void testExtractUserInfoNoThreadContext() {
        PrincipalExtractor.UserPrincipalInfo userInfo = principalExtractor.extractUserInfo();
        assertNull(userInfo);
    }

    public void testExtractUserInfoInvalidFormat() {
        PrincipalExtractor.UserPrincipalInfo userInfo = extractUserInfoWithContext("|role1,role2|admin");
        assertNull(userInfo);
    }
}
