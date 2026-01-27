/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.auth;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.Strings;
import org.opensearch.threadpool.ThreadPool;

/**
 * Extracts the principal (username and roles) from the thread context when security plugin is available
 */
public class UserPrincipalContext {
    private static final String SECURITY_USER_INFO_THREAD_CONTEXT = "_opendistro_security_user_info";
    private static final Pattern PIPE_DELIMITER_PATTERN = Pattern.compile("(?<!\\\\)\\|");

    private final String userString;

    public UserPrincipalContext(ThreadPool threadPool) {
        if (threadPool == null) {
            this.userString = null;
            return;
        }
        ThreadContext threadContext = threadPool.getThreadContext();
        this.userString = threadContext != null ? threadContext.getTransient(SECURITY_USER_INFO_THREAD_CONTEXT) : null;
    }

    /**
     * Get the raw user string
     * @return raw user string, or null if not available
     */
    public String getUserString() {
        return userString;
    }

    /**
     * Parses the stored user string into {@link UserPrincipalInfo}.
     * User String format is pipe separated: user_name|backendroles|roles|...
     * We only extract the username (index 0) and roles (index 2).
     */
    public UserPrincipalInfo extractUserInfo() {
        if (Strings.isNullOrEmpty(userString)) {
            return null;
        }

        String[] strs = PIPE_DELIMITER_PATTERN.split(userString);
        if (strs.length == 0 || Strings.isNullOrEmpty(strs[0])) {
            return null;
        }

        String userName = unescapePipe(strs[0].trim());
        List<String> roles = List.of();

        if (strs.length > 2 && !Strings.isNullOrEmpty(strs[2])) {
            roles = Arrays.stream(strs[2].split(",")).map(this::unescapePipe).toList();
        }

        return new UserPrincipalInfo(userName, roles);
    }

    private String unescapePipe(String input) {
        return input == null ? "" : input.replace("\\|", "|");
    }

    /**
     * Holds parsed user information (username and roles).
     */
    public static class UserPrincipalInfo {
        private final String userName;
        private final List<String> roles;

        UserPrincipalInfo(String userName, List<String> roles) {
            this.userName = userName;
            this.roles = List.copyOf(roles);
        }

        public String getUserName() {
            return userName;
        }

        public List<String> getRoles() {
            return roles;
        }
    }
}
