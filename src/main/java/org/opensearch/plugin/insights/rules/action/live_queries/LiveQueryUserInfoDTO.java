/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.live_queries;

import java.io.IOException;
import java.util.List;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext;

/**
 * Serializable holder for a captured user identity (username, roles, backend roles),
 * used to carry live query user info across nodes during fan-out.
 */
public class LiveQueryUserInfoDTO implements Writeable {

    private final String username;
    private final List<String> roles;
    private final List<String> backendRoles;

    public LiveQueryUserInfoDTO(String username, List<String> roles, List<String> backendRoles) {
        this.username = username;
        this.roles = roles != null ? roles : List.of();
        this.backendRoles = backendRoles != null ? backendRoles : List.of();
    }

    public LiveQueryUserInfoDTO(UserPrincipalContext.UserPrincipalInfo info) {
        this(info == null ? null : info.getUserName(), info == null ? null : info.getRoles(), info == null ? null : info.getBackendRoles());
    }

    public LiveQueryUserInfoDTO(StreamInput in) throws IOException {
        this.username = in.readOptionalString();
        List<String> readRoles = in.readOptionalStringList();
        this.roles = readRoles != null ? readRoles : List.of();
        List<String> readBackendRoles = in.readOptionalStringList();
        this.backendRoles = readBackendRoles != null ? readBackendRoles : List.of();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(username);
        out.writeOptionalStringCollection(roles.isEmpty() ? null : roles);
        out.writeOptionalStringCollection(backendRoles.isEmpty() ? null : backendRoles);
    }

    public String getUsername() {
        return username;
    }

    public List<String> getRoles() {
        return roles;
    }

    public List<String> getBackendRoles() {
        return backendRoles;
    }
}
