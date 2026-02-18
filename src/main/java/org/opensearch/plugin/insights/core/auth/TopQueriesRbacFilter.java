/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.auth;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.plugin.insights.core.auth.UserPrincipalContext.UserPrincipalInfo;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.FilterByMode;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

/**
 * Utility class for RBAC-based filtering of top query records.
 */
public class TopQueriesRbacFilter {

    private static final String ALL_ACCESS_ROLE = "all_access";

    private TopQueriesRbacFilter() {}

    /**
     * Filter a list of search query records based on the RBAC filter mode and requesting user.
     *
     * @param records the records to filter
     * @param filterByMode the active filter mode
     * @param userInfo the requesting user's identity info, or null if unavailable
     * @return the filtered list of records
     */
    public static List<SearchQueryRecord> filterRecords(
        List<SearchQueryRecord> records,
        FilterByMode filterByMode,
        UserPrincipalInfo userInfo
    ) {
        if (filterByMode == FilterByMode.NONE) {
            return records;
        }

        if (userInfo == null) {
            return Collections.emptyList();
        }

        if (isAdmin(userInfo)) {
            return records;
        }

        switch (filterByMode) {
            case USERNAME:
                return filterByUsername(records, userInfo.getUserName());
            case BACKEND_ROLES:
                return filterByBackendRoles(records, userInfo.getBackendRoles());
            default:
                return records;
        }
    }

    /**
     * Check if the user has the all_access role (admin).
     *
     * @param userInfo the user's principal info
     * @return true if the user has the all_access role
     */
    public static boolean isAdmin(UserPrincipalInfo userInfo) {
        if (userInfo == null || userInfo.getRoles() == null) {
            return false;
        }
        return userInfo.getRoles().contains(ALL_ACCESS_ROLE);
    }

    /**
     * Filter records by username — only return records whose USERNAME attribute matches the requesting user.
     */
    private static List<SearchQueryRecord> filterByUsername(List<SearchQueryRecord> records, String requestingUsername) {
        if (requestingUsername == null) {
            return Collections.emptyList();
        }
        return records.stream().filter(record -> {
            Object usernameAttr = record.getAttributes().get(Attribute.USERNAME);
            return usernameAttr != null && requestingUsername.equals(usernameAttr.toString());
        }).collect(Collectors.toList());
    }

    /**
     * Filter records by backend roles — only return records that share at least one backend role
     * with the requesting user.
     */
    private static List<SearchQueryRecord> filterByBackendRoles(List<SearchQueryRecord> records, List<String> requestingBackendRoles) {
        if (requestingBackendRoles == null || requestingBackendRoles.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> requestingRolesSet = new HashSet<>(requestingBackendRoles);
        return records.stream().filter(record -> {
            Set<String> recordRoles = getStringSetAttribute(record, Attribute.BACKEND_ROLES);
            return recordRoles != null && !Collections.disjoint(requestingRolesSet, recordRoles);
        }).collect(Collectors.toList());
    }

    /**
     * Extract a string collection attribute as a Set.
     * Handles both String[] (in-memory/XContent path) and List (transport deserialization path,
     * since readGenericValue converts arrays to List).
     *
     * @return the attribute values as a Set, or null if the attribute is absent or unrecognized
     */
    @SuppressWarnings("unchecked")
    private static Set<String> getStringSetAttribute(SearchQueryRecord record, Attribute attribute) {
        Object value = record.getAttributes().get(attribute);
        if (value instanceof String[]) {
            return new HashSet<>(Arrays.asList((String[]) value));
        } else if (value instanceof List) {
            return new HashSet<>((List<String>) value);
        }
        return null;
    }
}
