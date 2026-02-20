/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.util.Locale;

/**
 * Enum representing the RBAC filter mode for the Top Queries API.
 * Controls how query records are filtered based on user identity.
 */
public enum FilterByMode {
    /**
     * No filtering — all records are returned to all users.
     */
    NONE,

    /**
     * Filter by username — users see only their own queries.
     */
    USERNAME,

    /**
     * Filter by backend roles — users see queries from users who share at least one backend role.
     */
    BACKEND_ROLES;

    /**
     * Parse a string into a FilterByMode.
     *
     * @param value the string value to parse
     * @return the corresponding FilterByMode
     * @throws IllegalArgumentException if the value does not match any mode
     */
    public static FilterByMode fromString(String value) {
        try {
            return FilterByMode.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Invalid filter_by_mode value: [" + value + "]. Valid values are: none, username, backend_roles"
            );
        }
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
