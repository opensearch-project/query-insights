/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link FilterByMode}
 */
public class FilterByModeTests extends OpenSearchTestCase {

    public void testFromStringNone() {
        assertEquals(FilterByMode.NONE, FilterByMode.fromString("none"));
        assertEquals(FilterByMode.NONE, FilterByMode.fromString("NONE"));
        assertEquals(FilterByMode.NONE, FilterByMode.fromString("None"));
    }

    public void testFromStringUsername() {
        assertEquals(FilterByMode.USERNAME, FilterByMode.fromString("username"));
        assertEquals(FilterByMode.USERNAME, FilterByMode.fromString("USERNAME"));
    }

    public void testFromStringBackendRoles() {
        assertEquals(FilterByMode.BACKEND_ROLES, FilterByMode.fromString("backend_roles"));
        assertEquals(FilterByMode.BACKEND_ROLES, FilterByMode.fromString("BACKEND_ROLES"));
    }

    public void testFromStringInvalid() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FilterByMode.fromString("invalid"));
        assertTrue(e.getMessage().contains("Invalid filter_by_mode value"));
    }

    public void testToString() {
        assertEquals("none", FilterByMode.NONE.toString());
        assertEquals("username", FilterByMode.USERNAME.toString());
        assertEquals("backend_roles", FilterByMode.BACKEND_ROLES.toString());
    }
}
