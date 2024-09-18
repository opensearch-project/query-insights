/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.health_stats;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the {@link HealthStatsAction} class.
 */
public class HealthStatsActionTests extends OpenSearchTestCase {

    public void testSingletonInstance() {
        assertNotNull(HealthStatsAction.INSTANCE);
    }

    public void testActionName() {
        assertEquals("cluster:admin/opensearch/insights/health_stats", HealthStatsAction.NAME);
        assertEquals(HealthStatsAction.NAME, HealthStatsAction.INSTANCE.name());
    }

    public void testActionResponse() {
        // Verify that the response type supplier produces a HealthStatsResponse object
        assertNotNull(HealthStatsAction.INSTANCE.getResponseReader());
    }
}
