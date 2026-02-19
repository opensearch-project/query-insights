/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.recommendations;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link Direction}
 */
public class DirectionTests extends OpenSearchTestCase {

    public void testEnumValues() {
        Direction[] values = Direction.values();
        assertEquals(3, values.length);
        assertEquals(Direction.INCREASE, Direction.valueOf("INCREASE"));
        assertEquals(Direction.DECREASE, Direction.valueOf("DECREASE"));
        assertEquals(Direction.NEUTRAL, Direction.valueOf("NEUTRAL"));
    }

    public void testOrdinals() {
        assertEquals(0, Direction.INCREASE.ordinal());
        assertEquals(1, Direction.DECREASE.ordinal());
        assertEquals(2, Direction.NEUTRAL.ordinal());
    }
}
