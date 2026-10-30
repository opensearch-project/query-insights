/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.settings;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_EXPORTER_TYPE;

import java.util.Set;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.insights.core.exporter.SinkType;
import org.opensearch.test.OpenSearchTestCase;

public class QueryInsightsSettingsTests extends OpenSearchTestCase {

    public void testExporterTypeAcceptsEverySinkType() {
        for (SinkType sinkType : SinkType.allSinkTypes()) {
            Settings settings = Settings.builder().put(TOP_N_EXPORTER_TYPE.getKey(), sinkType.toString()).build();
            assertEquals(sinkType.toString(), TOP_N_EXPORTER_TYPE.get(settings));
        }
    }

    public void testExporterTypeRejectsEmptyAndBlankValues() {
        for (String value : new String[] { "", " ", "invalid" }) {
            Settings settings = Settings.builder().put(TOP_N_EXPORTER_TYPE.getKey(), value).build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> TOP_N_EXPORTER_TYPE.get(settings));
            assertTrue(e.getMessage(), e.getMessage().contains("Invalid exporter type [" + value + "]"));
        }
    }

    public void testEmptyExporterTypeIsRejectedBeforeItIsApplied() {
        // A dynamic update is validated before it is committed to cluster state. An empty exporter type must fail here,
        // otherwise SinkType.parse throws in the update consumer while the cluster-manager applies the new state.
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(TOP_N_EXPORTER_TYPE));
        Settings update = Settings.builder().put(TOP_N_EXPORTER_TYPE.getKey(), "").build();
        expectThrows(IllegalArgumentException.class, () -> clusterSettings.validate(update, true));
    }
}
