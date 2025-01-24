/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.utils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Util functions for exporter and reader
 *
 */
public class ExporterReaderUtils {

    private ExporterReaderUtils() {}

    /**
     * Generates a consistent 5-digit numeric hash based on the current UTC date.
     * The generated hash is deterministic, meaning it will return the same result for the same date.
     *
     * @return A 5-digit numeric string representation of the current date's hash.
     */
    public static String generateLocalIndexDateHash() {
        // Get the current date in UTC (yyyy-MM-dd format)
        String currentDate = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT)
            .format(Instant.now().atOffset(ZoneOffset.UTC).toLocalDate());

        // Generate a 5-digit numeric hash from the date's hashCode
        return String.format(Locale.ROOT, "%05d", (currentDate.hashCode() % 100000 + 100000) % 100000);
    }
}
