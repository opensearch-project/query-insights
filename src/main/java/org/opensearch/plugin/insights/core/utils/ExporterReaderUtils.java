/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Util functions for exporter and reader
 *
 */
public class ExporterReaderUtils {

    private ExporterReaderUtils() {}

    /**
     * Generates a consistent 5-digit numeric hash based on the given UTC date.
     * The generated hash is deterministic, meaning it will return the same result for the same date.
     *
     * @return A 5-digit numeric string representation of the current date's hash.
     */
    public static String generateLocalIndexDateHash(LocalDate date) {
        // Get the date string in UTC (yyyy-MM-dd format)
        String dateString = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT).format(date);

        // Generate a 5-digit numeric hash from the date's hashCode
        return String.format(Locale.ROOT, "%05d", (dateString.hashCode() % 100000 + 100000) % 100000);
    }
}
