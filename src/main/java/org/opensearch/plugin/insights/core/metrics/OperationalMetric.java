/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.metrics;

import java.util.Locale;

public enum OperationalMetric {
    LOCAL_INDEX_READER_PARSING_EXCEPTIONS("Number of errors when parsing with LocalIndexReader"),
    LOCAL_INDEX_READER_SEARCH_EXCEPTIONS("Number of errors when searching with LocalIndexReader"),
    LOCAL_INDEX_EXPORTER_BULK_FAILURES("Number of failures when ingesting Query Insights data to local indices"),
    LOCAL_INDEX_EXPORTER_DELETE_FAILURES("Number of failures when deleting local indices"),
    LOCAL_INDEX_EXPORTER_EXCEPTIONS("Number of exceptions in Query Insights LocalIndexExporter"),
    INVALID_EXPORTER_TYPE_FAILURES("Number of invalid exporter type failures"),
    DATA_INGEST_EXCEPTIONS("Number of exceptions during data ingest in Query Insights"),
    QUERY_CATEGORIZE_EXCEPTIONS("Number of exceptions when categorizing the queries"),
    EXPORTER_FAIL_TO_CLOSE_EXCEPTION("Number of failures when closing the exporter"),
    READER_FAIL_TO_CLOSE_EXCEPTION("Number of failures when closing the reader"),
    TOP_N_QUERIES_USAGE_COUNT("Number of times the top n queries API is used"),
    TOP_N_QUERIES_SOURCE_TRUNCATION("Number of times a query source was truncated due to size limit");

    private final String description;

    OperationalMetric(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%s", name());
    }
}
