/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.reader;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.core.xcontent.NamedXContentRegistry;

/**
 * Factory class for validating and creating Readers based on provided settings
 */
public class QueryInsightsReaderFactory {
    /**
     * Logger of the query insights Reader factory
     */
    private final Logger logger = LogManager.getLogger();
    final private Client client;
    final private Set<QueryInsightsReader> Readers;

    /**
     * Constructor of QueryInsightsReaderFactory
     *
     * @param client OS client
     */
    public QueryInsightsReaderFactory(final Client client) {
        this.client = client;
        this.Readers = new HashSet<>();
    }

    /**
     * Create a Reader based on provided parameters
     *
     * @param indexPattern the index pattern if creating an index Reader
     * @param namedXContentRegistry for parsing purposes
     * @return QueryInsightsReader the created Reader
     */
    public QueryInsightsReader createReader(String indexPattern, NamedXContentRegistry namedXContentRegistry) {
        QueryInsightsReader Reader = new LocalIndexReader(
            client,
            DateTimeFormatter.ofPattern(indexPattern, Locale.ROOT),
            namedXContentRegistry
        );
        this.Readers.add(Reader);
        return Reader;
    }

    /**
     * Update a Reader based on provided parameters
     *
     * @param Reader The Reader to update
     * @param indexPattern the index pattern if creating an index Reader
     * @return QueryInsightsReader the updated Reader sink
     */
    public QueryInsightsReader updateReader(QueryInsightsReader Reader, String indexPattern) {
        if (Reader.getClass() == LocalIndexReader.class) {
            ((LocalIndexReader) Reader).setIndexPattern(DateTimeFormatter.ofPattern(indexPattern, Locale.ROOT));
        }
        return Reader;
    }

    /**
     * Close a Reader
     *
     * @param Reader the Reader to close
     */
    public void closeReader(QueryInsightsReader Reader) throws IOException {
        if (Reader != null) {
            Reader.close();
            this.Readers.remove(Reader);
        }
    }

    /**
     * Close all Readers
     *
     */
    public void closeAllReaders() {
        for (QueryInsightsReader Reader : Readers) {
            try {
                closeReader(Reader);
            } catch (IOException e) {
                logger.error("Fail to close query insights Reader, error: ", e);
            }
        }
    }
}
