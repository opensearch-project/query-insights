/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.recommendations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.core.service.recommendations.RecommendationService;
import org.opensearch.plugin.insights.rules.action.recommendations.AnalyzeQueryAction;
import org.opensearch.plugin.insights.rules.action.recommendations.AnalyzeQueryRequest;
import org.opensearch.plugin.insights.rules.action.recommendations.AnalyzeQueryResponse;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.recommendations.Recommendation;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to analyze a query and generate recommendations
 */
public class TransportAnalyzeQueryAction extends HandledTransportAction<AnalyzeQueryRequest, AnalyzeQueryResponse> {
    private static final Logger log = LogManager.getLogger(TransportAnalyzeQueryAction.class);

    private final RecommendationService recommendationService;
    private final NamedXContentRegistry xContentRegistry;

    /**
     * Constructor
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param queryInsightsService the query insights service
     * @param xContentRegistry the named XContent registry
     */
    @Inject
    public TransportAnalyzeQueryAction(
        TransportService transportService,
        ActionFilters actionFilters,
        QueryInsightsService queryInsightsService,
        NamedXContentRegistry xContentRegistry
    ) {
        super(AnalyzeQueryAction.NAME, transportService, actionFilters, AnalyzeQueryRequest::new);
        this.recommendationService = queryInsightsService.getRecommendationService();
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(Task task, AnalyzeQueryRequest request, ActionListener<AnalyzeQueryResponse> listener) {
        try {
            // Parse the query source
            SearchSourceBuilder searchSourceBuilder = parseQuerySource(request.getQuerySource());

            // Create a synthetic SearchQueryRecord for analysis
            SearchQueryRecord record = createSyntheticRecord(searchSourceBuilder, request.getIndices());

            // Generate recommendations
            List<Recommendation> recommendations = recommendationService.generateRecommendations(record);

            // Return the response
            listener.onResponse(new AnalyzeQueryResponse(recommendations));
        } catch (IllegalArgumentException | ParsingException e) {
            log.debug("Invalid query in analyze request", e);
            listener.onFailure(e);
        } catch (Exception e) {
            log.error("Error analyzing query", e);
            listener.onFailure(e);
        }
    }

    private SearchSourceBuilder parseQuerySource(String querySource) throws Exception {
        // The querySource may be just the query part (e.g., {"term": {...}})
        // We need to wrap it in a full SearchSourceBuilder format
        String wrappedQuery = "{\"query\":" + querySource + "}";
        try (
            XContentParser parser = MediaTypeRegistry.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, wrappedQuery)
        ) {
            return SearchSourceBuilder.fromXContent(parser);
        }
    }

    private SearchQueryRecord createSyntheticRecord(SearchSourceBuilder searchSourceBuilder, List<String> indices) {
        // Create a minimal SearchQueryRecord for recommendation analysis
        long timestamp = System.currentTimeMillis();
        Map<MetricType, Measurement> measurements = new HashMap<>();
        Map<Attribute, Object> attributes = new HashMap<>();

        // Add indices attribute if provided
        if (indices != null && !indices.isEmpty()) {
            attributes.put(Attribute.INDICES, new ArrayList<>(indices));
        }

        String id = UUID.randomUUID().toString();

        return new SearchQueryRecord(timestamp, measurements, attributes, searchSourceBuilder, null, id);
    }
}
