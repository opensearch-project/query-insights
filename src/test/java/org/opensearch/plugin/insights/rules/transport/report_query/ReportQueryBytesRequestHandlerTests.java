/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.rules.transport.report_query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.plugin.insights.core.service.ChildSearchCosts;
import org.opensearch.plugin.insights.core.service.FinishedQueriesCache;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.report_query.ReportQueryBytesAction;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.TransportChannel;

/** Tests for {@link ReportQueryBytesRequestHandler}: the payload the SQL plugin sends becomes the expected record. */
public class ReportQueryBytesRequestHandlerTests extends OpenSearchTestCase {

    private SearchQueryRecord ingestRecordFor(final boolean failed) throws Exception {
        final QueryInsightsService service = mock(QueryInsightsService.class);
        final ReportQueryBytesRequestHandler handler = new ReportQueryBytesRequestHandler(service);

        final BytesTransportRequest request = new BytesTransportRequest(serializePayload(failed), Version.CURRENT);
        final TransportChannel channel = mock(TransportChannel.class);

        handler.messageReceived(request, channel, mock(Task.class));

        final ArgumentCaptor<SearchQueryRecord> captor = ArgumentCaptor.forClass(SearchQueryRecord.class);
        verify(service).addRecord(captor.capture());
        return captor.getValue();
    }

    /** Writes the payload in the same field order as the SQL plugin's {@code QueryInsightsReporter}. */
    private org.opensearch.core.common.bytes.BytesReference serializePayload(final boolean failed) throws Exception {
        return serializePayload("PPL:node-1:42", failed);
    }

    private org.opensearch.core.common.bytes.BytesReference serializePayload(final String marker, final boolean failed) throws Exception {
        final BytesStreamOutput out = new BytesStreamOutput();
        out.writeVInt(ReportQueryBytesAction.FORMAT_VERSION);
        out.writeString("PPL");
        out.writeString(marker);
        out.writeString("node-1");
        out.writeString("source=my_index | head 10");
        out.writeVLong(1_700_000_000_000L);
        out.writeVLong(15L);
        out.writeVLong(1_000L);
        out.writeVLong(2_048L);
        out.writeString("");
        out.writeBoolean(failed);
        return out.bytes();
    }

    public void testFailedFlagTrueSetsFailedAttribute() throws Exception {
        final SearchQueryRecord record = ingestRecordFor(true);
        assertEquals(true, record.getAttributes().get(Attribute.FAILED));
    }

    public void testFailedFlagFalseSetsFailedAttribute() throws Exception {
        final SearchQueryRecord record = ingestRecordFor(false);
        assertEquals(false, record.getAttributes().get(Attribute.FAILED));
    }

    public void testRecordsTheFinishedQueryUnderItsLiveQueryId() throws Exception {
        final QueryInsightsService service = mock(QueryInsightsService.class);
        final FinishedQueriesCache finishedQueries = mock(FinishedQueriesCache.class);
        when(service.getFinishedQueriesCache()).thenReturn(finishedQueries);

        new ReportQueryBytesRequestHandler(service).messageReceived(
            new BytesTransportRequest(serializePayload(false), Version.CURRENT),
            mock(TransportChannel.class),
            mock(Task.class)
        );

        final ArgumentCaptor<SearchQueryRecord> finished = ArgumentCaptor.forClass(SearchQueryRecord.class);
        final ArgumentCaptor<SearchQueryRecord> topN = ArgumentCaptor.forClass(SearchQueryRecord.class);
        final InOrder order = inOrder(finishedQueries, service);
        // Captured before addRecord shares the record with the drain thread.
        order.verify(finishedQueries).capture(finished.capture(), eq("node-1:42"));
        order.verify(service).addRecord(topN.capture());
        assertSame(topN.getValue(), finished.getValue());
    }

    public void testTheQueryTakesTheCostOfItsSearches() throws Exception {
        final QueryInsightsService service = mock(QueryInsightsService.class);
        final ChildSearchCosts held = new ChildSearchCosts();
        held.add("PPL:node-1:42", 30L, 300L);
        held.add("PPL:node-1:42", 70L, 700L);
        when(service.takeChildSearchCosts("PPL:node-1:42")).thenReturn(held.take("PPL:node-1:42"));

        new ReportQueryBytesRequestHandler(service).messageReceived(
            new BytesTransportRequest(serializePayload(false), Version.CURRENT),
            mock(TransportChannel.class),
            mock(Task.class)
        );

        final ArgumentCaptor<SearchQueryRecord> captor = ArgumentCaptor.forClass(SearchQueryRecord.class);
        verify(service).addRecord(captor.capture());
        final SearchQueryRecord record = captor.getValue();
        // The payload reports 1,000 ns and 2,048 B of the query's own; its searches add the rest.
        assertEquals(1_100L, record.getMeasurement(MetricType.CPU).longValue());
        assertEquals(3_048L, record.getMeasurement(MetricType.MEMORY).longValue());
        assertEquals("latency is the query's own", 15L, record.getMeasurement(MetricType.LATENCY).longValue());
    }

    public void testMarkerWithoutATaskIdRecordsNoFinishedQuery() throws Exception {
        final QueryInsightsService service = mock(QueryInsightsService.class);
        final FinishedQueriesCache finishedQueries = mock(FinishedQueriesCache.class);
        when(service.getFinishedQueriesCache()).thenReturn(finishedQueries);

        new ReportQueryBytesRequestHandler(service).messageReceived(
            new BytesTransportRequest(serializePayload("", false), Version.CURRENT),
            mock(TransportChannel.class),
            mock(Task.class)
        );

        verify(finishedQueries, never()).capture(any(SearchQueryRecord.class), anyString());
        verify(service).addRecord(any(SearchQueryRecord.class));
    }

    public void testLiveQueryIdIsTheMarkerWithoutItsSource() {
        assertEquals("node-1:42", ReportQueryBytesRequestHandler.liveQueryIdOf("PPL:node-1:42"));
        assertEquals("aB_c-9:7", ReportQueryBytesRequestHandler.liveQueryIdOf("SQL:aB_c-9:7"));
        for (String marker : new String[] { null, "", "PPL", "PPL:node-1", "PPL::42", "PPL:node-1:", "PPL:node-1:4x2", "PPL:a:b:1" }) {
            assertNull(marker, ReportQueryBytesRequestHandler.liveQueryIdOf(marker));
        }
    }
}
