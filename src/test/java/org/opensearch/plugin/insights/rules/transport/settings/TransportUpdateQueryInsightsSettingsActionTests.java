/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.transport.settings;

/*
 * NOTE: This test only covers exception handling. Testing the actual settings update flow requires mocking
 * client.admin().cluster().updateSettings() with async callbacks, which is problematic because Mockito
 * can't create and stub mocks inside doAnswer() callbacks. The settings mapping logic is better tested
 * in integration tests with real cluster clients.
 */

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsRequest;
import org.opensearch.plugin.insights.rules.action.settings.UpdateQueryInsightsSettingsResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;

/**
 * Unit tests for {@link TransportUpdateQueryInsightsSettingsAction}
 */
public class TransportUpdateQueryInsightsSettingsActionTests extends OpenSearchTestCase {

    private TransportUpdateQueryInsightsSettingsAction action;
    private TransportService transportService;
    private ActionFilters actionFilters;
    private Client client;

    @Before
    public void setup() {
        transportService = mock(TransportService.class);
        actionFilters = mock(ActionFilters.class);
        client = mock(Client.class);

        AdminClient adminClient = mock(AdminClient.class);
        ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        action = new TransportUpdateQueryInsightsSettingsAction(transportService, actionFilters, client);
    }

    public void testConstructor() {
        assertNotNull(action);
    }

    public void testUpdateSettingsWithException() {
        UpdateQueryInsightsSettingsRequest request = mock(UpdateQueryInsightsSettingsRequest.class);
        when(request.getSettings()).thenThrow(new RuntimeException("Request processing failed"));

        @SuppressWarnings("unchecked")
        ActionListener<UpdateQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(null, request, listener);

        // Verify exception was caught and passed to listener
        verify(listener).onFailure(any(RuntimeException.class));
    }

    public void testUpdateSettingsWithNullPointerException() {
        UpdateQueryInsightsSettingsRequest request = mock(UpdateQueryInsightsSettingsRequest.class);
        when(request.getSettings()).thenThrow(new NullPointerException("Null value encountered"));

        @SuppressWarnings("unchecked")
        ActionListener<UpdateQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(null, request, listener);

        // Verify exception was caught and passed to listener
        verify(listener).onFailure(any(NullPointerException.class));
    }

    public void testUpdateSettingsWithIllegalArgumentException() {
        UpdateQueryInsightsSettingsRequest request = mock(UpdateQueryInsightsSettingsRequest.class);
        when(request.getSettings()).thenThrow(new IllegalArgumentException("Invalid setting value"));

        @SuppressWarnings("unchecked")
        ActionListener<UpdateQueryInsightsSettingsResponse> listener = mock(ActionListener.class);

        action.doExecute(null, request, listener);

        // Verify exception was caught and passed to listener
        verify(listener).onFailure(any(IllegalArgumentException.class));
    }
}
