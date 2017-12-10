/*
 * MIT License
 *
 * Copyright (c) 2017 Barracks Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.barracks.bigqueryservice.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.core.RetrySettings;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.HttpTransportOptions;
import com.google.cloud.bigquery.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.barracks.bigqueryservice.model.DeviceEventHook;
import io.barracks.bigqueryservice.model.DeviceRequest;
import io.barracks.bigqueryservice.model.GoogleClientSecret;
import io.barracks.bigqueryservice.utils.DeviceEventHookUtils;
import io.barracks.bigqueryservice.utils.DeviceEventUtils;
import io.barracks.bigqueryservice.utils.DeviceRequestUtils;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.internal.verification.Times;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.io.ClassPathResource;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryClientTest {

    @InjectMocks
    @Spy
    private BigQueryClient bigQueryClient;

    @Mock
    private BigQuery bigQuery;

    @Mock
    private InsertAllResponse response;

    @Mock
    private ObjectMapper mapper;

    @Test
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT", justification = "mock doReturn")
    public void sendDataToBigQuery_whenInsertFails_shouldCreateTableAndDatasetAndLogErrors() throws Exception {
        // Given
        final DeviceEventHook deviceEventHook = DeviceEventHookUtils.getDeviceEventHook();

        doReturn(bigQuery).when(bigQueryClient).buildBigQueryCredentialsAndProjectId(deviceEventHook.getHook().getGoogleClientSecret());
        doReturn(response).when(bigQuery).insertAll(any());
        doReturn(true).when(response).hasErrors();

        // When
        bigQueryClient.sendEventToBigQuery(deviceEventHook);

        // Then
        verify(bigQuery).create(any(TableInfo.class));
        verify(bigQuery).create(any(DatasetInfo.class));
        verify(bigQuery, new Times(2)).insertAll(any());
        verify(response, new Times(2)).getInsertErrors();

    }

    @Test
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT", justification = "mock doReturn")
    public void sendDataToBigQuery_whenTableAndDatasetExistAndSucceeds_shouldCreateTableAndDatasetAndSendRequest() throws Exception {
        // Given
        final DeviceEventHook deviceEventHook = DeviceEventHookUtils.getDeviceEventHook();

        doReturn(bigQuery).when(bigQueryClient).buildBigQueryCredentialsAndProjectId(deviceEventHook.getHook().getGoogleClientSecret());
        doReturn(response).when(bigQuery).insertAll(any());
        doReturn(false).when(response).hasErrors();

        // When
        bigQueryClient.sendEventToBigQuery(deviceEventHook);

        // Then
        verify(bigQuery).create(any(TableInfo.class));
        verify(bigQuery).create(any(DatasetInfo.class));
        verify(bigQuery, new Times(2)).insertAll(any());
        verify(response, never()).getInsertErrors();
    }

    @Test
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT", justification = "mock doReturn")
    public void sendDataToBigQuery_whenNoUserAgentAndIPAddress_shouldCreateTableAndDatasetAndSendRequest() throws Exception {
        // Given
        final DeviceRequest request = DeviceRequestUtils.getDeviceRequest().toBuilder()
                .ipAddress(null)
                .userAgent(null)
                .build();

        final DeviceEventHook deviceEventHook = DeviceEventHookUtils.getDeviceEventHook()
                .toBuilder()
                .deviceEvent(
                        DeviceEventUtils.getDeviceEvent()
                                .toBuilder()
                                .request(request)
                                .build()
                )
                .build();

        doReturn(bigQuery).when(bigQueryClient).buildBigQueryCredentialsAndProjectId(deviceEventHook.getHook().getGoogleClientSecret());
        doReturn(response).when(bigQuery).insertAll(any());
        doReturn(false).when(response).hasErrors();

        // When
        bigQueryClient.sendEventToBigQuery(deviceEventHook);

        // Then
        verify(bigQuery).create(any(TableInfo.class));
        verify(bigQuery).create(any(DatasetInfo.class));
        verify(bigQuery, new Times(2)).insertAll(any());
        verify(response, never()).getInsertErrors();
    }

    @Test
    public void buildBigQueryCredentialsAndProjectId_whenAllIsFine_shouldSetCredentialsAndProjectId() throws IOException {
        //Given
        final ObjectMapper objectMapper = new ObjectMapper();
        final ClassPathResource resource = new ClassPathResource("googleClientSecret.json", getClass());
        final GoogleClientSecret googleClientSecret = objectMapper.readValue(resource.getInputStream(), GoogleClientSecret.class);
        final RetrySettings retrySettings = RetrySettings.newBuilder().setMaxAttempts(10)
                .setMaxRetryDelay(Duration.millis(1000L))
                .setTotalTimeout(Duration.millis(2000L))
                .setInitialRetryDelay(Duration.millis(250L))
                .setRetryDelayMultiplier(1.0)
                .setInitialRpcTimeout(Duration.millis(2000L))
                .setRpcTimeoutMultiplier(1.0)
                .setMaxRpcTimeout(Duration.millis(2000L))
                .build();

        final HttpTransportOptions transportOptions = BigQueryOptions.getDefaultHttpTransportOptions().toBuilder()
                .setConnectTimeout(2000)
                .setReadTimeout(2000)
                .build();
        final BigQuery newBigQuery = BigQueryOptions.newBuilder()
                .setTransportOptions(transportOptions)
                .setRetrySettings(retrySettings)
                .setProjectId("DefaultProjectId")
                .build()
                .getService();

        doReturn(newBigQuery.getOptions()).when(bigQuery).getOptions();
        doReturn(objectMapper.writeValueAsBytes(googleClientSecret)).when(mapper).writeValueAsBytes(googleClientSecret);

        //When
        final BigQuery result = bigQueryClient.buildBigQueryCredentialsAndProjectId(googleClientSecret);

        //Then
        assertThat(result.getOptions().getProjectId()).isEqualTo(googleClientSecret.getProjectId());
        assertThat(result.getOptions().getCredentials()).isEqualTo(ServiceAccountCredentials.fromStream(new ByteArrayInputStream(objectMapper.writeValueAsBytes(googleClientSecret))));
    }
}
