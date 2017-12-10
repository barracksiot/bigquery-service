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

package io.barracks.bigqueryservice.manager;

import io.barracks.bigqueryservice.client.BigQueryClient;
import io.barracks.bigqueryservice.model.DeviceChangeEventHook;
import io.barracks.bigqueryservice.model.DeviceEventHook;
import io.barracks.bigqueryservice.model.DeviceRequest;
import io.barracks.bigqueryservice.utils.DeviceChangeEventHookUtils;
import io.barracks.bigqueryservice.utils.DeviceEventHookUtils;
import io.barracks.bigqueryservice.utils.DeviceRequestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.internal.verification.Times;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryManagerTest {

    @Mock
    private BigQueryClient bigQueryClient;

    @InjectMocks
    @Spy
    private BigQueryManager bigQueryManager;

    @Test
    public void sendDataToBigQuery_shouldNotThrowException_whenBigQueryClientThrowException() throws Exception {
        // Given
        final DeviceEventHook deviceEventHook = DeviceEventHookUtils.getDeviceEventHook();

        doThrow(Exception.class).when(bigQueryClient).sendEventToBigQuery(deviceEventHook);
        doNothing().when(bigQueryManager).incrementRabbitMQMetric(anyString());

        // When
        bigQueryManager.sendEventToBigQuery(deviceEventHook);

        // When / Then
        verify(bigQueryClient, new Times(1)).sendEventToBigQuery(deviceEventHook);
    }

    @Test
    public void sendDataToBigQuery_shouldCallClients_andReturnsNothing() throws Exception {
        // Given
        final DeviceEventHook deviceEventHook = DeviceEventHookUtils.getDeviceEventHook();
        doNothing().when(bigQueryManager).incrementRabbitMQMetric(anyString());

        // When
        bigQueryManager.sendEventToBigQuery(deviceEventHook);

        // When / Then
        verify(bigQueryClient, new Times(1)).sendEventToBigQuery(deviceEventHook);
    }

    @Test
    public void sendDataWithChangedRequestToBigQuery_shouldNotThrowException_whenBigQueryClientThrowException() throws Exception {
        // Given
        final DeviceChangeEventHook deviceChangeEventHook = DeviceChangeEventHookUtils.getDeviceChangeEventHook();
        final DeviceEventHook deviceEventHook = DeviceEventHook.builder()
                .deviceEvent(deviceChangeEventHook.getDeviceChangeEvent().getDeviceEvent())
                .hook(deviceChangeEventHook.getHook())
                .build();

        doThrow(Exception.class).when(bigQueryClient).sendEventToBigQuery(deviceEventHook);
        doNothing().when(bigQueryManager).incrementRabbitMQMetric(anyString());

        // When
        bigQueryManager.sendEventToBigQuery(deviceChangeEventHook);

        // When / Then
        verify(bigQueryClient, new Times(1)).sendEventToBigQuery(deviceEventHook);
    }

    @Test
    public void sendDataWithChangedRequestToBigQuery_shouldCallClients_andReturnsNothing() throws Exception {
        // Given
        final DeviceChangeEventHook deviceChangeEventHook = DeviceChangeEventHookUtils.getDeviceChangeEventHook();
        final DeviceEventHook deviceEventHook = DeviceEventHook.builder()
                .deviceEvent(deviceChangeEventHook.getDeviceChangeEvent().getDeviceEvent())
                .hook(deviceChangeEventHook.getHook())
                .build();
        doNothing().when(bigQueryManager).incrementRabbitMQMetric(anyString());

        // When
        bigQueryManager.sendEventToBigQuery(deviceChangeEventHook);

        // When / Then
        verify(bigQueryClient, new Times(1)).sendEventToBigQuery(deviceEventHook);
    }

}
