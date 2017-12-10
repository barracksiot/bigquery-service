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
import io.barracks.bigqueryservice.model.DeviceEvent;
import io.barracks.bigqueryservice.model.DeviceEventHook;
import io.barracks.bigqueryservice.model.DeviceRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BigQueryManager {

    private final CounterService counter;
    private BigQueryClient bigQueryClient;

    @Autowired
    public BigQueryManager(BigQueryClient bigQueryClient, CounterService counter) {
        this.bigQueryClient = bigQueryClient;
        this.counter = counter;
    }

    public void sendEventToBigQuery(DeviceEventHook deviceEventHook) {
        try {
            bigQueryClient.sendEventToBigQuery(deviceEventHook);
            incrementRabbitMQMetric("success");
        } catch (Exception e) {
            log.warn("Unable to send Device Event Hook to BigQueryService: " + deviceEventHook + " with exception : " + e);
            incrementRabbitMQMetric("error");
        }
    }

    public void sendEventToBigQuery(DeviceChangeEventHook deviceChangeEventHook) {
        try {
            final DeviceEvent deviceEvent = deviceChangeEventHook.getDeviceChangeEvent().getDeviceEvent();
            final DeviceEventHook deviceEventHook = DeviceEventHook.builder()
                    .deviceEvent(deviceEvent)
                    .hook(deviceChangeEventHook.getHook())
                    .build();

            bigQueryClient.sendEventToBigQuery(deviceEventHook);
            incrementRabbitMQMetric("success");
        } catch (Exception e) {
            log.warn("Unable to send Device Event Hook to BigQueryService: " + deviceChangeEventHook + " with exception : " + e);
            incrementRabbitMQMetric("error");
        }
    }

    void incrementRabbitMQMetric(String status) {
        counter.increment("message.process.bigquery.device.event." + status);
    }

}
